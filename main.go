package main

import (
	"bufio"
	"container/list"
	"database/sql"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	_ "github.com/mattn/go-sqlite3"
)

type Frontier struct {
	filePath string
	mu       sync.Mutex
}

func NewFrontier(filePath string) *Frontier {
	return &Frontier{filePath: filePath}
}

func (f *Frontier) AddURL(url string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := os.OpenFile(f.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.WriteString(url + "\n"); err != nil {
		return err
	}
	return nil
}

func (f *Frontier) GetNextURL() (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := os.OpenFile(f.filePath, os.O_RDWR, 0644)
	if err != nil {
		return "", err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	url, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	url = strings.TrimSpace(url)

	// Read the rest of the file
	restOfFile, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	// Truncate and rewrite the file without the first line
	if err := file.Truncate(0); err != nil {
		return "", err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return "", err
	}
	if _, err := file.Write(restOfFile); err != nil {
		return "", err
	}

	return url, nil
}

type Scraper struct {
	db       *sql.DB
	visited  *LRUCache
	frontier *Frontier
}

func NewScraper(db *sql.DB, frontier *Frontier, cacheSize int) *Scraper {
	return &Scraper{
		db:       db,
		frontier: frontier,
		visited:  NewLRUCache(cacheSize),
	}
}

func (s *Scraper) Run() error {
	for {
		url, err := s.frontier.GetNextURL()
		if err != nil {
			if err.Error() == "no URLs in frontier" {
				break
			}
			return err
		}

		err = s.ProcessURL(url)
		if err != nil {
			log.Printf("Error processing %s: %v", url, err)
		}
	}
	return nil
}

func (s *Scraper) ProcessURL(urlStr string) error {
	// Check if URL has been visited
	if !s.visited.Add(urlStr) {
		return nil // URL already visited, skip processing
	}

	// Fetcher
	html, err := s.fetch(urlStr)
	if err != nil {
		return err
	}

	// Parser
	title, body, links, err := s.parse(html)
	if err != nil {
		return err
	}

	// Content store
	err = s.store(urlStr, html, title, body)
	if err != nil {
		return err
	}

	// URL extractor
	for _, link := range links {
		normalizedURL := s.normalizeURL(urlStr, link)
		if normalizedURL != "" {
			s.frontier.AddURL(normalizedURL)
		}
	}

	// Indexer
	return s.index(urlStr, body)
}

func (s *Scraper) fetch(urlStr string) (string, error) {
	resp, err := http.Get(urlStr)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func (s *Scraper) parse(html string) (string, string, []string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return "", "", nil, err
	}

	title := doc.Find("title").Text()
	body := doc.Find("body").Text()

	var links []string
	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists {
			links = append(links, href)
		}
	})

	return title, body, links, nil
}

func (s *Scraper) store(urlStr, html, title, body string) error {
	_, err := s.db.Exec("INSERT OR REPLACE INTO pages (url, html, text, title, last_crawled) VALUES (?, ?, ?, ?, ?)",
		urlStr, html, body, title, time.Now())
	return err
}

func (s *Scraper) normalizeURL(base, href string) string {
	u, err := url.Parse(href)
	if err != nil {
		return ""
	}

	baseURL, err := url.Parse(base)
	if err != nil {
		return ""
	}

	u = baseURL.ResolveReference(u)
	normalized := u.String()
	// Only return the URL if it hasn't been visited
	if s.visited.Add(normalized) {
		return normalized
	}

	return u.String()
}

func (s *Scraper) index(urlStr, body string) error {
	words := strings.Fields(body)
	wordCount := make(map[string]int)
	for _, word := range words {
		word = strings.ToLower(word)
		wordCount[word]++
	}

	for word, count := range wordCount {
		_, err := s.db.Exec("INSERT INTO inverted_index (term, page_id, frequency) VALUES (?, (SELECT id FROM pages WHERE url = ?), ?)",
			word, urlStr, count)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	db, err := sql.Open("sqlite3", "db.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = migrations(db)
	if err != nil {
		log.Fatal(err)
	}

	frontier := NewFrontier("frontier.txt")
	scraper := NewScraper(db, frontier, 10000) // Cache size of 10,000 URLs

	// Add initial URL
	frontier.AddURL("https://imdawon.com")

	err = scraper.Run()
	if err != nil {
		log.Fatal(err)
	}
}

type LRUCache struct {
	capacity int
	cache    map[string]*list.Element
	list     *list.List
	mutex    sync.Mutex
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		list:     list.New(),
	}
}

func (c *LRUCache) Add(key string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if elem, exists := c.cache[key]; exists {
		c.list.MoveToFront(elem)
		return false
	}

	if c.list.Len() >= c.capacity {
		oldest := c.list.Back()
		if oldest != nil {
			c.list.Remove(oldest)
			delete(c.cache, oldest.Value.(string))
		}
	}

	elem := c.list.PushFront(key)
	c.cache[key] = elem
	return true
}

func migrations(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS pages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		url TEXT UNIQUE,
		html TEXT,
		text TEXT,
		title TEXT,
		last_crawled TIMESTAMP
	  );

	CREATE TABLE IF NOT EXISTS links (
		from_page_id INTEGER,
		to_url TEXT,
		FOREIGN KEY(from_page_id) REFERENCES pages(id)
	  );
	  
	  CREATE TABLE IF NOT EXISTS inverted_index (
		term TEXT,
		page_id INTEGER,
		frequency INTEGER,
		FOREIGN KEY(page_id) REFERENCES pages(id)
	  );

	CREATE INDEX IF NOT EXISTS idx_links_from_page_id ON links(from_page_id);
	CREATE INDEX IF NOT EXISTS idx_inverted_index_term ON inverted_index(term);
	CREATE INDEX IF NOT EXISTS idx_inverted_index_page_id ON inverted_index(page_id);
	`

	_, err := db.Exec(query)
	return err

}
