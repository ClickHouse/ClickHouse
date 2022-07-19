package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
)

func compress(data io.Reader) io.Reader {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)

	go func() {
		_, _ = io.Copy(gw, data)
		gw.Close()
		pw.Close()
	}()

	return pr
}

func main() {
	database := os.Getenv("CLICKHOUSE_DATABASE")
	p, err := url.Parse("http://localhost:8123/")
	if err != nil {
		panic(err)
	}
	q := p.Query()

	q.Set("query", "INSERT INTO "+database+".graphite FORMAT RowBinary")
	p.RawQuery = q.Encode()
	queryUrl := p.String()

	var req *http.Request

	req, err = http.NewRequest("POST", queryUrl, compress(os.Stdin))
	req.Header.Add("Content-Encoding", "gzip")

	if err != nil {
		panic(err)
	}

	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		panic(fmt.Errorf("clickhouse response status %d: %s", resp.StatusCode, string(body)))
	}
}
