# Meilisearch

Meilisearch is a table engine and a table function to access data which lies in [meilisearch](https://www.meilisearch.com/).  Meilisearch is an open-source search engine providing typo-tolerance, filters, synonyms, etc.


- Follow the Meilisearch quick start and load the movies table
  ```bash
  curl -O https://docs.meilisearch.com/movies.json
  ```
  Response
  ```bash
  curl \
    -X POST 'http://localhost:7700/indexes/movies/documents' \
    -H 'Content-Type: application/json' \
    --data-binary @movies.json
  ```

- Create a view in ClickHouse that is associated with Meilisearch movies table
  ```sql
  CREATE TABLE
  movies_table(id String, title String, release_date Int64)
  ENGINE = MeiliSearch('http://localhost:7700', 'movies', '')
  ```
  Response:
  ```text
  CREATE TABLE movies_table
  (
      `id` String,
      `title` String,
      `release_date` Int64
  )
  ENGINE = MeiliSearch('http://localhost:7700', 'movies', '')

  Query id: 240294db-489a-495a-9dc4-21dcacd929a8

  Ok.

  0 rows in set. Elapsed: 0.011 sec.
  ```

- Verify, still from ClickHouse: 
  ```sql
  SELECT COUNT() FROM movies_table
  ```
  Response
  ```text
  19546
  ```

- Search Meilisearch from ClickHouse
  ```sql
  SELECT * FROM movies_table
  WHERE meiliMatch(\'"q"="abaca"\')
  ```
