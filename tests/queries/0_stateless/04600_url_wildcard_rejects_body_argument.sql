-- The `body(...)` argument forms the HTTP request body when reading from the `url` table function.
-- When the path contains listable wildcards (`*` / `**`), the read is served by expanding HTTP index
-- pages through the `web` object storage, which issues plain `GET` requests and cannot carry a body.
-- The AST rebuilt for that path (`makeWebObjectStorageEngineArgs`) drops `body(...)`, so it must be
-- rejected loudly instead of silently downgrading the request to a `GET`.
--
-- The rejection fires before any HTTP request: for an explicit structure during storage creation, and
-- for an omitted (auto) structure during schema inference, so the unroutable URL is never contacted.
-- It is also independent of the experimental setting, so it holds whether wildcard expansion from index
-- pages is enabled or not.

-- Explicit structure (rejected in `TableFunctionURL::getStorage`):
SELECT * FROM url('http://localhost:11111/data/*.json', 'JSONEachRow', 'x UInt32', body('payload')) SETTINGS allow_experimental_url_wildcard_from_index_pages = 1; -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:11111/data/*.json', 'JSONEachRow', 'x UInt32', body('')) SETTINGS allow_experimental_url_wildcard_from_index_pages = 1; -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:11111/data/*.json', 'JSONEachRow', 'x UInt32', body('payload')) SETTINGS allow_experimental_url_wildcard_from_index_pages = 0; -- { serverError BAD_ARGUMENTS }

-- Auto structure (rejected in `TableFunctionURL::getActualTableStructure`, before the schema-inference request):
SELECT * FROM url('http://localhost:11111/data/*.json', body('payload')) SETTINGS allow_experimental_url_wildcard_from_index_pages = 1; -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:11111/data/*.json', body('')) SETTINGS allow_experimental_url_wildcard_from_index_pages = 1; -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://localhost:11111/data/*.json', body('payload')) SETTINGS allow_experimental_url_wildcard_from_index_pages = 0; -- { serverError BAD_ARGUMENTS }

-- A subquery body is rejected on the wildcard path too (the subquery is never interpreted).
SELECT * FROM url('http://localhost:11111/data/*.json', body((SELECT 1))) SETTINGS allow_experimental_url_wildcard_from_index_pages = 1; -- { serverError BAD_ARGUMENTS }
