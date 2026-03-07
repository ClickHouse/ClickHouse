-- Tags: no-fasttest, no-parallel
-- Verify that the max_http_post_redirects setting exists and defaults to 0.
-- Full redirect testing requires an HTTP server, but we can verify the setting
-- is recognized and configurable.
-- See https://github.com/ClickHouse/ClickHouse/issues/42806

-- Setting should exist and default to 0
SELECT value FROM system.settings WHERE name = 'max_http_post_redirects';

-- Setting should be changeable
SET max_http_post_redirects = 5;
SELECT value FROM system.settings WHERE name = 'max_http_post_redirects';
