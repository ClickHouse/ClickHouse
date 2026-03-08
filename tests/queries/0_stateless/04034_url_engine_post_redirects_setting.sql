-- Tags: no-fasttest, no-parallel
-- Verify that the http_allow_redirects_on_post setting exists and defaults to false.
-- Full redirect testing requires an HTTP server, but we can verify the setting
-- is recognized and configurable.
-- See https://github.com/ClickHouse/ClickHouse/issues/42806

-- Setting should exist and default to false (0)
SELECT value FROM system.settings WHERE name = 'http_allow_redirects_on_post';

-- Setting should be changeable
SET http_allow_redirects_on_post = 1;
SELECT value FROM system.settings WHERE name = 'http_allow_redirects_on_post';
