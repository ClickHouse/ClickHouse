SET max_threads = 10 * 10;
SELECT value FROM system.settings WHERE name = 'max_threads';
SELECT value FROM system.settings WHERE name = 'max_threads' SETTINGS max_threads = 5 * 5;
SELECT value FROM system.settings WHERE name = 'query_cache_tag';
SET query_cache_tag = currentDatabase();
SELECT value FROM system.settings WHERE name = 'query_cache_tag';
