-- Tags: no-parallel
-- no-parallel: runs SYSTEM DROP ENCRYPTION HEADERS CACHE, which clears a process-global cache
-- shared with other tests (also required by the style check for any test using SYSTEM DROP).
--
-- The encryption header cache is a global cache configured by the `encryption_header_cache_size`
-- server setting (default 50 MiB) and cleared by `SYSTEM DROP ENCRYPTION HEADERS CACHE`. This
-- checks the setting is registered, the metric exists, and both spellings of the drop statement
-- execute. The cache's hit/serve behaviour is covered deterministically by the ReaderExecutor
-- gtests (the global cache makes per-query hit assertions flaky under parallel runs).

-- The server setting exists with the 50 MiB default.
SELECT name, value FROM system.server_settings WHERE name = 'encryption_header_cache_size';

-- The size metric is registered.
SELECT count() FROM system.metrics WHERE metric = 'EncryptionHeaderCacheBytes';

-- Both statement spellings parse and execute (clearing a global cache is always allowed here).
SYSTEM DROP ENCRYPTION HEADERS CACHE;
SYSTEM CLEAR ENCRYPTION HEADERS CACHE;
