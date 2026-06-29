-- Engines whose only change-detection signal is a value derived from the current content - a size, a
-- modification time or an ETag - cannot satisfy the loop-free (no-ABA) contract of modification_hash: such
-- a value returns to an earlier one across an A -> B -> A change (e.g. a same-size rewrite back to
-- byte-identical content within the same second restores all of them). The consistent query cache
-- (query_cache_use_only_when_data_was_not_changed) and REFRESH ... IF CHANGED validate by comparing the
-- referenced-tables hash sampled before the read with the one sampled after, so a value that can repeat
-- would let a concurrent A -> B -> A pass that check while the read actually saw the transient state B.
-- File, URL and object storage therefore fail closed: they report a NULL modification_hash, so the
-- consistency consumers never reuse a result for them. The real ABA race needs a controllable external
-- server transitioning A -> B -> A around a read and is not deterministically reproducible here; this test
-- locks in the invariant that forecloses it. (issue #108713, AI-review thread on PR #108721.)

DROP TABLE IF EXISTS t_file;
DROP TABLE IF EXISTS t_url;
DROP TABLE IF EXISTS t_url_glob;

-- File: size and modification time are weak validators, so modification_hash is NULL (fail closed).
CREATE TABLE t_file (x UInt64) ENGINE = File(CSV);
INSERT INTO t_file VALUES (1), (2), (3);
SELECT 'file null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't_file';

-- URL: the ETag / size / Last-Modified are all content-derived and can repeat across an A -> B -> A rewrite,
-- so modification_hash is NULL (fail closed). No server is contacted - the guard returns before any HTTP
-- request, so this is safe and deterministic even though the host is unreachable.
CREATE TABLE t_url (x UInt64) ENGINE = URL('http://localhost:1/a.csv', CSV);
CREATE TABLE t_url_glob (x UInt64) ENGINE = URL('http://localhost:1/{a,b}.csv', CSV);
SELECT 'url null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't_url';
SELECT 'url glob null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't_url_glob';

-- End to end: with query_cache_use_only_when_data_was_not_changed, a query over a fail-closed table (File
-- reports NULL) is never served a stale result - the cache is bypassed, so an append is reflected
-- immediately. A stale result here (still 3 after the insert) would mean the cache was wrongly keyed on a
-- value that did not change with the data. The query runs in its own database, so its cache key is isolated.
SELECT count() FROM t_file SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
INSERT INTO t_file VALUES (4);
SELECT count() FROM t_file SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

DROP TABLE t_file;
DROP TABLE t_url;
DROP TABLE t_url_glob;
