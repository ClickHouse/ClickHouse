-- Tags: no-ordinary-database, no-fasttest, no-parallel-replicas, need-query-parameters
-- no-ordinary-database, no-fasttest: the test uses experimental transactions.
-- no-parallel-replicas: reads inside a transaction return wrong (multiplied) counts with parallel
-- replicas, which are not transaction-aware (same as other transaction tests, e.g. 04060).

-- Tests that the consistent query cache (`query_cache_use_only_when_data_was_not_changed`) fails
-- closed under `implicit_transaction = 1`. The implicit transaction is started only on the cache-miss
-- path, *after* the cache probe, so at probe time `getCurrentTransaction` is still null even though
-- the read itself will run on a transaction snapshot. Without the fail-close, the probe samples each
-- table's live state and a cache entry could be served to (or stored by) a query whose transaction
-- snapshot has diverged from that live state - the same divergence 04826 tests for an explicit
-- `BEGIN TRANSACTION`. (AI-review thread on PR #108721.)

DROP TABLE IF EXISTS t_implicit_txn;
CREATE TABLE t_implicit_txn (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_implicit_txn VALUES (1), (2);

-- `system.query_cache` is server-wide and its entries outlive a single test run, so the lookups below
-- must be immune to entries of a concurrent or earlier run of this very test: the marker literals are
-- chosen so that neither is a substring of the other, and the current database name is folded into the
-- cached queries (the predicate is a no-op for the result, and the query parameter is substituted
-- before the query text is stored), which makes every run's entries distinguishable.

-- With `implicit_transaction = 1` the consistent cache is bypassed: no entry is stored, and the second
-- run cannot be a hit.
SELECT count(), 'qc_04843_implicit' FROM t_implicit_txn WHERE {CLICKHOUSE_DATABASE:String} != '' SETTINGS implicit_transaction = 1, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT count(), 'qc_04843_implicit' FROM t_implicit_txn WHERE {CLICKHOUSE_DATABASE:String} != '' SETTINGS implicit_transaction = 1, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT 'query with implicit_transaction not stored', count() = 0 FROM system.query_cache WHERE query LIKE '%qc_04843_implicit%' AND query LIKE '%' || currentDatabase() || '%' AND query NOT LIKE '%system.query_cache%';

-- The bypass must happen at the probe, not at finalization: before the fix the entry was also never
-- *stored* (the writer-side validator ran inside the by-then-started transaction and dropped it), but
-- the probe still consulted the cache, so a hit against an entry under the same key would have been
-- served. With the fail-close the queries must not touch the cache at all - neither a hit nor a miss.
SYSTEM FLUSH LOGS query_log;
SELECT 'implicit_transaction queries did not touch the cache', sum(ProfileEvents['QueryCacheHits'] + ProfileEvents['QueryCacheMisses']) = 0 FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish' AND current_database = currentDatabase() AND query LIKE '%qc_04843_implicit%' AND query NOT LIKE '%system.query_log%';

-- The control: the same query without `implicit_transaction` is stored.
SELECT count(), 'qc_04843_control' FROM t_implicit_txn WHERE {CLICKHOUSE_DATABASE:String} != '' SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT 'control query stored', count() > 0 FROM system.query_cache WHERE query LIKE '%qc_04843_control%' AND query LIKE '%' || currentDatabase() || '%' AND query NOT LIKE '%system.query_cache%';

DROP TABLE t_implicit_txn;
