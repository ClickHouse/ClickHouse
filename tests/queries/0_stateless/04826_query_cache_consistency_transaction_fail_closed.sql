-- Tags: no-ordinary-database, no-fasttest, need-query-parameters
-- no-ordinary-database, no-fasttest: the test uses experimental transactions.

-- Tests that the consistent query cache (`query_cache_use_only_when_data_was_not_changed`) fails
-- closed inside a transaction. The referenced-tables modification hash samples each table's live
-- state, while a query inside a transaction reads the transaction's snapshot; the two can diverge for
-- the whole duration of the query (a commit from another session after `BEGIN TRANSACTION`), so the
-- query could hit a cache entry keyed by the live state or store its snapshot result under the live
-- key without the pre/post finalization recheck noticing. Until the hash is transaction-snapshot-aware
-- the cache must be bypassed. (AI-review thread on PR #108721.)

DROP TABLE IF EXISTS t_txn;
CREATE TABLE t_txn (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_txn VALUES (1), (2);

-- `system.query_cache` is server-wide and its entries outlive a single test run, so the lookups below
-- must be immune to entries of a concurrent or earlier run of this very test: the marker literals are
-- chosen so that neither is a substring of the other, and the current database name is folded into the
-- cached queries (the predicate is a no-op for the result, and the query parameter is substituted
-- before the query text is stored), which makes every run's entries distinguishable.

-- Inside a transaction the consistent cache is bypassed: no entry is stored.
BEGIN TRANSACTION;
SELECT count(), 'qc_04826_txn' FROM t_txn WHERE {CLICKHOUSE_DATABASE:String} != '' SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
COMMIT;
SELECT 'query inside a transaction not stored', count() = 0 FROM system.query_cache WHERE query LIKE '%qc_04826_txn%' AND query LIKE '%' || currentDatabase() || '%' AND query NOT LIKE '%system.query_cache%';

-- The control: the same query outside a transaction is stored.
SELECT count(), 'qc_04826_control' FROM t_txn WHERE {CLICKHOUSE_DATABASE:String} != '' SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT 'control query stored', count() > 0 FROM system.query_cache WHERE query LIKE '%qc_04826_control%' AND query LIKE '%' || currentDatabase() || '%' AND query NOT LIKE '%system.query_cache%';

DROP TABLE t_txn;
