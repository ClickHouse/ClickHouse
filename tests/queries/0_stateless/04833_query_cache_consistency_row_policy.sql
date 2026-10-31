-- Tags: need-query-parameters

-- Tests that the consistent query cache (`query_cache_use_only_when_data_was_not_changed`) fails
-- closed when a referenced table has an active row policy for the current user: an `ALTER ROW POLICY`
-- (or a `CREATE`/`DROP` of one) changes what the user reads while every referenced table is unchanged,
-- and the cache key only folds the user and role IDs, not the policy. A filter that is literally
-- always true does not affect the result, so it does not disable the feature. (AI-review thread on
-- PR #108721.)

DROP TABLE IF EXISTS t_rp;
CREATE TABLE t_rp (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_rp VALUES (1), (2);

-- `system.query_cache` is server-wide and its entries outlive a single test run, so the lookups below
-- must be immune to entries of a concurrent or earlier run of this very test: the marker literals are
-- chosen so that none is a substring of another, and the current database name is folded into the
-- cached queries (the predicate is a no-op for the result, and the query parameter is substituted
-- before the query text is stored), which makes every run's entries distinguishable.

-- The control: without a row policy the query is stored.
SELECT sum(x), 'qc_04833_nopolicy' FROM t_rp WHERE {CLICKHOUSE_DATABASE:String} != '' SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT 'no policy stored', count() > 0 FROM system.query_cache WHERE query LIKE '%qc_04833_nopolicy%' AND query LIKE '%' || currentDatabase() || '%' AND query NOT LIKE '%system.query_cache%';

-- With a non-trivial row policy the consistent cache is bypassed: no entry is stored.
CREATE ROW POLICY p_04833 ON t_rp FOR SELECT USING x = 1 TO CURRENT_USER;
SELECT sum(x), 'qc_04833_filtered' FROM t_rp WHERE {CLICKHOUSE_DATABASE:String} != '' SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT 'non-trivial policy not stored', count() = 0 FROM system.query_cache WHERE query LIKE '%qc_04833_filtered%' AND query LIKE '%' || currentDatabase() || '%' AND query NOT LIKE '%system.query_cache%';

-- A policy whose filter is literally always true does not affect the result and does not disable the
-- feature. (When it is later altered to a real condition, the check above starts failing closed, so no
-- stale entry can be served across the change in either direction.)
DROP ROW POLICY p_04833 ON t_rp;
CREATE ROW POLICY p_04833 ON t_rp FOR SELECT USING 1 TO CURRENT_USER;
SELECT sum(x), 'qc_04833_trivial' FROM t_rp WHERE {CLICKHOUSE_DATABASE:String} != '' SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT 'always-true policy stored', count() > 0 FROM system.query_cache WHERE query LIKE '%qc_04833_trivial%' AND query LIKE '%' || currentDatabase() || '%' AND query NOT LIKE '%system.query_cache%';

DROP ROW POLICY p_04833 ON t_rp;
DROP TABLE t_rp;
