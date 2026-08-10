-- Regression test for the top-K `ORDER BY ... LIMIT` optimization with a row policy.
--
-- A row policy restricts rows inside the reader, just like a `WHERE` / `PREWHERE`, but `tryOptimizeTopK`
-- decided `where_clause` from the plan-visible filters only, so a query filtered by a policy alone took
-- the unfiltered fast path: `perform_top_k_optimization` narrowed the read to the marks holding the
-- smallest sort key values, the policy discarded every row in them, and the query returned fewer rows
-- than the `LIMIT` - here nothing at all instead of 100, 101, 102.

DROP ROW POLICY IF EXISTS rp_04812 ON t_04812;
DROP TABLE IF EXISTS t_04812;

CREATE TABLE t_04812 (key UInt64, INDEX mm_key key TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;

INSERT INTO t_04812 SELECT number FROM numbers(300);

-- Drops the first 100 rows by the sort key `key`, so the first surviving row is `key` = 100.
CREATE ROW POLICY rp_04812 ON t_04812 FOR SELECT USING key >= 100 TO ALL;

-- Must return the first three surviving rows in `key` order, never fewer, on both analyzers.
--
-- Every setting the narrowing depends on is pinned, because the test runner randomizes them and the
-- bug only shows with the values below: `use_skip_indexes_on_data_read = 0` skips the narrowing
-- altogether, and `query_plan_max_limit_for_top_k_optimization` below the `LIMIT` disables the
-- optimization. All of them are pinned to their default values, so the query is the one a user runs.
SELECT key FROM t_04812 ORDER BY key LIMIT 3
    SETTINGS enable_analyzer = 0, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1,
             query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1, enable_parallel_replicas = 0;

SELECT '--';

SELECT key FROM t_04812 ORDER BY key LIMIT 3
    SETTINGS enable_analyzer = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1,
             query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1, enable_parallel_replicas = 0;

DROP ROW POLICY rp_04812 ON t_04812;
DROP TABLE t_04812;
