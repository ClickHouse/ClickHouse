-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- Lazy materialization for ORDER BY ... LIMIT applies inside a distributed-plan fragment: the
-- fragment carries the local top-N bound, and the worker defers the wide column to a second read of
-- only the rows its local top-N kept. Results must be identical to the non-distributed plan.

SET enable_analyzer = 1, enable_parallel_replicas = 0;
SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000;
SET distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4;
SET distributed_plan_max_rows_to_broadcast = 1000;

DROP TABLE IF EXISTS t_dist_lazy;

CREATE TABLE t_dist_lazy (a UInt64, b String, payload String) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1024;

SYSTEM STOP MERGES t_dist_lazy;

INSERT INTO t_dist_lazy SELECT number, toString(number), repeat('p', 200) FROM numbers(100000);
INSERT INTO t_dist_lazy SELECT number + 100000, toString(number), repeat('q', 200) FROM numbers(100000);

-- The local top-N bound must sit inside the fragment, below the gather. Without it the worker loses
-- the limit (SortingStep does not serialize it) and sorts its whole bucket.
SELECT 'local top-N in fragment', countIf(explain LIKE '%Limit (local top-N)%') > 0
FROM (EXPLAIN PLAN distributed = 1 SELECT a, payload FROM t_dist_lazy ORDER BY b, a LIMIT 5 SETTINGS make_distributed_plan = 1);

SELECT 'ORDER BY local', a, b, substring(payload, 1, 4) FROM t_dist_lazy ORDER BY b, a LIMIT 5
SETTINGS make_distributed_plan = 0;
SELECT 'ORDER BY distributed', a, b, substring(payload, 1, 4) FROM t_dist_lazy ORDER BY b, a LIMIT 5
SETTINGS make_distributed_plan = 1;

SELECT 'DESC local', a, substring(payload, 1, 4) FROM t_dist_lazy ORDER BY b DESC, a DESC LIMIT 5
SETTINGS make_distributed_plan = 0;
SELECT 'DESC distributed', a, substring(payload, 1, 4) FROM t_dist_lazy ORDER BY b DESC, a DESC LIMIT 5
SETTINGS make_distributed_plan = 1;

-- The coordinator applies OFFSET once; each bucket keeps its top (limit + offset) rows.
SELECT 'OFFSET local', a, substring(payload, 1, 4) FROM t_dist_lazy ORDER BY b, a LIMIT 5 OFFSET 25
SETTINGS make_distributed_plan = 0;
SELECT 'OFFSET distributed', a, substring(payload, 1, 4) FROM t_dist_lazy ORDER BY b, a LIMIT 5 OFFSET 25
SETTINGS make_distributed_plan = 1;

-- A filter splits into a main half (evaluated before the local limit) and a lazy half.
SELECT 'FILTER local', a, substring(payload, 1, 4) FROM t_dist_lazy WHERE b LIKE '5%' ORDER BY b, a LIMIT 5
SETTINGS make_distributed_plan = 0;
SELECT 'FILTER distributed', a, substring(payload, 1, 4) FROM t_dist_lazy WHERE b LIKE '5%' ORDER BY b, a LIMIT 5
SETTINGS make_distributed_plan = 1;

SELECT 'EXPRESSION local', a, substring(payload, 1, 4) FROM t_dist_lazy ORDER BY a % 7, b, a LIMIT 5
SETTINGS make_distributed_plan = 0;
SELECT 'EXPRESSION distributed', a, substring(payload, 1, 4) FROM t_dist_lazy ORDER BY a % 7, b, a LIMIT 5
SETTINGS make_distributed_plan = 1;

-- A limit above the lazy materialization threshold: the rewrite does not apply, results stay correct.
SELECT 'BEYOND THRESHOLD local', a, substring(payload, 1, 4) FROM t_dist_lazy ORDER BY b, a LIMIT 3 OFFSET 199997
SETTINGS make_distributed_plan = 0;
SELECT 'BEYOND THRESHOLD distributed', a, substring(payload, 1, 4) FROM t_dist_lazy ORDER BY b, a LIMIT 3 OFFSET 199997
SETTINGS make_distributed_plan = 1;

SELECT 'WITH TIES local', count() FROM (SELECT a FROM t_dist_lazy ORDER BY a % 7 LIMIT 3 WITH TIES SETTINGS make_distributed_plan = 0);
SELECT 'WITH TIES distributed', count() FROM (SELECT a FROM t_dist_lazy ORDER BY a % 7 LIMIT 3 WITH TIES SETTINGS make_distributed_plan = 1);

-- `payload` must not be read for every scanned row. Read a key-only query as the baseline for what
-- the sort itself costs, then compare the extra cost of `payload` with and without the rewrite.
-- Top-K dynamic filtering reaches the same saving by a different route (it prunes rows during the
-- read), which would leave the `query_plan_optimize_lazy_materialization = 0` run nothing to be a
-- baseline for, so pin it off here - the sort key is a `String`, i.e. a variable-length type.
SELECT a FROM t_dist_lazy ORDER BY b, a LIMIT 5 FORMAT Null
SETTINGS make_distributed_plan = 1, use_top_k_dynamic_filtering = 0, log_comment = '04665_key_only';

SELECT a, payload FROM t_dist_lazy ORDER BY b, a LIMIT 5 FORMAT Null
SETTINGS make_distributed_plan = 1, use_top_k_dynamic_filtering = 0, log_comment = '04665_lazy_on';

SELECT a, payload FROM t_dist_lazy ORDER BY b, a LIMIT 5 FORMAT Null
SETTINGS make_distributed_plan = 1, use_top_k_dynamic_filtering = 0,
    query_plan_optimize_lazy_materialization = 0, log_comment = '04665_lazy_off';

SYSTEM FLUSH LOGS query_log;

-- The bytes are read by the fragments, and each fragment logs its own `system.query_log` entry with
-- its own `query_id` - the coordinator's entry only counts the rows it received. So resolve each
-- query's `initial_query_id` from its coordinator entry (the one in this database), then sum over
-- every entry of that query. `argMax` keeps a repeated run of this test from accumulating.
WITH
    (SELECT argMax(initial_query_id, event_time_microseconds) FROM system.query_log
     WHERE event_date >= yesterday() AND type = 'QueryFinish'
       AND current_database = currentDatabase() AND log_comment = '04665_key_only') AS key_only_query,
    (SELECT argMax(initial_query_id, event_time_microseconds) FROM system.query_log
     WHERE event_date >= yesterday() AND type = 'QueryFinish'
       AND current_database = currentDatabase() AND log_comment = '04665_lazy_on') AS lazy_on_query,
    (SELECT argMax(initial_query_id, event_time_microseconds) FROM system.query_log
     WHERE event_date >= yesterday() AND type = 'QueryFinish'
       AND current_database = currentDatabase() AND log_comment = '04665_lazy_off') AS lazy_off_query
SELECT 'payload almost free', (payload_bytes - key_only_bytes) * 4 < (no_lazy_bytes - key_only_bytes)
FROM
(
    SELECT
        sumIf(read_bytes, initial_query_id = key_only_query) AS key_only_bytes,
        sumIf(read_bytes, initial_query_id = lazy_on_query) AS payload_bytes,
        sumIf(read_bytes, initial_query_id = lazy_off_query) AS no_lazy_bytes
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish'
);

-- Optimization should not be shipped in distributed plan if exact_rows_before_limit is set
SELECT 'local top-N in fragment with exact_rows_before_limit=1', countIf(explain LIKE '%Limit (local top-N)%') > 0
FROM (EXPLAIN PLAN distributed = 1 SELECT a, payload FROM t_dist_lazy ORDER BY b, a LIMIT 5 SETTINGS make_distributed_plan = 1, exact_rows_before_limit = 1);

DROP TABLE t_dist_lazy;
