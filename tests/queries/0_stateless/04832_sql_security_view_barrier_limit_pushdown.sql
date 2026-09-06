-- A `SQL SECURITY DEFINER` / `NONE` view that can hide rows is a barrier for the LIMIT
-- pushdown too: `tryPushDownLimit` moves the invoker's `LimitStep` below row-preserving
-- steps, and once it crossed the view's sealing step it seeds `DistinctStep::limit_hint`
-- (or a sorting limit), which stops reading the source by the rows the view itself drops
-- or collapses. `optimizeLimitForAggregationInOrder` walks the same chain to seed
-- `AggregatingStep::limit_hint`. Both walks now fail closed on a barrier step.

-- Pin everything the plan shape and the `read_rows` comparison depend on: the test also
-- runs with randomized settings. A single thread and the read-path injections pinned off
-- keep `read_rows` exactly reproducible; none of them affects what the barrier guards.
SET query_plan_push_down_limit = 1, optimize_distinct_in_order = 1,
    optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
    optimize_sorting_by_input_stream_properties = 1,
    enable_parallel_replicas = 0, make_distributed_plan = 0,
    max_threads = 1,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    page_cache_inject_eviction = 0;

DROP TABLE IF EXISTS t04832;
CREATE TABLE t04832 (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04832 SELECT number % 2 FROM numbers(1000);

CREATE VIEW v04832_invoker SQL SECURITY INVOKER AS SELECT DISTINCT x FROM t04832;
CREATE VIEW v04832_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT DISTINCT x FROM t04832;

-- For the `INVOKER` view the limit crosses the converting expression (0): the expression is
-- merged upwards, so it appears above the `LimitStep`, and the limit sits directly on the
-- `DistinctStep` where it seeds the hint. For the `DEFINER` view the sealing step is a
-- barrier: the limit must stay above it. Before the fix the definer line was 0 as well.
SELECT 'invoker twin, limit stays above the converting expression:',
       minIf(n, explain LIKE '%Limit (preliminary LIMIT)%') < minIf(n, explain LIKE '%Convert VIEW subquery result%')
FROM (SELECT explain, rowNumberInAllBlocks() AS n FROM (EXPLAIN compact = 0 SELECT * FROM v04832_invoker LIMIT 2));

SELECT 'definer: limit stays above the seal:',
       minIf(n, explain LIKE '%Limit (preliminary LIMIT)%') < minIf(n, explain LIKE '%Convert VIEW subquery result%')
FROM (SELECT explain, rowNumberInAllBlocks() AS n FROM (EXPLAIN compact = 0 SELECT * FROM v04832_definer LIMIT 2));

-- The barrier only drops the optimization, never the correctness of the result.
SELECT 'definer view results:', arraySort(groupArray(x)) = [0, 1] FROM (SELECT x FROM v04832_definer LIMIT 2);

-- The same contract with the old analyzer, where the view is read through `StorageView::read`.
SET enable_analyzer = 0;

SELECT 'old analyzer, invoker twin, limit stays above the converting expression:',
       minIf(n, explain LIKE '%Limit (preliminary LIMIT)%') < minIf(n, explain LIKE '%Convert VIEW subquery result%')
FROM (SELECT explain, rowNumberInAllBlocks() AS n FROM (EXPLAIN compact = 0 SELECT * FROM v04832_invoker LIMIT 2));

SELECT 'old analyzer, definer: limit stays above the seal:',
       minIf(n, explain LIKE '%Limit (preliminary LIMIT)%') < minIf(n, explain LIKE '%Convert VIEW subquery result%')
FROM (SELECT explain, rowNumberInAllBlocks() AS n FROM (EXPLAIN compact = 0 SELECT * FROM v04832_definer LIMIT 2));

SET enable_analyzer = DEFAULT;

DROP VIEW v04832_invoker;
DROP VIEW v04832_definer;
DROP TABLE t04832;

-- `read_rows` must not depend on how the rows the view collapses are distributed. Twin
-- tables with the identical visible result — the group keys 0 and 1 — but opposite raw
-- multiplicities: in one the first group holds a single row, in the other almost all of
-- them. A limit hint seeded on the view's own in-order aggregation would stop reading at
-- the first group boundary and the two reads would diverge.
CREATE TABLE t04832_a (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t04832_b (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t04832_a SELECT if(number = 0, 0, 1) FROM numbers(300000);
INSERT INTO t04832_b SELECT if(number = 299999, 1, 0) FROM numbers(300000);

CREATE VIEW v04832_a DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k FROM t04832_a GROUP BY k;
CREATE VIEW v04832_b DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k FROM t04832_b GROUP BY k;

SELECT k FROM v04832_a ORDER BY k LIMIT 1 SETTINGS log_comment = '04832_probe_small_first_group';
SELECT k FROM v04832_b ORDER BY k LIMIT 1 SETTINGS log_comment = '04832_probe_large_first_group';

SYSTEM FLUSH LOGS query_log;
-- `count() != 2` guards against the comparison passing vacuously on an empty match.
SELECT 'reading the view costs the same whatever it collapses:', multiIf(
        count() != 2, 'MISSING',
        anyIf(read_rows, log_comment = '04832_probe_small_first_group') = anyIf(read_rows, log_comment = '04832_probe_large_first_group'),
        'same', 'DISCLOSED')
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment LIKE '04832_probe_%' AND type = 'QueryFinish';

DROP VIEW v04832_a;
DROP VIEW v04832_b;
DROP TABLE t04832_a;
DROP TABLE t04832_b;
