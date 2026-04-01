-- Tags: no-random-settings, no-parallel-replicas

-- EXPLAIN checks for TopN aggregation optimization.
-- Separated from correctness tests so that random settings fuzzing
-- can run against the correctness file without breaking EXPLAIN expectations.

DROP TABLE IF EXISTS t_topn;

CREATE TABLE t_topn (trace_id String, start_time DateTime, service_name String, value UInt64)
ENGINE = MergeTree ORDER BY start_time;

INSERT INTO t_topn SELECT
    'trace_' || toString(number % 1000),
    toDateTime('2024-01-01') + number,
    'service_' || toString(number % 5),
    number
FROM numbers(10000);

DROP TABLE IF EXISTS t_topn_unsorted;

CREATE TABLE t_topn_unsorted (trace_id String, start_time DateTime, value UInt64)
ENGINE = MergeTree ORDER BY trace_id;

INSERT INTO t_topn_unsorted SELECT
    'trace_' || toString(number % 100),
    toDateTime('2024-01-01') + number,
    number
FROM numbers(1000);

DROP TABLE IF EXISTS t_topn_argminmax;
CREATE TABLE t_topn_argminmax (grp String, ts DateTime, payload String)
ENGINE = MergeTree ORDER BY ts;

INSERT INTO t_topn_argminmax SELECT
    'g' || toString(number % 50),
    toDateTime('2024-01-01') + number,
    'payload_' || toString(number)
FROM numbers(500);

DROP TABLE IF EXISTS t_topn_lc;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_topn_lc (grp String, val LowCardinality(UInt64))
ENGINE = MergeTree ORDER BY grp;
SET allow_suspicious_low_cardinality_types = 0;

INSERT INTO t_topn_lc SELECT
    'g' || toString(number % 200),
    number
FROM numbers(10000);

-- Positive: TopNAggregating appears for max DESC
SELECT '-- EXPLAIN PLAN: has TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT trace_id, max(start_time) AS m
    FROM t_topn
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

-- Negative: incompatible aggregate (count)
SELECT '-- EXPLAIN with count: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT trace_id, max(start_time) AS m, count(*) AS c
    FROM t_topn
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

-- Negative: WITH TIES
SELECT '-- EXPLAIN WITH TIES: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT trace_id, max(start_time) AS m
    FROM t_topn
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5 WITH TIES
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

-- Negative: wrong sort direction (max + ASC)
SELECT '-- EXPLAIN max ASC: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT trace_id, max(start_time) AS m
    FROM t_topn
    GROUP BY trace_id
    ORDER BY m ASC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

-- Negative: wrong sort direction (min + DESC)
SELECT '-- EXPLAIN min DESC: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT trace_id, min(start_time) AS m
    FROM t_topn
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

-- Negative: multi-column ORDER BY
SELECT '-- EXPLAIN multi-column ORDER BY: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT trace_id, max(start_time) AS m
    FROM t_topn
    GROUP BY trace_id
    ORDER BY m DESC, trace_id ASC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

-- Negative: OFFSET
SELECT '-- EXPLAIN LIMIT OFFSET: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT trace_id, max(start_time) AS m
    FROM t_topn
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5 OFFSET 10
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

-- Mode 1: sorted input
SELECT '-- mode 1: has TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT trace_id, max(start_time) AS m
    FROM t_topn
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 3
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

SELECT '-- mode 1: sorted input true';
SELECT count() > 0 FROM (
    EXPLAIN actions=1
    SELECT trace_id, max(start_time) AS m
    FROM t_topn
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 3
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%Sorted input: true%';

-- argMin: output sort != determining column, should NOT optimize
SELECT '-- EXPLAIN argMin: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT grp, argMin(payload, ts) AS earliest
    FROM t_topn_argminmax
    GROUP BY grp
    ORDER BY earliest ASC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

-- Pruning level 0: Mode 2 disabled
SELECT '-- EXPLAIN pruning level 0: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN SELECT trace_id, max(start_time) AS m
    FROM t_topn_unsorted
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 0
) WHERE explain LIKE '%TopNAggregating%';

-- Pruning level 1: Mode 2 enabled
SELECT '-- EXPLAIN pruning level 1: has TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN SELECT trace_id, max(start_time) AS m
    FROM t_topn_unsorted
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 1
) WHERE explain LIKE '%TopNAggregating%';

-- Max-limit gate: large LIMIT should disable Mode 2
SELECT '-- EXPLAIN max-limit gate default: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT trace_id, max(start_time) AS m
    FROM t_topn_unsorted
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5000
    SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 2, use_top_k_dynamic_filtering = 1
) WHERE explain LIKE '%TopNAggregating%';

-- Raising topn_aggregation_max_limit should re-enable Mode 2
SELECT '-- EXPLAIN max-limit gate override: has TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT trace_id, max(start_time) AS m
    FROM t_topn_unsorted
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5000
    SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 2, use_top_k_dynamic_filtering = 1, topn_aggregation_max_limit = 10000
) WHERE explain LIKE '%TopNAggregating%';

-- Verify threshold pruning and __topKFilter prewhere
SELECT '-- EXPLAIN threshold pruning: has TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN actions=1
    SELECT trace_id, max(start_time) AS m
    FROM t_topn_unsorted
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1, use_top_k_dynamic_filtering = 1
) WHERE explain LIKE '%TopNAggregating%';

SELECT '-- EXPLAIN threshold pruning: has Threshold pruning true';
SELECT count() > 0 FROM (
    EXPLAIN actions=1
    SELECT trace_id, max(start_time) AS m
    FROM t_topn_unsorted
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1, use_top_k_dynamic_filtering = 1
) WHERE explain LIKE '%Threshold pruning: true%';

SELECT '-- EXPLAIN threshold pruning: has __topKFilter prewhere';
SELECT count() > 0 FROM (
    EXPLAIN actions=1
    SELECT trace_id, max(start_time) AS m
    FROM t_topn_unsorted
    GROUP BY trace_id
    ORDER BY m DESC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1, use_top_k_dynamic_filtering = 1
) WHERE explain LIKE '%__topKFilter%';

-- LowCardinality: optimization applies
SELECT '-- LowCardinality Mode 2: has TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT grp, max(val) AS m
    FROM t_topn_lc
    GROUP BY grp
    ORDER BY m DESC
    LIMIT 5
    SETTINGS optimize_topn_aggregation = 1
) WHERE explain LIKE '%TopNAggregating%';

DROP TABLE t_topn;
DROP TABLE t_topn_unsorted;
DROP TABLE t_topn_argminmax;
DROP TABLE t_topn_lc;
