-- Tags: no-random-settings, no-fasttest, no-parallel-replicas, no-old-analyzer
-- Verify that TopN aggregation rejects companion aggregates whose determining
-- argument column differs from the ORDER BY aggregate's argument.
-- See: https://github.com/ClickHouse/ClickHouse/issues/75098

DROP TABLE IF EXISTS t_topn_mismatch;
CREATE TABLE t_topn_mismatch (grp UInt32, ts UInt64, val UInt64, payload String)
ENGINE = MergeTree ORDER BY ts;

INSERT INTO t_topn_mismatch SELECT
    number % 50 AS grp,
    number AS ts,
    cityHash64(number) % 10000 AS val,
    'p' || toString(number) AS payload
FROM numbers(5000);

-- argMax(payload, val) alongside max(ts): val != ts → must NOT optimize
SELECT '-- EXPLAIN argMax mismatched arg (should NOT optimize)';
EXPLAIN PLAN
SELECT grp, max(ts) AS m, argMax(payload, val) AS p
FROM t_topn_mismatch
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- max(val) alongside max(ts): val != ts → must NOT optimize
SELECT '-- EXPLAIN max mismatched arg (should NOT optimize)';
EXPLAIN PLAN
SELECT grp, max(ts) AS m, max(val) AS m2
FROM t_topn_mismatch
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- argMax(payload, ts) alongside max(ts): ts == ts → SHOULD optimize
SELECT '-- EXPLAIN argMax same arg (should optimize)';
EXPLAIN PLAN
SELECT grp, max(ts) AS m, argMax(payload, ts) AS p
FROM t_topn_mismatch
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- Correctness: argMax(payload, val) with mismatched arg gives correct results
-- (falls back to standard pipeline)
SELECT '-- correctness: mismatched companion optimized';
SELECT grp, max(ts) AS m, argMax(payload, val) AS p
FROM t_topn_mismatch
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- correctness: mismatched companion reference';
SELECT grp, max(ts) AS m, argMax(payload, val) AS p
FROM t_topn_mismatch
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- any() companion with different column is OK (any takes any row)
SELECT '-- EXPLAIN any companion (should optimize)';
EXPLAIN PLAN
SELECT grp, max(ts) AS m, any(val) AS v
FROM t_topn_mismatch
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

DROP TABLE t_topn_mismatch;
