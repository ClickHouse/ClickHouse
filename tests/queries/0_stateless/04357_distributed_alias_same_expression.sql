-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106403
-- Two (or more) ALIAS columns expanding to the same expression are deduplicated into a single
-- column on the shard, while the initiator keeps them distinct. This must reconcile without a
-- NUMBER_OF_COLUMNS_DOESNT_MATCH exception on a Distributed table / with parallel replicas.

DROP TABLE IF EXISTS local_same_expr;
DROP TABLE IF EXISTS dist_same_expr;

CREATE TABLE local_same_expr
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x),
    `b` UInt8 ALIAS x + 1
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_same_expr
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x),
    `b` UInt8 ALIAS x + 1
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_same_expr, rand());

INSERT INTO local_same_expr (dt, x) VALUES ('2024-01-01 00:00:00', 7);

SET enable_analyzer = 1;

-- Basic case: two ALIAS columns with same expression + ORDER BY on a non-projected column
SELECT a1, a2 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

SELECT a1, a2 FROM dist_same_expr ORDER BY dt DESC;

SELECT a1, a2, dt FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Renamed ALIAS projections: user applies AS rename
SELECT a1 AS first, a2 AS second FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Repeated ALIAS projection: same ALIAS column selected twice
SELECT a1, a2, a2 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Mixed: ALIAS column and the same expression written directly.
SELECT a1, toString(x) FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Interleaved repetitions.
SELECT a1, a2, a1 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;
SELECT a1, a1, a2, a2 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Pure user-written duplication, no ALIAS columns involved.
SELECT toString(x), toString(x) FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- A third ALIAS column with a different expression must not be collapsed with the others.
SELECT a1, a2, b FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- GROUP BY on duplicate ALIAS columns.
SELECT a1, a2 FROM dist_same_expr GROUP BY a1, a2 ORDER BY a1;
SELECT a1, a2, count() AS c FROM dist_same_expr GROUP BY a1, a2 ORDER BY a1;

-- HAVING referencing a duplicate ALIAS column.
SELECT a1, a2, count() AS c FROM dist_same_expr GROUP BY a1, a2 HAVING a1 != '' ORDER BY a1;

-- Aggregate over a duplicate ALIAS expression.
SELECT a1, toString(x), sum(x) AS s FROM dist_same_expr GROUP BY a1, toString(x) ORDER BY a1;

-- DISTINCT over duplicate ALIAS columns.
SELECT DISTINCT a1, a2 FROM dist_same_expr ORDER BY a1;

-- Same scenarios with parallel replicas reading from the local MergeTree table.
SET allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT a1, a2 FROM local_same_expr ORDER BY dt DESC LIMIT 1;
SELECT a1, a2, count() AS c FROM local_same_expr GROUP BY a1, a2 ORDER BY a1;

SET allow_experimental_parallel_reading_from_replicas = 0;

DROP TABLE dist_same_expr;
DROP TABLE local_same_expr;
