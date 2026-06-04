-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106403
-- Two ALIAS columns expanding to the same expression should work with ORDER BY
-- on a Distributed table.

DROP TABLE IF EXISTS local_same_expr;
DROP TABLE IF EXISTS dist_same_expr;

CREATE TABLE local_same_expr
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x)
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_same_expr
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x)
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_same_expr, rand());

INSERT INTO local_same_expr VALUES ('2024-01-01 00:00:00', 7);

-- Basic case: two ALIAS columns with same expression + ORDER BY
SELECT a1, a2 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

SELECT a1, a2 FROM dist_same_expr ORDER BY dt DESC;

SELECT a1, a2, dt FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Renamed ALIAS projections: user applies AS rename
SELECT a1 AS first, a2 AS second FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Repeated ALIAS projection: same ALIAS column selected twice (should not be wrapped twice)
SELECT a1, a2, a2 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

DROP TABLE dist_same_expr;
DROP TABLE local_same_expr;
