-- Secondary indices and projections whose expressions were written with redundant parentheses
-- (`INDEX ix (b * c)`, `PROJECTION p (SELECT (b) ...)`, `PROJECTION p (WITH (b + 1) AS y ...)`)
-- must be interchangeable with the same index/projection written without them: they are the same
-- definition, so `ATTACH PARTITION FROM` must not fail with "Tables have different secondary
-- indices" or "Tables have different projections".
-- https://github.com/ClickHouse/ClickHouse/pull/92340 started preserving the parentheses in stored
-- metadata, so a table created by a version that keeps them was rejected against the canonical form.

DROP TABLE IF EXISTS t_idx_src_04612;
DROP TABLE IF EXISTS t_idx_dst_04612;
DROP TABLE IF EXISTS t_prj_src_04612;
DROP TABLE IF EXISTS t_prj_dst_04612;
DROP TABLE IF EXISTS t_neg_src_04612;
DROP TABLE IF EXISTS t_neg_dst_04612;

-- Secondary index: `INDEX ix (b * c)` vs `INDEX ix b * c`.
CREATE TABLE t_idx_src_04612 (a UInt32, b UInt32, c UInt32, INDEX ix (b * c) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_idx_dst_04612 (a UInt32, b UInt32, c UInt32, INDEX ix b * c TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_idx_src_04612 VALUES (1, 1, 1), (1, 2, 2), (2, 3, 3);
ALTER TABLE t_idx_dst_04612 ATTACH PARTITION 1 FROM t_idx_src_04612;
SELECT a, b, c FROM t_idx_dst_04612 ORDER BY a, b, c;

-- Projection: parenthesized SELECT/ORDER BY elements, a WITH-clause alias, and an aliased SELECT
-- element must all compare equal to their unparenthesized form.
CREATE TABLE t_prj_src_04612 (a UInt32, b UInt32, c UInt32,
    PROJECTION p (WITH (b + 1) AS y SELECT (a) AS x, sum(y) GROUP BY (a)))
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_prj_dst_04612 (a UInt32, b UInt32, c UInt32,
    PROJECTION p (WITH b + 1 AS y SELECT a AS x, sum(y) GROUP BY a))
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_prj_src_04612 VALUES (1, 1, 1), (1, 2, 2), (2, 3, 3);
ALTER TABLE t_prj_dst_04612 ATTACH PARTITION 1 FROM t_prj_src_04612;
SELECT a, b, c FROM t_prj_dst_04612 ORDER BY a, b, c;

-- Genuinely different index expressions must still be rejected: canonicalization only strips
-- redundant parentheses, it does not make `b * c` and `b + c` compare equal.
CREATE TABLE t_neg_src_04612 (a UInt32, b UInt32, c UInt32, INDEX ix (b * c) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_neg_dst_04612 (a UInt32, b UInt32, c UInt32, INDEX ix b + c TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_neg_src_04612 VALUES (1, 1, 1);
ALTER TABLE t_neg_dst_04612 ATTACH PARTITION 1 FROM t_neg_src_04612; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_idx_src_04612;
DROP TABLE t_idx_dst_04612;
DROP TABLE t_prj_src_04612;
DROP TABLE t_prj_dst_04612;
DROP TABLE t_neg_src_04612;
DROP TABLE t_neg_dst_04612;
