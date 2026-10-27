-- Regression test for "Not-ready Set" error when IN (subquery) condition
-- gets moved to PREWHERE by optimizePrewhere after applyFilters already ran.
-- https://github.com/ClickHouse/ClickHouse/issues/100318

-- Pin the optimizer settings that trigger the rewrite this test exercises;
-- otherwise randomized runs may disable PREWHERE move and skip the fixed path.
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;

CREATE TABLE t_100318_log (v0 UInt32) ENGINE = Log;
CREATE TABLE t_100318_mt (v0 UInt32, v1 UInt32, v2 DateTime, PRIMARY KEY(v1)) ENGINE = SummingMergeTree;
CREATE TABLE t_100318_rmt (v0 UInt32, v1 UInt32, PRIMARY KEY(v0)) ENGINE = ReplacingMergeTree;

INSERT INTO t_100318_mt VALUES (13, 23000, '2100-01-05');
INSERT INTO t_100318_mt VALUES (16, 26000, '2066-10-07');
INSERT INTO t_100318_rmt VALUES (91, 101000);

SELECT 1 FROM (SELECT 1 FROM t_100318_log)
WHERE EXISTS (
    SELECT 1
    UNION ALL
    SELECT ref_4.v0 FROM (
        SELECT row_number() OVER (PARTITION BY t_100318_mt.v0) AS c_1
        FROM t_100318_mt
        WHERE t_100318_mt.v2 IN (SELECT 1 FROM t_100318_log)
    ) AS ref_3
    INNER JOIN t_100318_rmt AS ref_4 ON (ref_3.c_1 = ref_4.v0)
);

DROP TABLE t_100318_log;
DROP TABLE t_100318_mt;
DROP TABLE t_100318_rmt;
