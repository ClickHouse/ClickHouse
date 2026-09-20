-- Regression test: GLOBAL IN (subquery) must not be moved to PREWHERE,
-- because GLOBAL IN sets are populated via external tables attached by ReadFromRemote
-- and cannot be built synchronously during PREWHERE evaluation.
-- Also covers null-aware variants (globalNullIn/globalNotNullIn) via transform_null_in.
-- https://github.com/ClickHouse/ClickHouse/pull/100375

-- Pin the optimizer settings that drive PREWHERE assignment so that randomized
-- runs which disable them do not bypass the `cannotBeMoved` guard under test.
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET transform_null_in = 1;

CREATE TABLE t_100375_mt (v0 UInt32, v1 UInt32, v2 Nullable(DateTime), PRIMARY KEY(v1)) ENGINE = SummingMergeTree;
CREATE TABLE t_100375_log (v0 UInt32) ENGINE = Log;

INSERT INTO t_100375_mt VALUES (13, 23000, '2100-01-05');
INSERT INTO t_100375_mt VALUES (16, 26000, '2066-10-07');

SELECT 1 FROM (SELECT 1 FROM t_100375_log)
WHERE EXISTS (
    SELECT 1
    UNION ALL
    SELECT ref_4.v0 FROM (
        SELECT row_number() OVER (PARTITION BY t_100375_mt.v0) AS c_1
        FROM t_100375_mt
        WHERE t_100375_mt.v2 GLOBAL IN (SELECT 1 FROM t_100375_log)
    ) AS ref_3
    INNER JOIN t_100375_mt AS ref_4 ON (ref_3.c_1 = ref_4.v0)
);

DROP TABLE t_100375_mt;
DROP TABLE t_100375_log;
