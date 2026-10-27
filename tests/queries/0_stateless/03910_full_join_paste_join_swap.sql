-- Test for exception in PasteJoinTransform when FULL JOIN is combined with PASTE JOIN
-- and query_plan_join_swap_table = 'true'. The join swap changes column ordering in the
-- FULL JOIN output, which can cause wrong number of columns reaching the PASTE JOIN.
--
-- We need join_algorithm = 'hash' (not 'concurrent_hash_join') because optimizeJoinLegacy
-- only swaps when the join is a HashJoin (typeid_cast<const HashJoin *> check).
-- We need query_plan_use_new_logical_join_step = 0 because the new logical join step path
-- marks JoinStep as optimized, causing optimizeJoinLegacy to skip it.
--
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=b373b658edd0a03cb8daacf2c6d77aedd250e7f1&name_0=MasterCI&name_1=Stress%20test%20%28arm_asan%29

SET query_plan_join_swap_table = 'true', join_algorithm = 'hash', query_plan_use_new_logical_join_step = 0, enable_analyzer = 1;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (1);

-- Original failing query from the stress test
SELECT 1 FROM t0 FULL JOIN (SELECT 0 AS c0) tx ON t0.c0 = tx.c0
PASTE JOIN (SELECT 0 AS c0, 1 AS c1) ty ORDER BY ty.c0, ty.c1;

-- SELECT * variant to force all columns through the pipeline
SELECT * FROM t0 FULL JOIN (SELECT 0 AS c0) tx ON t0.c0 = tx.c0
PASTE JOIN (SELECT 0 AS c0, 1 AS c1) ty ORDER BY ty.c0, ty.c1;

-- With join_use_nulls (always set in some stress test threads)
SELECT * FROM t0 FULL JOIN (SELECT 0 AS c0) tx ON t0.c0 = tx.c0
PASTE JOIN (SELECT 0 AS c0, 1 AS c1) ty ORDER BY ty.c0, ty.c1
SETTINGS join_use_nulls = 1;

-- Multiple rows to exercise non-joined rows from both sides of the FULL JOIN
INSERT INTO TABLE t0 (c0) SELECT number FROM numbers(10);

SELECT count(), sum(t0_c0), sum(tx_c0), sum(ty_c0), sum(ty_c1)
FROM (
    SELECT t0.c0 AS t0_c0, tx.c0 AS tx_c0, ty.c0 AS ty_c0, ty.c1 AS ty_c1
    FROM t0 FULL JOIN (SELECT number::UInt8 AS c0 FROM numbers(5)) tx ON t0.c0 = tx.c0
    PASTE JOIN (SELECT number::UInt8 AS c0, (number + 1)::UInt8 AS c1 FROM numbers(20)) ty
    ORDER BY ty.c0, ty.c1
);

-- Without swap for comparison
SELECT count(), sum(t0_c0), sum(tx_c0), sum(ty_c0), sum(ty_c1)
FROM (
    SELECT t0.c0 AS t0_c0, tx.c0 AS tx_c0, ty.c0 AS ty_c0, ty.c1 AS ty_c1
    FROM t0 FULL JOIN (SELECT number::UInt8 AS c0 FROM numbers(5)) tx ON t0.c0 = tx.c0
    PASTE JOIN (SELECT number::UInt8 AS c0, (number + 1)::UInt8 AS c1 FROM numbers(20)) ty
    ORDER BY ty.c0, ty.c1
) SETTINGS query_plan_join_swap_table = 'false';

DROP TABLE t0;
