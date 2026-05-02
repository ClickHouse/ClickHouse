-- Test: trivial count with mixed partition + non-deterministic filter
-- Covers: src/Storages/MergeTree/MergeTreeData.cpp:1702 — null filter_dag guard
--   reached via AND children with allow_partial_result=false:
--     1. partition column child passes canEvaluateSubtree (allowed_inputs=null)
--     2. rand() child returns nullptr at VirtualColumnUtils.cpp:627
--     3. AND with mixed null+valid + allow_partial_result=false → returns nullptr
--   PR test 03002 covers TOP-LEVEL non-deterministic filter only.
DROP TABLE IF EXISTS t_part_rand;
CREATE TABLE t_part_rand (x UInt64, p UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY x;
INSERT INTO t_part_rand SELECT number, number % 4 FROM numbers(1000);
-- AND filter: deterministic partition + non-deterministic rand
SELECT count() > 0 AND count() < 250 FROM t_part_rand WHERE p = 1 AND rand() % 100 < 50;
-- Plain partition filter (deterministic) — exact count
SELECT count() FROM t_part_rand WHERE p = 1;
DROP TABLE t_part_rand;
