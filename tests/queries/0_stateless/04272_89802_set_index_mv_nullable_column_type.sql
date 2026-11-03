-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/89802
-- A `SET` skip index built on top of a non-`Nullable` storage column used to fail with
--   Logical error: 'Unexpected return type from equals. Expected Nullable(UInt8). Got UInt8...'
-- when read through a MaterializedView that declares the column as `Nullable` and
-- `query_plan_merge_expressions = 0` keeps the MV's `_CAST` step separate from the WHERE filter.
-- The bug was that `MergeTreeIndexConditionSet` reused the predicate's pre-resolved function on
-- the granule block, whose column carries the storage (non-`Nullable`) type.

DROP TABLE IF EXISTS t_89802;
DROP TABLE IF EXISTS v_89802;

CREATE TABLE t_89802 (c0 Int, INDEX i0 c0 TYPE set(0)) ENGINE = MergeTree() ORDER BY tuple();
CREATE MATERIALIZED VIEW v_89802 TO t_89802 (c0 Nullable(Int)) AS (SELECT 1 c0);
INSERT INTO TABLE t_89802 (c0) VALUES (1), (2), (3), (10);

SELECT 'no_match', count() FROM v_89802 WHERE c0 = 0 SETTINGS query_plan_merge_expressions = 0;
SELECT 'match', count() FROM v_89802 WHERE c0 = 1 SETTINGS query_plan_merge_expressions = 0;
SELECT 'multi_match', count() FROM v_89802 WHERE c0 IN (1, 2, 100) SETTINGS query_plan_merge_expressions = 0;
SELECT 'and_pred', count() FROM v_89802 WHERE c0 = 1 AND c0 < 10 SETTINGS query_plan_merge_expressions = 0;
SELECT 'or_pred', count() FROM v_89802 WHERE c0 = 1 OR c0 = 2 SETTINGS query_plan_merge_expressions = 0;
SELECT 'not_pred', count() FROM v_89802 WHERE NOT (c0 = 100) SETTINGS query_plan_merge_expressions = 0;

-- Same checks with `query_plan_merge_expressions = 1` to confirm the result-shape is identical.
SELECT 'no_match_m1', count() FROM v_89802 WHERE c0 = 0 SETTINGS query_plan_merge_expressions = 1;
SELECT 'match_m1', count() FROM v_89802 WHERE c0 = 1 SETTINGS query_plan_merge_expressions = 1;

-- Wider type difference (UInt8 storage vs Nullable(Int32) MV).
DROP TABLE IF EXISTS t_89802_u8;
DROP TABLE IF EXISTS v_89802_u8;
CREATE TABLE t_89802_u8 (c0 UInt8, INDEX i0 c0 TYPE set(0)) ENGINE = MergeTree() ORDER BY tuple();
CREATE MATERIALIZED VIEW v_89802_u8 TO t_89802_u8 (c0 Nullable(Int32)) AS (SELECT 1 c0);
INSERT INTO TABLE t_89802_u8 (c0) VALUES (5), (10);

SELECT 'u8_match', count() FROM v_89802_u8 WHERE c0 = 5 SETTINGS query_plan_merge_expressions = 0;
SELECT 'u8_no_match', count() FROM v_89802_u8 WHERE c0 = 99 SETTINGS query_plan_merge_expressions = 0;

DROP TABLE v_89802;
DROP TABLE t_89802;
DROP TABLE v_89802_u8;
DROP TABLE t_89802_u8;
