-- `(fe1 FULL JOIN fe2) LEFT JOIN fe3` may be regrouped as `(fe1 LEFT JOIN fe3) FULL JOIN fe2` only
-- if the `LEFT JOIN` condition rejects nulls on `fe1`, which the `FULL JOIN` pads. Here it does
-- not: `ifNull(fe1.x, 0) = fe3.y` matches a padded `fe1` row with `fe3.y = 0`, so the rows of
-- `fe2` without a match in `fe1` must find `fe3`. The result must be the same as without reordering.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'dpsub,greedy';
SET use_hash_table_stats_for_join_reordering = 0;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS fe1;
DROP TABLE IF EXISTS fe2;
DROP TABLE IF EXISTS fe3;
CREATE TABLE fe1 (k Nullable(UInt32), x Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE fe2 (k2 Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE fe3 (y Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO fe1 VALUES (1, 5);
INSERT INTO fe2 SELECT number + 100 FROM numbers(1000);
INSERT INTO fe3 VALUES (0);

SELECT '-- CD-C';
SELECT count(), countIf(fe3.y = 0)
FROM fe1 FULL JOIN fe2 ON fe1.k = fe2.k2
LEFT JOIN fe3 ON ifNull(fe1.x, 0) = fe3.y
SETTINGS query_plan_optimize_join_order_conflict_detector = 'c';

SELECT '-- CD-A';
SELECT count(), countIf(fe3.y = 0)
FROM fe1 FULL JOIN fe2 ON fe1.k = fe2.k2
LEFT JOIN fe3 ON ifNull(fe1.x, 0) = fe3.y
SETTINGS query_plan_optimize_join_order_conflict_detector = 'a';

DROP TABLE fe1;
DROP TABLE fe2;
DROP TABLE fe3;
