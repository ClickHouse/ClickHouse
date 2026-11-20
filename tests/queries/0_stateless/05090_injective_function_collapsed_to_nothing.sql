-- { echo }
SET enable_analyzer = 1;
SET optimize_injective_functions_in_group_by = 1;
SET optimize_injective_functions_in_limit_by = 1;
SET optimize_truncate_order_by_after_group_by_keys = 1;
-- The carrier is concatAssumeInjective: its injectivity claim is a user opt-in, so unlike a function's own false claim it cannot be removed to make this test pass vacuously.
DROP TABLE IF EXISTS t_nothing_key;
CREATE TABLE t_nothing_key (fs FixedString(2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nothing_key VALUES ('aa'), ('aa'), ('aa'), ('bb'), ('bb'), ('cc'), ('cc');
-- The resolver collapses the result type, so this expression is one constant NULL over all 7 rows.
SELECT toTypeName(concatAssumeInjective(fs, NULL)) FROM t_nothing_key LIMIT 1;
SELECT count() FROM (SELECT DISTINCT concatAssumeInjective(fs, NULL) FROM t_nothing_key);
-- GROUP BY must produce a single group holding every row.
SELECT concatAssumeInjective(fs, NULL) AS k, count() FROM t_nothing_key GROUP BY k;
SELECT count() FROM (SELECT 1 FROM t_nothing_key GROUP BY concatAssumeInjective(fs, NULL)) SETTINGS optimize_injective_functions_in_group_by = 0;
-- The other collapse branch: an argument already typed `Nothing` collapses the result to bare
-- `Nothing`, which no non-empty column can hold, so this branch is asserted on the query tree.
SELECT countIf(explain ILIKE '%concatAssumeInjective%') AS unwrapped_fn_nodes, countIf(explain ILIKE '%GROUP BY%') AS group_by_sections FROM (EXPLAIN QUERY TREE SELECT count() FROM t_nothing_key GROUP BY concatAssumeInjective(fs, assumeNotNull(materialize(NULL))));
SELECT countIf(explain ILIKE '%concatAssumeInjective%') AS unwrapped_fn_nodes, countIf(explain ILIKE '%GROUP BY%') AS group_by_sections FROM (EXPLAIN QUERY TREE SELECT count() FROM t_nothing_key GROUP BY concatAssumeInjective(fs, assumeNotNull(materialize(NULL))) SETTINGS optimize_injective_functions_in_group_by = 0);
-- LIMIT BY must keep a single row.
SELECT count() FROM (SELECT concatAssumeInjective(fs, NULL) AS k FROM t_nothing_key LIMIT 1 BY k);
SELECT count() FROM (SELECT concatAssumeInjective(fs, NULL) AS k FROM t_nothing_key LIMIT 1 BY k) SETTINGS optimize_injective_functions_in_limit_by = 0;
-- ORDER BY must keep the fs DESC tiebreaker, because a constant leading expression does not cover the key.
SELECT countIf(explain ILIKE '%sort_direction: DESCENDING%') AS keeps_tiebreaker, countIf(explain ILIKE '%SORT id%') AS sort_nodes FROM (EXPLAIN QUERY TREE SELECT fs FROM t_nothing_key GROUP BY fs ORDER BY concatAssumeInjective(fs, NULL), fs DESC);
SELECT countIf(explain ILIKE '%sort_direction: DESCENDING%') AS keeps_tiebreaker, countIf(explain ILIKE '%SORT id%') AS sort_nodes FROM (EXPLAIN QUERY TREE SELECT fs FROM t_nothing_key GROUP BY fs ORDER BY concatAssumeInjective(fs, NULL), fs DESC SETTINGS optimize_truncate_order_by_after_group_by_keys = 0);
-- A claim that does hold must still be used: with a non-NULL argument the result type does not
-- collapse, so the key is unwrapped and the redundant tiebreaker is still truncated.
SELECT countIf(explain ILIKE '%concatAssumeInjective%') AS unwrapped_fn_nodes, countIf(explain ILIKE '%GROUP BY%') AS group_by_sections FROM (EXPLAIN QUERY TREE SELECT count() FROM t_nothing_key GROUP BY concatAssumeInjective(fs, 'x'));
SELECT countIf(explain ILIKE '%sort_direction: DESCENDING%') AS keeps_tiebreaker, countIf(explain ILIKE '%SORT id%') AS sort_nodes FROM (EXPLAIN QUERY TREE SELECT fs FROM t_nothing_key GROUP BY fs ORDER BY concatAssumeInjective(fs, 'x'), fs DESC);
DROP TABLE t_nothing_key;
-- A function whose result type cannot be Nullable (Array) evaluates a NULL argument as f(default), so NULL and 0 must stay separate keys.
DROP TABLE IF EXISTS t_injective_array_null;
CREATE TABLE t_injective_array_null (n Nullable(UInt8), m UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_injective_array_null VALUES (NULL, 0), (0, 0), (0, 0), (1, 1);
SELECT bitmaskToArray(n) AS k, count() FROM t_injective_array_null GROUP BY k ORDER BY ALL;
SELECT bitmaskToArray(n) AS k, count() FROM t_injective_array_null GROUP BY k ORDER BY ALL SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT bitPositionsToArray(n) AS k, count() FROM t_injective_array_null GROUP BY k ORDER BY ALL;
SELECT bitmaskToArray(toLowCardinality(n)) AS k, count() FROM t_injective_array_null GROUP BY k ORDER BY ALL SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT count() FROM (SELECT n FROM t_injective_array_null LIMIT 1 BY bitmaskToArray(n));
SELECT count() FROM (SELECT n FROM t_injective_array_null LIMIT 1 BY bitmaskToArray(n)) SETTINGS optimize_injective_functions_in_limit_by = 0;
SELECT n FROM t_injective_array_null GROUP BY n ORDER BY bitmaskToArray(n), n DESC;
SELECT n FROM t_injective_array_null GROUP BY n ORDER BY bitmaskToArray(n), n DESC SETTINGS optimize_truncate_order_by_after_group_by_keys = 0;
-- Without a Nullable argument the claim still holds, so the key is still unwrapped.
SELECT countIf(explain ILIKE '%bitmaskToArray%') AS unwrapped_fn_nodes FROM (EXPLAIN QUERY TREE SELECT count() FROM t_injective_array_null GROUP BY bitmaskToArray(m));
DROP TABLE t_injective_array_null;
