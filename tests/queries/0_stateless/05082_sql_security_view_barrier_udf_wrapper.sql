-- `StorageView::canHideRows` classifies the stored view AST before SQL user-defined functions are
-- expanded (`UserDefinedSQLFunctionVisitor` on the legacy path, `resolveFunction` on the analyzer
-- path). A `SQL SECURITY DEFINER` / `NONE` view whose only row-hiding construct sits behind a UDF
-- wrapper - `CREATE FUNCTION f AS (a) -> arrayJoin(a)` - used to be classified as projection-only
-- and stayed on the fast path, where the invoker's predicate is evaluated on rows the view hides.
-- The classifier now descends into SQL UDF bodies, recursively.

SET query_plan_lift_up_array_join = 1, query_plan_filter_push_down = 1, query_plan_merge_expressions = 1, enable_parallel_replicas = 0, max_threads = 1;

DROP TABLE IF EXISTS t05082;
CREATE TABLE t05082 (key UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t05082 SELECT number, if(number = 42, [], [number, number + 1]) FROM numbers(100);

DROP FUNCTION IF EXISTS f05082_array_join;
DROP FUNCTION IF EXISTS f05082_nested;
DROP FUNCTION IF EXISTS f05082_sum;
DROP FUNCTION IF EXISTS f05082_plain;
CREATE FUNCTION f05082_array_join AS (a) -> arrayJoin(a);
CREATE FUNCTION f05082_nested AS (a) -> f05082_array_join(a) + 0;
CREATE FUNCTION f05082_sum AS (x) -> sum(x);
CREATE FUNCTION f05082_plain AS (x) -> x + 1;

CREATE VIEW v05082_invoker SQL SECURITY INVOKER AS
    SELECT key, f05082_array_join(arr) AS item FROM t05082;
CREATE VIEW v05082_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT key, f05082_array_join(arr) AS item FROM t05082;
CREATE VIEW v05082_none SQL SECURITY NONE AS
    SELECT key, f05082_array_join(arr) AS item FROM t05082;
CREATE VIEW v05082_nested_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT key, f05082_nested(arr) AS item FROM t05082;

SET enable_analyzer = 1;

-- The `INVOKER` view stays fully optimizable, so the outer predicate reaches the row that the
-- empty array hides. This is the positive control proving that the oracle discriminates.
SELECT 'invoker, analyzer:';
SELECT count() FROM v05082_invoker WHERE throwIf(key = 42, 'DISCLOSED') = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The barrier views keep the predicate above the row-dropping `arrayJoin` hidden in the UDF.
SELECT 'definer, analyzer:', count() FROM v05082_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;
SELECT 'none, analyzer:', count() FROM v05082_none WHERE throwIf(key = 42, 'DISCLOSED') = 0;
SELECT 'nested udf definer, analyzer:', count() FROM v05082_nested_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;

SET analyzer_inline_views = 1;
SELECT 'definer, analyzer, inline views:', count() FROM v05082_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;
SELECT 'nested udf definer, analyzer, inline views:', count() FROM v05082_nested_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;
SET analyzer_inline_views = DEFAULT;

SET enable_analyzer = 0;

SELECT 'invoker, legacy analyzer:';
SELECT count() FROM v05082_invoker WHERE throwIf(key = 42, 'DISCLOSED') = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT 'definer, legacy analyzer:', count() FROM v05082_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;
SELECT 'none, legacy analyzer:', count() FROM v05082_none WHERE throwIf(key = 42, 'DISCLOSED') = 0;
SELECT 'nested udf definer, legacy analyzer:', count() FROM v05082_nested_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;

SET enable_analyzer = 1;

-- The barrier only drops the optimization, never the result.
SELECT 'definer results:', count(), min(key), max(key) FROM v05082_definer WHERE key % 2 = 0;

-- The sibling carrier from the same root cause: a UDF that expands to an aggregate without
-- `GROUP BY` collapses the rows. An outer predicate never moves below an aggregation, so there is
-- no pushdown oracle; the classification is observable through `analyzer_inline_views` instead.
-- A barrier view that provably hides no rows is inlined and disappears from the query tree, while
-- one that hides rows stays a table expression.
CREATE VIEW v05082_plain_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT f05082_plain(key) AS k FROM t05082;
CREATE VIEW v05082_sum_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT f05082_sum(key) AS s FROM t05082;

SET analyzer_inline_views = 1;
SELECT 'plain udf definer kept as a table expression:', countIf(explain LIKE '%table_name: %v05082_plain_definer%')
    FROM (EXPLAIN QUERY TREE SELECT k FROM v05082_plain_definer);
SELECT 'sum udf definer kept as a table expression:', countIf(explain LIKE '%table_name: %v05082_sum_definer%')
    FROM (EXPLAIN QUERY TREE SELECT s FROM v05082_sum_definer);
SELECT 'sum udf definer result:', s FROM v05082_sum_definer;
SET analyzer_inline_views = DEFAULT;

SET enable_analyzer = DEFAULT;

DROP VIEW v05082_invoker;
DROP VIEW v05082_definer;
DROP VIEW v05082_none;
DROP VIEW v05082_nested_definer;
DROP VIEW v05082_plain_definer;
DROP VIEW v05082_sum_definer;
DROP FUNCTION f05082_array_join;
DROP FUNCTION f05082_nested;
DROP FUNCTION f05082_sum;
DROP FUNCTION f05082_plain;
DROP TABLE t05082;
