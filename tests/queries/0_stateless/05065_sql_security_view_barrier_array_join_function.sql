-- The `arrayJoin` function is the expression-level twin of the `ARRAY JOIN` clause: an empty array
-- drops its row. `StorageView::canHideRows` used to look only at `arrayJoinExpressionList`, so a
-- `SQL SECURITY DEFINER` / `NONE` view carrying the function form was classified as projection-only
-- and stayed on the fast path, where the invoker's predicate is evaluated on rows the view hides.

SET query_plan_lift_up_array_join = 1, query_plan_filter_push_down = 1, query_plan_merge_expressions = 1, enable_parallel_replicas = 0, max_threads = 1;

DROP TABLE IF EXISTS t05065;
CREATE TABLE t05065 (key UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t05065 SELECT number, if(number = 42, [], [number, number + 1]) FROM numbers(100);

CREATE VIEW v05065_invoker SQL SECURITY INVOKER AS
    SELECT key, arrayJoin(arr) AS item FROM t05065;
CREATE VIEW v05065_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT key, arrayJoin(arr) AS item FROM t05065;
CREATE VIEW v05065_none SQL SECURITY NONE AS
    SELECT key, arrayJoin(arr) AS item FROM t05065;

SET enable_analyzer = 1;

-- The `INVOKER` view stays fully optimizable, so the outer predicate reaches the row that the
-- empty array hides. This is the positive control proving that the oracle discriminates.
SELECT 'invoker, analyzer:';
SELECT count() FROM v05065_invoker WHERE throwIf(key = 42, 'DISCLOSED') = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The barrier views keep the predicate above the row-dropping `arrayJoin`.
SELECT 'definer, analyzer:', count() FROM v05065_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;
SELECT 'none, analyzer:', count() FROM v05065_none WHERE throwIf(key = 42, 'DISCLOSED') = 0;

SET analyzer_inline_views = 1;
SELECT 'definer, analyzer, inline views:', count() FROM v05065_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;
SET analyzer_inline_views = DEFAULT;

SET enable_analyzer = 0;

SELECT 'invoker, legacy analyzer:';
SELECT count() FROM v05065_invoker WHERE throwIf(key = 42, 'DISCLOSED') = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT 'definer, legacy analyzer:', count() FROM v05065_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;
SELECT 'none, legacy analyzer:', count() FROM v05065_none WHERE throwIf(key = 42, 'DISCLOSED') = 0;

SET enable_analyzer = DEFAULT;

-- The barrier only drops the optimization, never the result.
SELECT 'definer results:', count(), min(key), max(key) FROM v05065_definer WHERE key % 2 = 0;

DROP VIEW v05065_invoker;
DROP VIEW v05065_definer;
DROP VIEW v05065_none;
DROP TABLE t05065;
