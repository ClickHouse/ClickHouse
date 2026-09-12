-- `additional_table_filters` keyed by a view is applied by `PlannerJoinTree`, which only looks at
-- table expressions. `analyzer_inline_views = 1` used to replace the view with its query tree
-- first, and the filter silently disappeared - a wrong result for an `INVOKER` view and a lost
-- barrier for a `SQL SECURITY DEFINER` / `NONE` one.

SET enable_analyzer = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t05066;
CREATE TABLE t05066 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t05066 SELECT number FROM numbers(5);

CREATE VIEW v05066_invoker SQL SECURITY INVOKER AS SELECT k FROM t05066;
CREATE VIEW v05066_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k FROM t05066;

SELECT 'invoker, no inlining:', arraySort(groupArray(k)) FROM v05066_invoker
SETTINGS analyzer_inline_views = 0, additional_table_filters = {'v05066_invoker': 'k != 2'};

SELECT 'invoker, inlining:', arraySort(groupArray(k)) FROM v05066_invoker
SETTINGS analyzer_inline_views = 1, additional_table_filters = {'v05066_invoker': 'k != 2'};

SELECT 'definer, inlining:', arraySort(groupArray(k)) FROM v05066_definer
SETTINGS analyzer_inline_views = 1, additional_table_filters = {'v05066_definer': 'k != 2'};

-- A view that is not named by the setting keeps being inlined, and the filter of another table
-- applies where it is supposed to.
SELECT 'unrelated key, inlining:', arraySort(groupArray(k)) FROM v05066_invoker
SETTINGS analyzer_inline_views = 1, additional_table_filters = {'t05066': 'k != 3'};

DROP VIEW v05066_invoker;
DROP VIEW v05066_definer;
DROP TABLE t05066;
