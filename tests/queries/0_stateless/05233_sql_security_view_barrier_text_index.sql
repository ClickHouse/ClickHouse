-- A `SQL SECURITY DEFINER` / `NONE` view whose own filtering uses a text-search function stays a
-- barrier after `processAndOptimizeTextIndexFunctions` rewrote it.
--
-- That pass walks the `FilterStep` / `ExpressionStep` chain above a `ReadFromMergeTree` and
-- replaces matching steps with freshly built ones - once for the filter sitting directly on the
-- reading step (the direct-read rewrite, which registers the synthetic
-- `__text_index_..._hasAllTokens_<hash>` column) and once for every step whose DAG gets a
-- tokenizer preprocessor injected. A replacement that does not inherit `isSecurityBarrier`
-- unfences the step: lazy materialization then splits the view's own filtering into a main and a
-- lazy half, and the post-lazy `tryMergeExpressions` / `tryMergeFilters` combine the invoker's
-- predicate with it, which reopens the exception oracle over the hidden rows.
--
-- Twin of `04818_sql_security_view_barrier_lazy_materialization` with a text-index predicate:
-- before the fix the `definer` line below was `1`, exactly like the `invoker` one.

-- Pin everything the plan shape depends on: the test also runs with randomized settings.
SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10,
    optimize_move_to_prewhere = 0, enable_parallel_replicas = 0,
    enable_full_text_index = 1, use_skip_indexes_on_data_read = 1,
    query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS t05233;

-- The `preprocessor` makes the pass rewrite steps above the reading step as well (the
-- inject-only path), while `query_plan_direct_read_from_text_index` exercises the rebuild of the
-- filter that sits directly on it.
CREATE TABLE t05233
(
    key UInt64,
    value UInt64,
    payload String,
    doc String,
    INDEX idx_doc doc TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(doc))
)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t05233 SELECT number, number, repeat('x', 50), if(number = 5000, 'secret admin', 'public record') FROM numbers(10000);

CREATE VIEW v05233_invoker SQL SECURITY INVOKER AS SELECT key, value, payload FROM t05233 WHERE hasAllTokens(doc, ['public']);
CREATE VIEW v05233_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT key, value, payload FROM t05233 WHERE hasAllTokens(doc, ['public']);

-- The text index really is used, otherwise the rewrite never runs and the checks below are vacuous.
SELECT 'the rewrite really runs:', count() > 0 FROM (EXPLAIN SELECT * FROM v05233_definer ORDER BY value LIMIT 3) WHERE explain LIKE '%__text_index_%';

-- The `INVOKER` view stays fully optimizable: the payload column is materialized lazily.
SELECT 'invoker filtering view is lazily materialized:', count() > 0 FROM (EXPLAIN SELECT * FROM v05233_invoker ORDER BY value LIMIT 3) WHERE explain LIKE '%LazilyReadFromMergeTree%';

-- The filtering `DEFINER` view is a barrier: its rewritten steps are not split.
SELECT 'definer filtering view is lazily materialized:', count() FROM (EXPLAIN SELECT * FROM v05233_definer ORDER BY value LIMIT 3) WHERE explain LIKE '%LazilyReadFromMergeTree%';

-- The barrier only drops the optimization, never the correctness of the result.
SELECT 'definer view results:', arraySort(groupArray(value)) = [0, 1, 2] FROM (SELECT value FROM v05233_definer ORDER BY value LIMIT 3);

-- The invoker's predicate must never be evaluated on the hidden row: `value = 5000` exists only
-- in the row the view hides.
SELECT 'invoker predicate never sees the hidden row:', sum(value) FROM (SELECT value FROM v05233_definer WHERE NOT throwIf(value = 5000, 'DISCLOSED') ORDER BY value LIMIT 3);

DROP VIEW v05233_invoker;
DROP VIEW v05233_definer;
DROP TABLE t05233;
