-- A `SQL SECURITY DEFINER` / `NONE` view that can hide rows is a barrier for lazy
-- materialization too: the rewrite splits every `Expression` / `Filter` step of the chain into
-- a main and a lazy half, and the rebuilt steps do not carry the barrier flag. The post-lazy
-- merge passes would then combine an invoker-supplied predicate with the view's own filtering
-- into a single step with no evaluation-order guarantee, reopening the exception oracle on the
-- hidden rows. A view that hides no rows, and an `INVOKER` view, keep the optimization.

-- Pin everything the plan shape depends on: the test also runs with randomized settings.
-- Lazy materialization exists only under the analyzer, so it is pinned on too.
SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10,
    optimize_move_to_prewhere = 0, enable_parallel_replicas = 0, enable_analyzer = 1;

DROP TABLE IF EXISTS t04818;
CREATE TABLE t04818 (key UInt64, value UInt64, payload String, owner String) ENGINE = MergeTree ORDER BY key;
INSERT INTO t04818 SELECT number, number, repeat('x', 50), if(number = 5000, 'admin', 'nobody') FROM numbers(10000);

CREATE VIEW v04818_invoker SQL SECURITY INVOKER AS SELECT key, value, payload FROM t04818 WHERE owner != 'admin';
CREATE VIEW v04818_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT key, value, payload FROM t04818 WHERE owner != 'admin';

-- The `INVOKER` view stays fully optimizable: the payload column is materialized lazily.
SELECT 'invoker filtering view is lazily materialized:', count() > 0 FROM (EXPLAIN SELECT * FROM v04818_invoker ORDER BY value LIMIT 3) WHERE explain LIKE '%LazilyReadFromMergeTree%';

-- The filtering `DEFINER` view is a barrier: its steps are not rebuilt.
SELECT 'definer filtering view is lazily materialized:', count() FROM (EXPLAIN SELECT * FROM v04818_definer ORDER BY value LIMIT 3) WHERE explain LIKE '%LazilyReadFromMergeTree%';

-- The barrier only drops the optimization, never the correctness of the result.
SELECT 'definer view results:', arraySort(groupArray(value)) = [0, 1, 2] FROM (SELECT value FROM v04818_definer ORDER BY value LIMIT 3);

-- The invoker's predicate must never be evaluated on the hidden row: `value = 5000` exists only
-- in the row the view hides, so a merge of the two filters below the sort could throw here.
SELECT 'invoker predicate never sees the hidden row:', sum(value) FROM (SELECT value FROM v04818_definer WHERE NOT throwIf(value = 5000, 'DISCLOSED') ORDER BY value LIMIT 3);

DROP VIEW v04818_invoker;
DROP VIEW v04818_definer;
DROP TABLE t04818;
