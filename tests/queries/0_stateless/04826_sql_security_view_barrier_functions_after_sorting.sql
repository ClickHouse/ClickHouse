-- A wrapper `SQL SECURITY DEFINER` view (a view over a `Merge` table, a remote table, or a nested
-- view) is protected only by the seal on its top step. `tryExecuteFunctionsAfterSorting` must not
-- replace a sealed `ExpressionStep` under a `SortingStep` with fresh unmarked steps: the read-in-
-- order and top-K walks could then descend through the wrapper again and shape the source's read
-- by the invoker's `ORDER BY`. This pins the plan of an `ORDER BY ... LIMIT` over such a view.

-- The old analyzer does not read a `Merge` wrapper in order at all, so the `INVOKER` control
-- below would show nothing to compare against; the barrier behavior under the old analyzer is
-- covered by 04822.
SET enable_analyzer = 1;
SET query_plan_execute_functions_after_sorting = 1, optimize_read_in_order = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t04826;
CREATE TABLE t04826 (key UInt64, owner String, val String) ENGINE = MergeTree ORDER BY key;
INSERT INTO t04826 SELECT number, 'nobody', toString(number) FROM numbers(10000);

-- The row hiding lives below the `Merge` wrapper, where the marking walk over the outer view's
-- subplan sees nothing to mark: the seal on the top step is the only protection.
CREATE VIEW v04826_inner DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM t04826 WHERE owner != 'x';
CREATE TABLE w04826 (key UInt64, owner String, val String) ENGINE = Merge(currentDatabase(), '^v04826_inner$');

-- The computed column merges into the seal, so the sorting sits over a sealed expression with
-- work that is unneeded for the sort — what `tryExecuteFunctionsAfterSorting` wants to split.
CREATE VIEW v04826_invoker SQL SECURITY INVOKER AS SELECT key, owner, concat(val, owner) AS c FROM w04826;
CREATE VIEW v04826_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT key, owner, concat(val, owner) AS c FROM w04826;

-- The `INVOKER` view stays fully optimizable: the invoker's `ORDER BY` exploits the source order.
SELECT 'invoker wrapper view sorts by a prefix of the source order:', count() > 0
    FROM (EXPLAIN actions = 1 SELECT * FROM v04826_invoker ORDER BY key LIMIT 3)
    WHERE explain LIKE '%Prefix sort description%' OR explain LIKE '%InOrder%';

-- The `DEFINER` view is a barrier: nothing above it may read the source in order or prune it
-- with a top-K filter by the invoker's `ORDER BY`.
SELECT 'definer wrapper view reads in order:', count()
    FROM (EXPLAIN actions = 1 SELECT * FROM v04826_definer ORDER BY key LIMIT 3)
    WHERE explain LIKE '%InOrder%';
SELECT 'definer wrapper view gets a top-K filter:', count()
    FROM (EXPLAIN actions = 1 SELECT * FROM v04826_definer ORDER BY key LIMIT 3)
    WHERE explain LIKE '%__topKFilter%';

-- The barrier only drops the optimization, never the correctness of the order.
SELECT 'definer wrapper view results:', groupArray(key) = [0, 1, 2]
    FROM (SELECT key FROM v04826_definer ORDER BY key LIMIT 3);

DROP VIEW v04826_invoker;
DROP VIEW v04826_definer;
DROP TABLE w04826;
DROP VIEW v04826_inner;
DROP TABLE t04826;
