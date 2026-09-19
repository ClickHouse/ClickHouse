-- A `SQL SECURITY DEFINER` / `NONE` view over `UNION ALL` is an optimization barrier: the
-- invoker's predicate must not be duplicated into the union's branches, where it would be
-- evaluated on rows the branches' own filtering hides. `tryLiftUpUnion` used to rebuild the
-- sealed step and the union as fresh unmarked steps, after which the filter pushdown could
-- descend into the branches.

SET query_plan_lift_up_union = 1, query_plan_filter_push_down = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t04825_a;
DROP TABLE IF EXISTS t04825_b;
CREATE TABLE t04825_a (key UInt64, owner String) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t04825_b (key UInt64, owner String) ENGINE = MergeTree ORDER BY key;
INSERT INTO t04825_a SELECT number, 'nobody' FROM numbers(1000);
INSERT INTO t04825_b SELECT number + 1000, 'nobody' FROM numbers(1000);

CREATE VIEW v04825_invoker SQL SECURITY INVOKER AS
    SELECT * FROM t04825_a WHERE owner != 'x'
    UNION ALL
    SELECT * FROM t04825_b WHERE owner != 'x';

CREATE VIEW v04825_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT * FROM t04825_a WHERE owner != 'x'
    UNION ALL
    SELECT * FROM t04825_b WHERE owner != 'x';

-- The `INVOKER` view stays fully optimizable: the outer predicate is duplicated into both
-- branches, so its function shows up more than once in the plan's actions.
SELECT 'invoker union view duplicates the outer predicate into branches:',
    count() > 1
    FROM (EXPLAIN actions = 1 SELECT * FROM v04825_invoker WHERE throwIf(key = 42) = 0)
    WHERE explain LIKE '%throwIf%';

-- The `DEFINER` view is a barrier: the outer predicate stays in a single filter above the union.
SELECT 'definer union view keeps the outer predicate in one filter:',
    count()
    FROM (EXPLAIN actions = 1 SELECT * FROM v04825_definer WHERE throwIf(key = 42) = 0)
    WHERE explain LIKE '%throwIf%';

-- The barrier only drops the optimization, never the result.
SELECT 'definer union view results:', count(), min(key), max(key) FROM v04825_definer WHERE key % 2 = 0;

DROP VIEW v04825_invoker;
DROP VIEW v04825_definer;
DROP TABLE t04825_a;
DROP TABLE t04825_b;
