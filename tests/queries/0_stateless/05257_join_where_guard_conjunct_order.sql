-- The conjuncts a JOIN rewrite leaves behind in a WHERE keep the order they were written in, so a guard is
-- still evaluated before the conjunct it guards. Without that, `toUInt64(a.s)` below runs on the rows the
-- guard would have removed and the query fails instead of returning a result.

-- One conjunct over the join key only is pushed into both sides, the two that span both sides stay behind.
-- `(a.k + b.m) % 4 = 0` is the guard: it keeps only the rows whose `a.s` is numeric.
SELECT count(), min(a.k)
FROM (SELECT number AS k, if(number % 2 = 0, toString(number), 'oops') AS s FROM numbers(8)) AS a
INNER JOIN (SELECT number AS k, number AS m FROM numbers(8)) AS b ON a.k = b.k
WHERE a.k < 100 AND (a.k + b.m) % 4 = 0 AND toUInt64(a.s) + b.m > 3
SETTINGS enable_join_runtime_filters = 0;

-- Liveness + order oracle for the cell above, which returns the same rows when the rewrite declines:
-- `AND column:` names every atom the filter step extracts, so require the guard to be among them and
-- `a.k < 100` not to be - the latter is only extracted while it is still in the WHERE. The extra settings
-- are pinned (all randomized in CI) because the assertion needs a deterministic plan, and
-- `query_plan_merge_filter_into_join_condition = 0` keeps this cell measuring push-down alone.
SELECT countIf(position(explain,
                        'AND column: equals(modulo(plus(__table1.k, __table3.m), 4_UInt8), 0_UInt8)') > 0) > 0
   AND countIf(position(explain, 'AND column: less(__table1.k, 100_UInt8)') > 0) = 0
FROM (
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT count(), min(a.k)
    FROM (SELECT number AS k, if(number % 2 = 0, toString(number), 'oops') AS s FROM numbers(8)) AS a
    INNER JOIN (SELECT number AS k, number AS m FROM numbers(8)) AS b ON a.k = b.k
    WHERE a.k < 100 AND (a.k + b.m) % 4 = 0 AND toUInt64(a.s) + b.m > 3
    SETTINGS enable_join_runtime_filters = 0, query_plan_merge_filter_into_join_condition = 0,
             query_plan_filter_push_down = 1, query_plan_merge_filters = 1,
             query_plan_remove_unused_columns = 1, query_plan_optimize_join_order_limit = 0,
             query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
             optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0
);

-- A second equality in the WHERE becomes part of the join condition and the other two conjuncts stay behind.
SELECT count(), min(a.k)
FROM (SELECT number AS k, number AS j, if(number % 2 = 0, toString(number), 'oops') AS s FROM numbers(8)) AS a
INNER JOIN (SELECT number AS k, number AS j, number AS m FROM numbers(8)) AS b ON a.k = b.k
WHERE a.j = b.j AND (a.k + b.m) % 4 = 0 AND toUInt64(a.s) + b.m > 3
SETTINGS enable_join_runtime_filters = 0, query_plan_merge_filter_into_join_condition = 1;

-- Same oracle for the filter-into-join-condition carrier: the guard must be among the extracted atoms and
-- `a.j = b.j` must not be, since that atom is only extracted while the merge leaves it in the WHERE.
SELECT countIf(position(explain,
                        'AND column: equals(modulo(plus(__table1.k, __table3.m), 4_UInt8), 0_UInt8)') > 0) > 0
   AND countIf(position(explain, 'AND column: equals(__table1.j, __table3.j)') > 0) = 0
FROM (
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT count(), min(a.k)
    FROM (SELECT number AS k, number AS j, if(number % 2 = 0, toString(number), 'oops') AS s FROM numbers(8)) AS a
    INNER JOIN (SELECT number AS k, number AS j, number AS m FROM numbers(8)) AS b ON a.k = b.k
    WHERE a.j = b.j AND (a.k + b.m) % 4 = 0 AND toUInt64(a.s) + b.m > 3
    SETTINGS enable_join_runtime_filters = 0, query_plan_merge_filter_into_join_condition = 1,
             query_plan_filter_push_down = 1, query_plan_merge_filters = 1,
             query_plan_remove_unused_columns = 1, query_plan_optimize_join_order_limit = 0,
             query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
             optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0
);

-- The same shape with the guard written twice. Repeating a conjunct must not change which of the two
-- conjuncts that stay behind is evaluated first.
SELECT count(), min(a.k)
FROM (SELECT number AS k, number AS j, if(number % 2 = 0, toString(number), 'oops') AS s FROM numbers(8)) AS a
INNER JOIN (SELECT number AS k, number AS j, number AS m FROM numbers(8)) AS b ON a.k = b.k
WHERE (a.j = b.j AND (a.k + b.m) % 4 = 0 AND toUInt64(a.s) + b.m > 3) AND (a.k + b.m) % 4 = 0
SETTINGS enable_join_runtime_filters = 0, query_plan_merge_filter_into_join_condition = 1;

-- Same oracle for the shared-atom shape, with `a.j = b.j` again the atom the merge consumes. The cell above
-- is green on master too, so this is what discriminates against reordering by reversal: reversing the
-- de-duplicated conjunct list hoists the throwing conjunct here, and only the predicate order hoists the guard.
SELECT countIf(position(explain,
                        'AND column: equals(modulo(plus(__table1.k, __table3.m), 4_UInt8), 0_UInt8)') > 0) > 0
   AND countIf(position(explain, 'AND column: equals(__table1.j, __table3.j)') > 0) = 0
FROM (
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT count(), min(a.k)
    FROM (SELECT number AS k, number AS j, if(number % 2 = 0, toString(number), 'oops') AS s FROM numbers(8)) AS a
    INNER JOIN (SELECT number AS k, number AS j, number AS m FROM numbers(8)) AS b ON a.k = b.k
    WHERE (a.j = b.j AND (a.k + b.m) % 4 = 0 AND toUInt64(a.s) + b.m > 3) AND (a.k + b.m) % 4 = 0
    SETTINGS enable_join_runtime_filters = 0, query_plan_merge_filter_into_join_condition = 1,
             query_plan_filter_push_down = 1, query_plan_merge_filters = 1,
             query_plan_remove_unused_columns = 1, query_plan_optimize_join_order_limit = 0,
             query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
             optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0
);

-- The same shape again, with the guarded conjunct written twice instead of the guard. It shares the carrier,
-- the shape and the pins of the two cells above and already has a pre-fix red of its own, so it gets no
-- EXPLAIN assertion: a fourth one would add size without coverage.
SELECT count(), min(a.k)
FROM (SELECT number AS k, number AS j, if(number % 2 = 0, toString(number), 'oops') AS s FROM numbers(8)) AS a
INNER JOIN (SELECT number AS k, number AS j, number AS m FROM numbers(8)) AS b ON a.k = b.k
WHERE (a.j = b.j AND (a.k + b.m) % 4 = 0 AND toUInt64(a.s) + b.m > 3) AND (toUInt64(a.s) + b.m > 3)
SETTINGS enable_join_runtime_filters = 0, query_plan_merge_filter_into_join_condition = 1;

-- The same invariant where an aggregate projection's own WHERE covers one query conjunct: the two that
-- are left over stay behind, and must keep their order too. `force_optimize_projection` makes the cell
-- fail rather than pass vacuously if the projection is not chosen.
DROP TABLE IF EXISTS t_guard_proj;
CREATE TABLE t_guard_proj (id UInt64, g UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192;
INSERT INTO t_guard_proj SELECT number, number, if(number % 4 = 0, toString(number), 'oops') FROM numbers(64);
ALTER TABLE t_guard_proj ADD PROJECTION pw (SELECT g, s, count() WHERE id < 1000 GROUP BY g, s);
ALTER TABLE t_guard_proj MATERIALIZE PROJECTION pw SETTINGS mutations_sync = 2;

SELECT count(), min(g) FROM (
    SELECT g, count() FROM t_guard_proj
    WHERE id < 1000 AND (g % 4 = 0) AND toUInt64(s) + g > 3
    GROUP BY g
    SETTINGS optimize_use_projections = 1, force_optimize_projection = 1);

-- Isolating control for the cell above: without the projection the same query is unaffected, which is
-- what makes that cell evidence about the projection path. Green either way, by design.
SELECT count(), min(g) FROM (
    SELECT g, count() FROM t_guard_proj
    WHERE id < 1000 AND (g % 4 = 0) AND toUInt64(s) + g > 3
    GROUP BY g
    SETTINGS optimize_use_projections = 0);

DROP TABLE t_guard_proj;
