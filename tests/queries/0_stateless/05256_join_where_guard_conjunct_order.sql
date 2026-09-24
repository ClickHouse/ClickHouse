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

-- A second equality in the WHERE becomes part of the join condition and the other two conjuncts stay behind.
SELECT count(), min(a.k)
FROM (SELECT number AS k, number AS j, if(number % 2 = 0, toString(number), 'oops') AS s FROM numbers(8)) AS a
INNER JOIN (SELECT number AS k, number AS j, number AS m FROM numbers(8)) AS b ON a.k = b.k
WHERE a.j = b.j AND (a.k + b.m) % 4 = 0 AND toUInt64(a.s) + b.m > 3
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
