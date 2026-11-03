-- The join is sharded by PK ranges, and the read is in order, so PREWHERE may prune a sorting-key
-- column that only the filter uses. The defect cells threw NOT_FOUND_COLUMN_IN_BLOCK before the fix;
-- the cells labelled `control` did not and must keep working. Result cells compare the sharded
-- result against an unsharded oracle (hash where the algorithm can change), so a silent wrong
-- result fails too; the rest are header and plan-shape guards. The guards matter because every
-- result cell passes vacuously if the plan silently stops sharding, prewhering or reading in order.

SET join_algorithm = 'full_sorting_merge';
SET query_plan_join_shard_by_pk_ranges = 1;
-- Either prewhere setting at 0 stops the PREWHERE move and makes every assertion below vacuous.
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
-- At 0 the filtered column is never pruned from the read output, so there is nothing to restore and
-- most cells below pass vacuously; `compatibility` draws below 25.12 revert this setting.
SET query_plan_remove_unused_columns = 1;
-- 0 routes to the branch that applies no sorting expression.
SET optimize_read_in_order = 1;
-- Parallel reading disables the sharding entirely.
SET enable_parallel_replicas = 0;
-- The correlated EXISTS cell needs the analyzer; the old analyzer rewrites EXISTS into a subquery
-- with no outer scope, and `compatibility` randomization can revert both settings.
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
-- A null-rejecting filter on the non-preserved side otherwise rewrites the outer join kinds, so the
-- labelled kind would not be the executed one (LEFT becomes INNER, FULL becomes RIGHT).
SET query_plan_convert_outer_join_to_inner_join = 0;

DROP TABLE IF EXISTS ok2;
CREATE TABLE ok2 (a UInt32, b UInt32, c Int64, d String)
ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS index_granularity = 64;
INSERT INTO ok2 SELECT number % 50, number % 200, toInt64(number), toString(number % 7) FROM numbers(2000);

-- A sorting-key column that is not the join key, filtered on either side, at any key position.
SELECT 'filter r.c', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94))
                   = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'filter r.b', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.b = 94))
                   = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.b = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'filter l.c', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE l.c = 94))
                   = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE l.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'filter both sides', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE l.c = 94 AND r.b = 94))
                          = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE l.c = 94 AND r.b = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'explicit PREWHERE', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN (SELECT * FROM ok2 PREWHERE c = 94) AS r ON l.a = r.a))
                          = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN (SELECT * FROM ok2 PREWHERE c = 94) AS r ON l.a = r.a) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

-- When the condition is a bare column the filter column IS that column, so it is also erased by
-- name after the DAG runs and adding it back to the outputs is not enough on its own. The cells
-- above never reach that state: a comparison makes the filter column a computed node, and a
-- `WHERE` moved by the optimizer is wrapped as well. Only an explicit bare `PREWHERE` does, at any
-- key position.
SELECT 'bare PREWHERE key col', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN (SELECT * FROM ok2 PREWHERE c) AS r ON l.a = r.a))
                              = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN (SELECT * FROM ok2 PREWHERE c) AS r ON l.a = r.a) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'bare PREWHERE key col mid', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN (SELECT * FROM ok2 PREWHERE b) AS r ON l.a = r.a))
                                  = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN (SELECT * FROM ok2 PREWHERE b) AS r ON l.a = r.a) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

-- Control for the cells above: a bare filter on a non-key column must keep working through the
-- sharded read-in-order path. It cannot observe the stronger property that such a column keeps
-- being removed: the restore is passed the sorting-key columns only, so a non-key column never
-- reaches the flag-clearing branch, and even an over-broad clear is invisible here because the
-- pipe-to-step header conversion drops a surplus kept filter column just as it drops the restored
-- ones. `ok2` has no non-key numeric column and a `String` cannot be a bare filter at all
-- (`canBeUsedInBooleanContext`), so this control needs its own table. `e` is `% 200 + 1`, always
-- truthy, so the control filters nothing and compares a full result.
DROP TABLE IF EXISTS nk05099;
CREATE TABLE nk05099 (a UInt32, b UInt32, c Int64, e UInt32, d String)
ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS index_granularity = 64;
INSERT INTO nk05099 SELECT number % 50, number % 200, toInt64(number), number % 200 + 1, toString(number % 7) FROM numbers(2000);

SELECT 'control bare PREWHERE non-key', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM nk05099 AS l INNER JOIN (SELECT * FROM nk05099 PREWHERE e) AS r ON l.a = r.a))
                                      = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM nk05099 AS l INNER JOIN (SELECT * FROM nk05099 PREWHERE e) AS r ON l.a = r.a) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

DROP TABLE nk05099;

-- An outer join reaches the branch only when the filter is on a side the join preserves: a filter on
-- the non-preserved side stays above the join as a `Filter (WHERE)` step and never becomes PREWHERE.
SELECT 'LEFT JOIN', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l LEFT JOIN ok2 AS r ON l.a = r.a WHERE l.c = 94))
                  = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l LEFT JOIN ok2 AS r ON l.a = r.a WHERE l.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'RIGHT JOIN', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l RIGHT JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94))
                   = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l RIGHT JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'FULL JOIN', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM (SELECT * FROM ok2 PREWHERE c = 94) AS l FULL JOIN ok2 AS r ON l.a = r.a))
                  = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM (SELECT * FROM ok2 PREWHERE c = 94) AS l FULL JOIN ok2 AS r ON l.a = r.a) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

-- Declared-output-header guard only. `DESCRIBE` resolves the analysis-time sample block and never
-- builds a pipeline, so this cannot observe the pipeline-time conversion that drops the restored
-- columns; it pins the step's advertised contract, which is what a caller sees. The pipeline-time
-- property is covered by the executing cells above: a leaked column trips the always-on arity check
-- in `Port.h` (`Code: 49`), not a debug-only assertion.
SELECT 'output header';
DESCRIBE (SELECT l.d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94);

-- All three preconditions of the fixed branch must hold, or every cell above passes vacuously.
-- `pretty = 0` pins the renderer: `compatibility` randomization can select the legacy one, which
-- spells the read type `ReadType:` rather than `Read type:`.
-- Exact counts, not `> 0`. The restore is gated per read step and this query has two of them, so
-- `ReadType: InOrder` must appear twice; one line per step, and `> 0` would still pass if one step
-- silently stopped reading in order. The two `Prewhere filter` lines are the name and the column of
-- the single PREWHERE, so `= 2` also pins that exactly one of them is pushed into a read.
SELECT 'plan shape',
       countIf(explain LIKE '%Sharding%') = 1
   AND countIf(explain LIKE '%Prewhere filter%') = 2
   AND countIf(explain LIKE '%ReadType: InOrder%') = 2
FROM (EXPLAIN actions = 1, pretty = 0
      SELECT l.d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94);

-- Must-stay-working controls: a non-key column is never in the sorting key, the join key is already
-- in the read output, and a filtered column kept in the output was never pruned.
SELECT 'control non-key col', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.d = '3'))
                            = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.d = '3') SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'control join key', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.a = 7))
                         = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.a = 7) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'control col in output', (SELECT sum(cityHash64(d, c)) FROM (SELECT l.d AS d, r.c AS c FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94))
                              = (SELECT sum(cityHash64(d, c)) FROM (SELECT l.d AS d, r.c AS c FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'control no filter', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a))
                          = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

-- A row policy on another sorting-key column prunes a column from the row-level-filter DAG rather
-- than from the PREWHERE DAG, so both DAGs must be restored.
DROP ROW POLICY IF EXISTS pol_05099 ON ok2;
CREATE ROW POLICY pol_05099 ON ok2 USING b < 1000 TO ALL;

-- The cell below is a result comparison, so it cannot see the row policy, the PREWHERE move, the
-- sharding or the in-order read silently declining on this path; and `b < 1000` excludes no row of
-- this fixture, so the policy's own effect is invisible to it. Pin the plan while the policy is
-- active, which is the only state in which both DAGs exist and the restore covers both. Exact
-- counts, not `> 0`: two read steps carry the policy, each printing a name and a column line.
SELECT 'row policy plan shape',
       countIf(explain LIKE '%Row level filter%') = 4
   AND countIf(explain LIKE '%Prewhere filter%') = 2
   AND countIf(explain LIKE '%Sharding%') = 1
   AND countIf(explain LIKE '%ReadType: InOrder%') = 2
FROM (EXPLAIN actions = 1, pretty = 0
      SELECT l.d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94);

SELECT 'row policy', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94))
                   = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

DROP ROW POLICY pol_05099 ON ok2;

-- The row-level DAG has the same hole as the PREWHERE one: a policy whose predicate is a bare
-- sorting-key column makes that column the row-level filter column, so it too is erased by name
-- after the DAG runs. This fixture's `b = 0` rows all have `a = 0` while `r.c = 94` restricts the
-- join to `a = 44`, so the policy changes no row of the joined result and the digest alone cannot
-- see it; the plan guard below is what pins the policy, the PREWHERE, the sharding and the
-- in-order read on this path.
DROP ROW POLICY IF EXISTS pol_bare_05099 ON ok2;
CREATE ROW POLICY pol_bare_05099 ON ok2 USING b TO ALL;

SELECT 'bare row policy plan shape',
       countIf(explain LIKE '%Row level filter%') = 4
   AND countIf(explain LIKE '%Prewhere filter%') = 2
   AND countIf(explain LIKE '%Sharding%') = 1
   AND countIf(explain LIKE '%ReadType: InOrder%') = 2
FROM (EXPLAIN actions = 1, pretty = 0
      SELECT l.d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94);

SELECT 'bare row policy col', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94))
                            = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

DROP ROW POLICY pol_bare_05099 ON ok2;
DROP TABLE ok2;

-- The restore matches inputs by column NAME, never by type, so a LowCardinality key column is
-- covered by the same name lookup as a plain one. An expression key is the other half: it requires
-- the expression's underlying input column, not the key output name, as the cell below asserts.
DROP TABLE IF EXISTS lc05099;
CREATE TABLE lc05099 (a UInt32, b LowCardinality(String), c Int64, d String)
ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS index_granularity = 64;
INSERT INTO lc05099 SELECT number % 50, toString(number % 200), toInt64(number), toString(number % 7) FROM numbers(2000);

SELECT 'LowCardinality key', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM lc05099 AS l INNER JOIN lc05099 AS r ON l.a = r.a WHERE r.b = '94'))
                           = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM lc05099 AS l INNER JOIN lc05099 AS r ON l.a = r.a WHERE r.b = '94') SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

DROP TABLE lc05099;

DROP TABLE IF EXISTS ex05099;
CREATE TABLE ex05099 (a UInt32, b UInt32, c Int64, d String)
ENGINE = MergeTree ORDER BY (a, b * 2, c) SETTINGS index_granularity = 64;
INSERT INTO ex05099 SELECT number % 50, number % 200, toInt64(number), toString(number % 7) FROM numbers(2000);

SELECT 'expression sorting key', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ex05099 AS l INNER JOIN ex05099 AS r ON l.a = r.a WHERE r.b = 94))
                               = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ex05099 AS l INNER JOIN ex05099 AS r ON l.a = r.a WHERE r.b = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

DROP TABLE ex05099;

-- A decorrelated correlated EXISTS under OR reaches the same branch, with the outer column pruned.
-- `i.s = o.s` is the correlated reference; the unqualified `v >= n` binds to the inner table.
DROP TABLE IF EXISTS t05099;
CREATE TABLE t05099 (s String, n UInt32, v Int64) ENGINE = MergeTree ORDER BY (s, n);
INSERT INTO t05099 SELECT toString(number % 10), number, number % 7 FROM numbers(1000);

SELECT 'correlated EXISTS under OR',
       (SELECT sum(cityHash64(n)) FROM (SELECT o.n AS n FROM t05099 AS o WHERE (EXISTS (SELECT 1 FROM t05099 AS i WHERE i.s = o.s AND v >= n)) OR o.n = 435))
     = (SELECT sum(cityHash64(n)) FROM (SELECT o.n AS n FROM t05099 AS o WHERE (EXISTS (SELECT 1 FROM t05099 AS i WHERE i.s = o.s AND v >= n)) OR o.n = 435) SETTINGS query_plan_join_shard_by_pk_ranges = 0);

DROP TABLE t05099;
