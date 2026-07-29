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

-- An outer join reaches the branch only when the filter is on a side the join preserves: a filter on
-- the non-preserved side stays above the join as a `Filter (WHERE)` step and never becomes PREWHERE.
SELECT 'LEFT JOIN', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l LEFT JOIN ok2 AS r ON l.a = r.a WHERE l.c = 94))
                  = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l LEFT JOIN ok2 AS r ON l.a = r.a WHERE l.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'RIGHT JOIN', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l RIGHT JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94))
                   = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l RIGHT JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

SELECT 'FULL JOIN', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM (SELECT * FROM ok2 PREWHERE c = 94) AS l FULL JOIN ok2 AS r ON l.a = r.a))
                  = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM (SELECT * FROM ok2 PREWHERE c = 94) AS l FULL JOIN ok2 AS r ON l.a = r.a) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

-- The restored columns must not reach the declared output header.
SELECT 'output header';
DESCRIBE (SELECT l.d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94);

-- No step declares a restored column: with everything but step names and headers suppressed, neither
-- may appear anywhere in the dump. `query_plan_remove_unused_columns = 0` keeps the filtered column
-- on every step above the read, sharded or not, so it is pinned rather than asserted around.
SELECT 'rfmt step header', countIf(explain LIKE '%b UInt32%' OR explain LIKE '%c Int64%') = 0
FROM (EXPLAIN header = 1, actions = 0, description = 0, indexes = 0, pretty = 0
      SELECT l.d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94)
SETTINGS query_plan_remove_unused_columns = 1;

-- All three preconditions of the fixed branch must hold, or every cell above passes vacuously.
-- `pretty = 0` pins the renderer: `compatibility` randomization can select the legacy one, which
-- spells the read type `ReadType:` rather than `Read type:`.
SELECT 'plan shape',
       countIf(explain LIKE '%Sharding%') = 1
   AND countIf(explain LIKE '%Prewhere filter%') > 0
   AND countIf(explain LIKE '%ReadType: InOrder%') > 0
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
DROP ROW POLICY IF EXISTS pol_04652 ON ok2;
CREATE ROW POLICY pol_04652 ON ok2 USING b < 1000 TO ALL;

SELECT 'row policy', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94))
                   = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ok2 AS l INNER JOIN ok2 AS r ON l.a = r.a WHERE r.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

DROP ROW POLICY pol_04652 ON ok2;
DROP TABLE ok2;

-- The restore set comes from the sorting key expression, so a wrapped or computed key column must
-- be covered too: LowCardinality is not stripped by removeNullable, and an expression key requires
-- the underlying column rather than the key name.
DROP TABLE IF EXISTS lc04652;
CREATE TABLE lc04652 (a UInt32, b LowCardinality(String), c Int64, d String)
ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS index_granularity = 64, allow_suspicious_low_cardinality_types = 1;
INSERT INTO lc04652 SELECT number % 50, toString(number % 200), toInt64(number), toString(number % 7) FROM numbers(2000);

SELECT 'LowCardinality key', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM lc04652 AS l INNER JOIN lc04652 AS r ON l.a = r.a WHERE r.b = '94'))
                           = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM lc04652 AS l INNER JOIN lc04652 AS r ON l.a = r.a WHERE r.b = '94') SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

DROP TABLE lc04652;

DROP TABLE IF EXISTS ex04652;
CREATE TABLE ex04652 (a UInt32, b UInt32, c Int64, d String)
ENGINE = MergeTree ORDER BY (a, b * 2, c) SETTINGS index_granularity = 64;
INSERT INTO ex04652 SELECT number % 50, number % 200, toInt64(number), toString(number % 7) FROM numbers(2000);

SELECT 'expression sorting key', (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ex04652 AS l INNER JOIN ex04652 AS r ON l.a = r.a WHERE r.c = 94))
                               = (SELECT sum(cityHash64(d)) FROM (SELECT l.d AS d FROM ex04652 AS l INNER JOIN ex04652 AS r ON l.a = r.a WHERE r.c = 94) SETTINGS join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 0);

DROP TABLE ex04652;

-- A decorrelated correlated EXISTS under OR reaches the same branch, with the outer column pruned.
-- `i.s = o.s` is the correlated reference; the unqualified `v >= n` binds to the inner table.
DROP TABLE IF EXISTS t04652;
CREATE TABLE t04652 (s String, n UInt32, v Int64) ENGINE = MergeTree ORDER BY (s, n);
INSERT INTO t04652 SELECT toString(number % 10), number, number % 7 FROM numbers(1000);

SELECT 'correlated EXISTS under OR',
       (SELECT sum(cityHash64(n)) FROM (SELECT o.n AS n FROM t04652 AS o WHERE (EXISTS (SELECT 1 FROM t04652 AS i WHERE i.s = o.s AND v >= n)) OR o.n = 435))
     = (SELECT sum(cityHash64(n)) FROM (SELECT o.n AS n FROM t04652 AS o WHERE (EXISTS (SELECT 1 FROM t04652 AS i WHERE i.s = o.s AND v >= n)) OR o.n = 435) SETTINGS query_plan_join_shard_by_pk_ranges = 0);

DROP TABLE t04652;
