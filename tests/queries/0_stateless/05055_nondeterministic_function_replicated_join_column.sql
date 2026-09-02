-- A function that is not deterministic in the scope of the query must produce an independent value for
-- every output row, even when its argument arrives as a lazily replicated column (`ColumnReplicated`)
-- from a hash join that fans out the probe side. The default implementation for replicated columns in
-- `IExecutableFunction::execute` evaluates the function once per row of the nested column and expands
-- that single result to every row replicated from it, which made all 100 rows fanned out from the same
-- source row share one value.
-- https://github.com/ClickHouse/ClickHouse/issues/117467

SET enable_lazy_columns_replication = 1;

-- 1000 output rows fan out from 10 source rows, so the bug produced exactly 10 distinct values.
-- `rand` returns UInt32, where 1000 independent draws still collide with probability ~1e-4, so compare
-- against a threshold instead of an exact count to keep the result deterministic.
SELECT uniqExact(rand(a.s)) > 900, count()
FROM (SELECT toString(number) AS s, number % 10 AS x FROM numbers(10)) AS a
INNER JOIN (SELECT number % 10 AS y FROM numbers(1000)) AS b ON a.x = b.y;

-- `rand64` and `generateUUIDv4` are wide enough that an exact count of distinct values is safe.
SELECT uniqExact(rand64(a.s)), count()
FROM (SELECT toString(number) AS s, number % 10 AS x FROM numbers(10)) AS a
INNER JOIN (SELECT number % 10 AS y FROM numbers(1000)) AS b ON a.x = b.y;

SELECT uniqExact(generateUUIDv4(a.s)), count()
FROM (SELECT toString(number) AS s, number % 10 AS x FROM numbers(10)) AS a
INNER JOIN (SELECT number % 10 AS y FROM numbers(1000)) AS b ON a.x = b.y;

-- The result must not depend on how the argument column is represented.
SELECT uniqExact(rand64(a.s)), count()
FROM (SELECT toString(number) AS s, number % 10 AS x FROM numbers(10)) AS a
INNER JOIN (SELECT number % 10 AS y FROM numbers(1000)) AS b ON a.x = b.y
SETTINGS enable_lazy_columns_replication = 0;

DROP TABLE IF EXISTS t_dim;
DROP TABLE IF EXISTS t_fact;
CREATE TABLE t_dim (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_fact (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_dim SELECT number, toString(number) FROM numbers(10);
INSERT INTO t_fact SELECT number % 10 FROM numbers(1000);

-- Keeping the fanned-out payload side as the probe side is what feeds the replicated code path.
SELECT uniqExact(rand64(d.s)), count()
FROM t_dim AS d INNER JOIN t_fact AS f ON d.k = f.k
SETTINGS query_plan_join_swap_table = 'false';

-- The duplicates used to be persisted by `INSERT ... SELECT`.
DROP TABLE IF EXISTS t_ids;
CREATE TABLE t_ids (u UUID) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ids SELECT generateUUIDv4(d.s)
FROM t_dim AS d INNER JOIN t_fact AS f ON d.k = f.k
SETTINGS query_plan_join_swap_table = 'false';
SELECT uniqExact(u), count() FROM t_ids;

-- A function that is deterministic in the scope of the query still uses the fast path for replicated
-- columns, so it is evaluated once per source row: 10 distinct values over 1000 output rows.
SELECT uniqExact(upper(a.s)), count()
FROM (SELECT toString(number) AS s, number % 10 AS x FROM numbers(10)) AS a
INNER JOIN (SELECT number % 10 AS y FROM numbers(1000)) AS b ON a.x = b.y;

DROP TABLE t_ids;
DROP TABLE t_dim;
DROP TABLE t_fact;
