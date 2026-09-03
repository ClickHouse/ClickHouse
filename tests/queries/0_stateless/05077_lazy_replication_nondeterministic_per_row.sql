-- A hash join carries the fanned-out side as lazily replicated columns. A function that is not
-- deterministic within the query must not be evaluated once per source row and expanded to every row
-- that replicates it: the column argument of `rand` and `generateUUIDv4` exists to get a value per row.

SELECT uniqExact(rand(a.s)), uniqExact(generateUUIDv4(a.s)), count()
FROM (SELECT toString(number) AS s, number % 10 AS x FROM numbers(10)) a
INNER JOIN (SELECT number % 10 AS y FROM numbers(1000)) b ON a.x = b.y;

SELECT uniqExact(rand(a.s)), uniqExact(generateUUIDv4(a.s)), count()
FROM (SELECT toString(number) AS s, number % 10 AS x FROM numbers(10)) a
INNER JOIN (SELECT number % 10 AS y FROM numbers(1000)) b ON a.x = b.y
SETTINGS enable_lazy_columns_replication = 0;

-- The values are persisted correctly.
DROP TABLE IF EXISTS t_lazy_replication_ids;
CREATE TABLE t_lazy_replication_ids (u UUID) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lazy_replication_ids
SELECT generateUUIDv4(a.s)
FROM (SELECT toString(number) AS s, number % 10 AS x FROM numbers(10)) a
INNER JOIN (SELECT number % 10 AS y FROM numbers(1000)) b ON a.x = b.y;
SELECT count(), uniqExact(u) FROM t_lazy_replication_ids;
DROP TABLE t_lazy_replication_ids;

-- A deterministic function still shares one evaluation per source row.
SELECT uniqExact(upper(a.s)), count()
FROM (SELECT toString(number) AS s, number % 10 AS x FROM numbers(10)) a
INNER JOIN (SELECT number % 10 AS y FROM numbers(1000)) b ON a.x = b.y;
