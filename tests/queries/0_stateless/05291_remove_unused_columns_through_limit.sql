-- `LIMIT` passes every column through, so the columns nobody reads above it are removed below it
-- too, down to the read. `WITH TIES` still needs the columns it compares rows by.

SET enable_analyzer = 1;
SET query_plan_remove_unused_columns = 1;
-- The plan is checked on one node.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_limit_unused;
CREATE TABLE t_limit_unused (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_limit_unused SELECT number, number % 10 FROM numbers(1000);

-- Nothing above the `LIMIT` reads a column, so the read reads none.
SELECT replaceRegexpOne(explain, '^[│└─ ]+', '') FROM (EXPLAIN header = 1 SELECT count() FROM (SELECT 1 FROM t_limit_unused LIMIT 500))
WHERE explain LIKE '%Header:%' OR explain LIKE '%Output:%';
SELECT count() FROM (SELECT 1 FROM t_limit_unused LIMIT 500);
SELECT count() FROM (SELECT a FROM t_limit_unused LIMIT 200 OFFSET 100);

-- `WITH TIES` keeps the sorting column, and the ties are all counted.
SELECT replaceRegexpOne(explain, '^[│└─ ]+', '') FROM (EXPLAIN header = 1 SELECT count() FROM (SELECT a FROM t_limit_unused ORDER BY b LIMIT 3 WITH TIES))
WHERE explain LIKE '%Limit (%' OR explain LIKE '%Header:%' OR explain LIKE '%b UInt64%';
SELECT count() FROM (SELECT a FROM t_limit_unused ORDER BY b LIMIT 3 WITH TIES);

DROP TABLE t_limit_unused;
