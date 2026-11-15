-- The test harness may randomize this setting, and the EXPLAIN check below depends on it.
SET optimize_trivial_count_query = 1;

DROP TABLE IF EXISTS t_memory_count;

CREATE TABLE t_memory_count (k UInt64, s String) ENGINE = Memory SETTINGS compress = 1;

SELECT count() FROM t_memory_count;

INSERT INTO t_memory_count SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_memory_count SELECT number, toString(number) FROM numbers(234);

SELECT count() FROM t_memory_count;
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_memory_count) WHERE explain LIKE '%Optimized trivial count%';
SELECT count() FROM t_memory_count SETTINGS optimize_trivial_count_query = 0;

-- count() with a filter is not trivial.
SELECT count() FROM t_memory_count WHERE k < 100;

-- A materialized CTE is filled during query execution, so its count() must not be
-- served from the (empty at planning time) storage metadata.
SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
WITH cte AS MATERIALIZED (SELECT number FROM numbers(42)) SELECT count() FROM cte;

TRUNCATE TABLE t_memory_count;
SELECT count() FROM t_memory_count;

DROP TABLE t_memory_count;
