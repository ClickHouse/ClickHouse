-- A materialized CTE is read back with `max_threads` sources, and the result does not change.

SET enable_analyzer = 1, enable_materialized_cte = 1, max_threads = 4;

WITH t AS MATERIALIZED (SELECT number AS n FROM numbers_mt(1000000) SETTINGS max_block_size = 1000)
SELECT count(), sum(n) FROM t WHERE n IN (SELECT n FROM t WHERE n % 7 = 0);

-- Fewer stored blocks than streams.
WITH t AS MATERIALIZED (SELECT number AS n FROM numbers(3))
SELECT count(), sum(n) FROM t WHERE n IN (SELECT n FROM t);

WITH t AS MATERIALIZED (SELECT number AS n FROM numbers(0))
SELECT count() FROM t WHERE n IN (SELECT n FROM t);

-- The filter is applied inside the source.
WITH t AS MATERIALIZED (SELECT number AS n FROM numbers_mt(1000000) SETTINGS max_block_size = 1000)
SELECT count(), sum(n) FROM t PREWHERE n % 3 = 0 WHERE n IN (SELECT n FROM t);

-- One stream per thread after the read of the stored CTE.
SELECT trimLeft(explain) FROM (
    EXPLAIN PIPELINE
    WITH t AS MATERIALIZED (SELECT number AS n FROM numbers(1000))
    SELECT count() FROM t WHERE n IN (SELECT n FROM t)
) WHERE explain LIKE '%FilterTransform%';

-- A sorted CTE is read with one source, so its rows keep their order.
WITH t AS MATERIALIZED (SELECT number AS n FROM numbers_mt(100000) ORDER BY n DESC SETTINGS max_block_size = 1000)
SELECT groupArray(n) = arrayReverseSort(groupArray(n)) FROM (SELECT n FROM t WHERE n IN (SELECT n FROM t));
