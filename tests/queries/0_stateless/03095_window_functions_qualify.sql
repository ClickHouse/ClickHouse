SET enable_analyzer = 1;

SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count FROM numbers(10) QUALIFY partition_count = 4 ORDER BY number;

SELECT '--';

SELECT number FROM numbers(10) QUALIFY (COUNT() OVER (PARTITION BY number % 3) AS partition_count) = 4 ORDER BY number;

SELECT '--';

SELECT number FROM numbers(10) QUALIFY number > 5 ORDER BY number;

SELECT '--';

SELECT (number % 2) AS key, count() FROM numbers(10) GROUP BY key HAVING key = 0 QUALIFY key == 0;

SELECT '--';

SELECT (number % 2) AS key, count() FROM numbers(10) GROUP BY key QUALIFY key == 0;

SELECT '--';

SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count FROM numbers(10) QUALIFY COUNT() OVER (PARTITION BY number % 3) = 4 ORDER BY number;

SELECT '--';

EXPLAIN header = 1, actions = 1
SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count FROM numbers(10) QUALIFY COUNT() OVER (PARTITION BY number % 3) = 4 ORDER BY number;

SELECT number % toUInt256(2) AS key, count() FROM numbers(10) GROUP BY key WITH CUBE WITH TOTALS QUALIFY key = toNullable(toNullable(0)); -- { serverError NOT_IMPLEMENTED }

SELECT number % 2 AS key, count(materialize(5)) IGNORE NULLS FROM numbers(10) WHERE toLowCardinality(toLowCardinality(materialize(2))) GROUP BY key WITH CUBE WITH TOTALS QUALIFY key = 0; -- { serverError NOT_IMPLEMENTED }

SELECT 4, count(4) IGNORE NULLS, number % 2 AS key FROM numbers(10) GROUP BY key WITH ROLLUP WITH TOTALS QUALIFY key = materialize(0); -- { serverError NOT_IMPLEMENTED }

SELECT 3, number % toLowCardinality(2) AS key, count() IGNORE NULLS FROM numbers(10) GROUP BY key WITH ROLLUP WITH TOTALS QUALIFY key = 0; -- { serverError NOT_IMPLEMENTED }
