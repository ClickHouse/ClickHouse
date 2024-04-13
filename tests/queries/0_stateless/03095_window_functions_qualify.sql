SET allow_experimental_analyzer = 1;

SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count FROM numbers(10) QUALIFY partition_count = 4 ORDER BY number;

SELECT '--';

SELECT number FROM numbers(10) QUALIFY (COUNT() OVER (PARTITION BY number % 3) AS partition_count) = 4 ORDER BY number;

SELECT '--';

SELECT number FROM numbers(10) QUALIFY number > 5 ORDER BY number;

SELECT '--';

SELECT (number % 2) AS key, count() FROM numbers(10) GROUP BY key HAVING key = 0 QUALIFY key == 0;

SELECT '--';

SELECT (number % 2) AS key, count() FROM numbers(10) GROUP BY key QUALIFY key == 0;
