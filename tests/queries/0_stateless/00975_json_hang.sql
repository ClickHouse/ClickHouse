-- Tags: no-fasttest

SELECT DISTINCT JSONExtractRaw(concat('{"x":', rand() % 2 ? 'true' : 'false', '}'), 'x') AS res FROM numbers(1000000) ORDER BY res;
