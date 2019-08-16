SELECT DISTINCT JSONExtractRaw(concat('{"x":', rand() % 2 ? 'true' : 'false', '}'), 'x') AS res FROM numbers(100000) ORDER BY res;
