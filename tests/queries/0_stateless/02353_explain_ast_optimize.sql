-- { echoOn }
EXPLAIN AST optimize=0 SELECT * FROM numbers(0);
EXPLAIN AST optimize=1 SELECT * FROM numbers(0);
EXPLAIN AST optimize=0 SELECT countDistinct(number) FROM numbers(0);
EXPLAIN AST optimize=1 SELECT countDistinct(number) FROM numbers(0);
-- { echoOff }
