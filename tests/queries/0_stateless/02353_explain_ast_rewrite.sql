-- { echoOn }
EXPLAIN AST rewrite=0 SELECT * FROM numbers(0);
EXPLAIN AST rewrite=1 SELECT * FROM numbers(0);
EXPLAIN AST rewrite=0 SELECT countDistinct(number) FROM numbers(0);
EXPLAIN AST rewrite=1 SELECT countDistinct(number) FROM numbers(0);
-- { echoOff }
