-- { echoOn }
EXPLAIN AST after_rewrite=0 SELECT * FROM numbers(0);
EXPLAIN AST after_rewrite=1 SELECT * FROM numbers(0);
-- { echoOff }
