-- { echoOn }
EXPLAIN AST optimize = 1 SELECT sum(number) OVER () FROM numbers(3);
EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT sum(number) OVER () FROM numbers(3);
-- { echoOff }
