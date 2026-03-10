-- Compact JSON (default)
EXPLAIN AST json=1 SELECT a + b AS sum FROM t WHERE a > 1;

-- Pretty-printed JSON
EXPLAIN AST json=1, pretty=1 SELECT 1;

-- CREATE TABLE schema
EXPLAIN AST json=1 CREATE TABLE t (a UInt32, b String) ENGINE = MergeTree ORDER BY a;

-- Error: json and graph are mutually exclusive
EXPLAIN AST json=1, graph=1 SELECT 1; -- { serverError BAD_ARGUMENTS }

-- pretty without json is silently ignored (produces normal text output)
EXPLAIN AST pretty=1 SELECT 1;
