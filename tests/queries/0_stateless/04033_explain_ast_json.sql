-- Compact JSON (default)
EXPLAIN AST json=1 SELECT a + b AS sum FROM t WHERE a > 1;

-- Pretty-printed JSON
EXPLAIN AST json=1, pretty=1 SELECT 1;

-- CREATE TABLE schema
EXPLAIN AST json=1 CREATE TABLE t (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
