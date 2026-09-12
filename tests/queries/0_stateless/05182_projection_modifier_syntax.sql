SET enable_analyzer = 1;
SET optimize_use_projections = 1;

DROP TABLE IF EXISTS t_syntax;

CREATE TABLE t_syntax
(
    key UInt64,
    value UInt64,
    PROJECTION p_value (SELECT key, value ORDER BY value)
)
ENGINE = MergeTree ORDER BY key;

-- { echoOn }

-- Formatting keeps the modifier after the table.
EXPLAIN SYNTAX SELECT value FROM t_syntax PROJECTION p_value WHERE key < 10;

-- The AST holds a dedicated node under the table expression.
EXPLAIN AST SELECT value FROM t_syntax AS t PROJECTION p_value WHERE key < 10;

-- The query tree carries it in the table expression modifiers.
EXPLAIN QUERY TREE SELECT value FROM t_syntax PROJECTION p_value WHERE key < 10;

-- The keyword is reserved as a bare alias only; with AS it still works.
SELECT 1 AS projection;
SELECT projection FROM (SELECT 1 AS projection);
