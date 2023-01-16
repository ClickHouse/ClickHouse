SET allow_experimental_analyzer = 0;

SELECT count() > 3 FROM (EXPLAIN PIPELINE header = 1 SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain LIKE '%Header: number UInt64%';
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE '%Sort%';
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE '%Sort%';
SELECT count() > 0 FROM (EXPLAIN CURRENT TRANSACTION);
SELECT count() == 1 FROM (EXPLAIN SYNTAX SELECT number FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE 'SELECT%';
SELECT trim(explain) == 'Asterisk' FROM (EXPLAIN AST SELECT * FROM system.numbers LIMIT 10) WHERE explain LIKE '%Asterisk%';

SELECT * FROM (
    EXPLAIN AST SELECT * FROM (
        EXPLAIN PLAN SELECT * FROM (
            EXPLAIN SYNTAX SELECT trim(explain) == 'Asterisk' FROM (
                EXPLAIN AST SELECT * FROM system.numbers LIMIT 10
            ) WHERE explain LIKE '%Asterisk%'
        )
    )
) FORMAT Null;

SELECT (EXPLAIN SYNTAX oneline = 1 SELECT 1) == 'SELECT 1';

SELECT * FROM viewExplain('', ''); -- { serverError BAD_ARGUMENTS }
SELECT * FROM viewExplain('EXPLAIN AST', ''); -- { serverError BAD_ARGUMENTS }
SELECT * FROM viewExplain('EXPLAIN AST', '', 1); -- { serverError BAD_ARGUMENTS }
SELECT * FROM viewExplain('EXPLAIN AST', '', ''); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t1 ( a UInt64 ) Engine = MergeTree ORDER BY tuple() AS SELECT number AS a FROM system.numbers LIMIT 100000;

SELECT rows > 1000 FROM (EXPLAIN ESTIMATE SELECT sum(a) FROM t1);
SELECT count() == 1 FROM (EXPLAIN ESTIMATE SELECT sum(a) FROM t1);

DROP TABLE IF EXISTS t1;

SET allow_experimental_analyzer = 1;

SELECT count() > 3 FROM (EXPLAIN PIPELINE header = 1 SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain LIKE '%Header: system.numbers.number__ UInt64%';
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE '%Sort%';
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE '%Sort%';
SELECT count() > 0 FROM (EXPLAIN CURRENT TRANSACTION);
SELECT count() == 1 FROM (EXPLAIN SYNTAX SELECT number FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE 'SELECT%';

-- We have `Identifier number` instead of `Asterisk` because query argument of `viewExplain` table function was analyzed.
-- Compare:
-- :) EXPLAIN AST SELECT *;
-- ┌─explain───────────────────────────┐
-- │ SelectWithUnionQuery (children 1) │
-- │  ExpressionList (children 1)      │
-- │   SelectQuery (children 1)        │
-- │    ExpressionList (children 1)    │
-- │     Asterisk                      │
-- └───────────────────────────────────┘
-- :) SELECT * FROM (EXPLAIN AST SELECT *);
-- ┌─explain─────────────────────────────────────┐
-- │ SelectWithUnionQuery (children 1)           │
-- │  ExpressionList (children 1)                │
-- │   SelectQuery (children 2)                  │
-- │    ExpressionList (children 1)              │
-- │     Identifier dummy                        │
-- │    TablesInSelectQuery (children 1)         │
-- │     TablesInSelectQueryElement (children 1) │
-- │      TableExpression (children 1)           │
-- │       TableIdentifier system.one            │
-- └─────────────────────────────────────────────┘
-- TODO: argument of `viewExplain` (and subquery in `EXAPLAN ...`) should not be analyzed.
-- See _Support query tree in table functions_ in https://github.com/ClickHouse/ClickHouse/issues/42648
SELECT trim(explain) == 'Identifier number' FROM (EXPLAIN AST SELECT * FROM system.numbers LIMIT 10) WHERE explain LIKE '%Identifier number%';

SELECT * FROM (
    EXPLAIN AST SELECT * FROM (
        EXPLAIN PLAN SELECT * FROM (
            EXPLAIN SYNTAX SELECT trim(explain) == 'Asterisk' FROM (
                EXPLAIN AST SELECT * FROM system.numbers LIMIT 10
            ) WHERE explain LIKE '%Asterisk%'
        )
    )
) FORMAT Null;

SELECT (EXPLAIN SYNTAX oneline = 1 SELECT 1) == 'SELECT 1 FROM system.one';

SELECT * FROM viewExplain('', ''); -- { serverError BAD_ARGUMENTS }
SELECT * FROM viewExplain('EXPLAIN AST', ''); -- { serverError BAD_ARGUMENTS }
SELECT * FROM viewExplain('EXPLAIN AST', '', 1); -- { serverError BAD_ARGUMENTS }
SELECT * FROM viewExplain('EXPLAIN AST', '', ''); -- { serverError BAD_ARGUMENTS }

-- EXPLAIN ESTIMATE is not supported in experimental analyzer
