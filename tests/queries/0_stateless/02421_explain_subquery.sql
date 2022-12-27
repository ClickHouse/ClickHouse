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

-- Note: here we have `Identifier number` instead of `Asterisk` because it's resolved in analyzer.
-- Without subsquery it still would be `Asterisk`, because in that case analyzer it's not used by InterpreterExplainQuery.
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

-- EXPLAIN ESTIMATE is not supported in experimental analyzer
