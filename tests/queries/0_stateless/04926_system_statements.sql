-- SQL statements expose their embedded documentation via system.statements.

-- Representative statements must be registered, with a non-empty description and syntax.
SELECT name, length(description) > 0 AS has_description, length(syntax) > 0 AS has_syntax
FROM system.statements
WHERE name IN ('SELECT', 'INSERT INTO', 'CREATE TABLE', 'DROP', 'ALTER', 'SYSTEM')
ORDER BY name;

-- Every statement must have a syntax and a description, and the description must contain usage examples.
SELECT count() = 0
FROM system.statements
WHERE empty(syntax) OR empty(description) OR position(description, '**Examples**') = 0;

-- The syntax of a statement mentions the statement itself.
SELECT position(syntax, 'DROP DATABASE') > 0
FROM system.statements
WHERE name = 'DROP';

-- Clauses of SELECT and sub-statements of ALTER refer to their parent statement.
SELECT name, parent_statement
FROM system.statements
WHERE name IN ('WHERE', 'GROUP BY', 'ALTER TABLE ... UPDATE', 'CREATE TABLE', 'CREATE TEMPORARY TABLE')
ORDER BY name;

-- Top-level statements have no parent statement.
SELECT parent_statement = ''
FROM system.statements
WHERE name = 'SELECT';

-- Every parent statement must itself be a registered statement.
SELECT count() = 0
FROM system.statements
WHERE parent_statement != '' AND parent_statement NOT IN (SELECT name FROM system.statements);

-- The related statements are exposed as an array of statement names.
SELECT related
FROM system.statements
WHERE name = 'UNDROP';

-- Every related statement must itself be a registered statement.
SELECT count() = 0
FROM
(
    SELECT arrayJoin(related) AS related_statement
    FROM system.statements
)
WHERE related_statement NOT IN (SELECT name FROM system.statements);

-- A statement is registered only once, and the statements of all the documented families are present.
SELECT uniqExact(name) = count() FROM system.statements;
SELECT countIf(parent_statement = '') > 0, countIf(parent_statement = 'SELECT') > 0, countIf(parent_statement = 'ALTER') > 0, countIf(parent_statement = 'CREATE') > 0
FROM system.statements;
