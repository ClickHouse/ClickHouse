-- SQL statements expose their embedded documentation via system.statements.

-- Representative statements must be registered, with a non-empty description and syntax.
SELECT name, length(description) > 0 AS has_description, length(syntax) > 0 AS has_syntax
FROM system.statements
WHERE name IN ('SELECT', 'INSERT INTO', 'CREATE TABLE', 'DROP', 'ALTER', 'SYSTEM')
ORDER BY name;

-- Every statement must have a syntax and a description.
SELECT count() = 0
FROM system.statements
WHERE empty(syntax) OR empty(description);

-- The syntax of a statement mentions the statement itself.
SELECT position(syntax, 'DROP DATABASE') > 0
FROM system.statements
WHERE name = 'DROP';

-- Clauses of SELECT and sub-statements of ALTER refer to their parent statement.
SELECT name, parent_name
FROM system.statements
WHERE name IN ('WHERE', 'GROUP BY', 'ALTER TABLE ... UPDATE', 'CREATE TABLE', 'CREATE TEMPORARY TABLE')
ORDER BY name;

-- Top-level statements have no parent statement.
SELECT parent_name = ''
FROM system.statements
WHERE name = 'SELECT';

-- Every parent statement must itself be a registered statement.
SELECT count() = 0
FROM system.statements
WHERE parent_name != '' AND parent_name NOT IN (SELECT name FROM system.statements);

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
SELECT countIf(parent_name = '') > 0, countIf(parent_name = 'SELECT') > 0, countIf(parent_name = 'ALTER') > 0, countIf(parent_name = 'CREATE') > 0
FROM system.statements;

-- The complete set of migrated statement documentation must stay registered.
SELECT name FROM system.statements ORDER BY name;
