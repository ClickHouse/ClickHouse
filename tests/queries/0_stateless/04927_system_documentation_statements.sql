-- The embedded documentation of the SQL statements, exposed by `system.statements`, is also collected by
-- `system.documentation`, as the `Statement` kind of entity.

-- Every registered statement is documented, and the two tables agree on the set of statements.
SELECT count() = (SELECT count() FROM system.statements) FROM system.documentation WHERE type = 'Statement';
SELECT count() = 0 FROM system.statements WHERE name NOT IN (SELECT name FROM system.documentation WHERE type = 'Statement');

-- Representative statements are present.
SELECT name FROM system.documentation
WHERE type = 'Statement' AND name IN ('SELECT', 'INSERT INTO', 'CREATE TABLE', 'WHERE')
ORDER BY name;

-- The documentation of a statement is rendered as Markdown assembled from the structured parts.
SELECT description LIKE '%**Syntax**%' AND description LIKE '%**Examples**%'
FROM system.documentation WHERE type = 'Statement' AND name = 'WHERE';

-- The enclosing statement is rendered as well, and a top-level statement has none.
SELECT description LIKE '%**Part of:** `SELECT`%'
FROM system.documentation WHERE type = 'Statement' AND name = 'WHERE';
SELECT description LIKE '%**Part of:**%'
FROM system.documentation WHERE type = 'Statement' AND name = 'SELECT';

-- Every statement carries the source file of the parser which documents it.
SELECT count() FROM system.documentation
WHERE type = 'Statement' AND (source = '' OR source NOT LIKE 'src/Parsers/%');
