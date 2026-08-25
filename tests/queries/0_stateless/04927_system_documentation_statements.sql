-- The embedded documentation of the SQL statements, exposed by `system.statements`, is also collected by
-- `system.documentation`, as the `Statement` kind of entity.

-- Every registered statement is documented, and the two tables agree on the set of statements.
SELECT count() = (SELECT count() FROM system.statements) FROM system.documentation WHERE type = 'Statement';
SELECT count() = 0 FROM system.statements WHERE name NOT IN (SELECT name FROM system.documentation WHERE type = 'Statement');

-- Representative statements are present.
SELECT name FROM system.documentation
WHERE type = 'Statement' AND name IN ('SELECT', 'INSERT INTO', 'CREATE TABLE', 'WHERE')
ORDER BY name;

-- The complete statement page is exposed verbatim through both system tables.
SELECT count() = 0
FROM system.statements AS statements
INNER JOIN system.documentation AS documentation
    ON documentation.type = 'Statement' AND documentation.name = statements.name
WHERE statements.description != documentation.description;

-- Complete pages are exposed verbatim, without synthetic enclosing-statement sections.
SELECT description NOT LIKE '%**Part of:**%'
FROM system.documentation WHERE type = 'Statement' AND name = 'WHERE';
SELECT description NOT LIKE '%**Part of:**%'
FROM system.documentation WHERE type = 'Statement' AND name = 'SELECT';

-- Every statement carries the source file of the parser which documents it.
SELECT count() FROM system.documentation
WHERE type = 'Statement' AND (source = '' OR source NOT LIKE 'src/Parsers/%');
