-- The embedded documentation of the SQL statements, exposed by `system.statements`, is also collected by
-- `system.documentation`, as the `Statement` kind of entity.

-- Every registered statement is documented, and the two tables agree on the set of statements.
SELECT count() = (SELECT count() FROM system.statements) FROM system.documentation WHERE type = 'Statement';
SELECT count() = 0 FROM system.statements WHERE name NOT IN (SELECT name FROM system.documentation WHERE type = 'Statement');

-- Representative statements are present.
SELECT name FROM system.documentation
WHERE type = 'Statement' AND name IN ('SELECT', 'INSERT INTO', 'CREATE TABLE', 'WHERE')
ORDER BY name;

-- The rendered document starts with the source-owned description.
SELECT count() = 0
FROM system.statements AS statements
INNER JOIN system.documentation AS documentation
    ON documentation.type = 'Statement' AND documentation.name = statements.name
WHERE NOT startsWith(documentation.description, statements.description);

-- Structured sections are included when the description is not already a complete page.
SELECT description LIKE '%**Syntax**%' AND description LIKE '%CREATE DATABASE ...%'
FROM system.documentation WHERE type = 'Statement' AND name = 'CREATE';

-- Structured syntax is not appended when the source description already contains it.
SELECT count() = 0
FROM system.statements AS statements
INNER JOIN system.documentation AS documentation
    ON documentation.type = 'Statement' AND documentation.name = statements.name
WHERE notEmpty(statements.syntax)
    AND position(statements.description, statements.syntax) > 0
    AND countSubstrings(documentation.description, statements.syntax)
        > countSubstrings(statements.description, statements.syntax);

-- A source description with an explicit syntax section or an SQL block introduced as syntax is not followed by
-- another synthetic syntax section, even when its formatting or punctuation differs from the structured syntax.
SELECT name, countSubstrings(description, '**Syntax**')
FROM system.documentation
WHERE type = 'Statement'
    AND name IN ('ALTER NAMED COLLECTION', 'ALTER QUOTA', 'ALTER TABLE ... CONSTRAINT')
ORDER BY name;

-- Complete pages are exposed verbatim, without synthetic enclosing-statement sections.
SELECT description NOT LIKE '%**Part of:**%'
FROM system.documentation WHERE type = 'Statement' AND name = 'WHERE';
SELECT description NOT LIKE '%**Part of:**%'
FROM system.documentation WHERE type = 'Statement' AND name = 'SELECT';

-- Every statement carries the source file of the parser which documents it.
SELECT count() FROM system.documentation
WHERE type = 'Statement' AND (source = '' OR source NOT LIKE 'src/Parsers/%');
