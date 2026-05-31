-- Database engines expose embedded documentation via system.database_engines.

-- Representative engines must have a non-empty description and syntax.
SELECT name, length(description) > 0 AS has_description, length(syntax) > 0 AS has_syntax
FROM system.database_engines
WHERE name IN ('Atomic', 'Replicated', 'MySQL', 'PostgreSQL', 'Memory')
ORDER BY name;

-- The Atomic documentation is migrated in full, so it mentions the engine.
SELECT position(description, 'Atomic') > 0
FROM system.database_engines
WHERE name = 'Atomic';

-- Related engines are exposed as an array.
SELECT related
FROM system.database_engines
WHERE name = 'Atomic';
