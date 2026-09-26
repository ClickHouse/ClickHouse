-- Database engines expose embedded documentation via system.database_engines.

-- Representative engines must have a non-empty description and syntax.
-- Only engines that are always compiled in are listed here, so the test is build-independent.
SELECT name, length(description) > 0 AS has_description, length(syntax) > 0 AS has_syntax
FROM system.database_engines
WHERE name IN ('Atomic', 'Replicated', 'Memory')
ORDER BY name;

-- The Atomic documentation is migrated in full, so it mentions the engine.
SELECT position(description, 'Atomic') > 0
FROM system.database_engines
WHERE name = 'Atomic';

-- Related engines are exposed as an array.
SELECT related
FROM system.database_engines
WHERE name = 'Atomic';

-- Every related engine referenced by an always-present engine must itself be a registered engine.
SELECT count() = 0
FROM
(
    SELECT arrayJoin(related) AS related_engine
    FROM system.database_engines
    WHERE name IN ('Atomic', 'Replicated', 'Memory', 'Ordinary')
)
WHERE related_engine NOT IN (SELECT name FROM system.database_engines);
