-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.

DROP TYPE IF EXISTS EmptyClauses;
DROP TYPE IF EXISTS NoClauses;

-- An empty string literal in INPUT / OUTPUT / DEFAULT is a present clause with an empty
-- value, and must stay distinguishable from an absent clause (NULL).
CREATE TYPE EmptyClauses AS UInt64 INPUT '' OUTPUT '' DEFAULT '';
CREATE TYPE NoClauses AS UInt64;

SELECT name, input_expression IS NULL, output_expression IS NULL, default_expression IS NULL FROM system.user_defined_types WHERE name IN ('EmptyClauses', 'NoClauses') ORDER BY name;
SHOW TYPE EmptyClauses;
SHOW TYPE NoClauses;

DROP TYPE EmptyClauses;
DROP TYPE NoClauses;
