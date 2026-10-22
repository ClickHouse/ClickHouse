-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.

DROP TYPE IF EXISTS Pair;

-- A repeated formal parameter name would be substituted from its last occurrence only.
CREATE TYPE Pair(T, T) AS Tuple(T, T); -- { serverError BAD_ARGUMENTS }

-- Distinct names are fine, and the type is not left half-registered by the rejection above.
CREATE TYPE Pair(T, U) AS Tuple(T, U);
SHOW TYPE Pair;
SELECT toTypeName(CAST(('a', 1), 'Pair(String, UInt8)'));
DROP TYPE Pair;
