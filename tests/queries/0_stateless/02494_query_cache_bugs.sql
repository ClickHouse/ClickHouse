-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Test for Bug 56258

SYSTEM DROP QUERY CACHE;

SELECT '-- Bug 56258: Check literals (ASTLiteral)';

SELECT 10 FORMAT Vertical SETTINGS use_query_cache = 1;
SELECT 10 AS x FORMAT Vertical SETTINGS use_query_cache = 1;

SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT '-- Bug 56258: Check functions (ASTFunction)';

SELECT toUInt64(42) FORMAT Vertical SETTINGS use_query_cache = 1;
SELECT toUInt64(42) AS x FORMAT Vertical SETTINGS use_query_cache = 1;

SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT '-- Bug 56258: Check identifiers (ASTIdentifier)';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(c UInt64) ENGINE = Memory AS SELECT 1;

SELECT c FROM tab FORMAT Vertical SETTINGS use_query_cache = 1;
SELECT c AS x FROM tab FORMAT Vertical SETTINGS use_query_cache = 1;

SELECT count(*) FROM system.query_cache;

DROP TABLE tab;

SYSTEM DROP QUERY CACHE;
