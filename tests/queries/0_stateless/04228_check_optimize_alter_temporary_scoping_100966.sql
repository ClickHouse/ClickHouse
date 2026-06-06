-- Test for https://github.com/ClickHouse/ClickHouse/pull/100966 follow-up.
-- Verify CHECK, OPTIMIZE, and ALTER TABLE use the same scoping precedence
-- as SHOW CREATE TABLE / DESCRIBE TABLE: an unqualified name resolves to the
-- TEMPORARY table when one exists, preferring it over a same-named permanent.

SET describe_compact_output = 1;

-- ============================================================================
-- CHECK TABLE: previously used Context::ResolveOrdinary and ignored TEMPORARY.
-- ============================================================================

-- (1) CHECK TABLE on a TEMPORARY-only table now succeeds (was UNKNOWN_TABLE).
DROP TABLE IF EXISTS t SYNC;
CREATE TEMPORARY TABLE t (x UInt64) ENGINE = Log;
INSERT INTO t VALUES (1), (2), (3);

SELECT 'CHECK on TEMPORARY-only Log table returns success:';
CHECK TABLE t SETTINGS check_query_single_value_result = 1;

DROP TABLE t;

-- (2) When a TEMPORARY and a permanent table both exist (Memory + MergeTree),
--     CHECK TABLE now targets the TEMPORARY one. Memory doesn't support CHECK,
--     so the query errors out -- previously it would have succeeded against the
--     permanent MergeTree.
CREATE TABLE t (hello String) ENGINE = MergeTree ORDER BY hello;
INSERT INTO t VALUES ('p1'), ('p2'), ('p3');
CREATE TEMPORARY TABLE t (world UInt64) ENGINE = Memory;

SELECT 'CHECK on collision targets TEMPORARY Memory (rejects):';
CHECK TABLE t; -- { serverError NOT_IMPLEMENTED }
SELECT 'OK';

DROP TEMPORARY TABLE t;
DROP TABLE t;

-- ============================================================================
-- OPTIMIZE TABLE: already used the default ResolveAll. Lock in.
-- ============================================================================
CREATE TABLE t (hello String) ENGINE = MergeTree ORDER BY hello;
CREATE TEMPORARY TABLE t (world UInt64) ENGINE = Memory;

SELECT 'OPTIMIZE on collision targets TEMPORARY Memory (rejects):';
OPTIMIZE TABLE t; -- { serverError NOT_IMPLEMENTED }
SELECT 'OK';

DROP TEMPORARY TABLE t;
DROP TABLE t;

-- ============================================================================
-- ALTER TABLE: already used tryResolveStorageID with default ResolveAll. Lock in.
-- ============================================================================
CREATE TABLE t (hello String) ENGINE = MergeTree ORDER BY hello;
CREATE TEMPORARY TABLE t (world UInt64) ENGINE = Memory;

ALTER TABLE t ADD COLUMN z UInt8;

SELECT 'ALTER on collision modifies TEMPORARY (z appears in DESCRIBE):';
DESCRIBE TEMPORARY TABLE t;

SELECT 'ALTER on collision does not modify permanent (no z in system.columns):';
SELECT count() FROM system.columns
WHERE database = currentDatabase() AND table = 't' AND name = 'z';

DROP TEMPORARY TABLE t;
DROP TABLE t;
