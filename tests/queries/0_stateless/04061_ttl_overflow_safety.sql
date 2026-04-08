-- Tags: no-fasttest

-- Verify that TTL expression overflow is detected and blocked instead of silently
-- deleting data. Previously, Date + toIntervalDay(N) used UInt16 arithmetic that
-- silently wrapped on overflow, corrupting part min/max TTL metadata and causing
-- entire parts to be dropped during merge.
--
-- The fix evaluates the TTL expression twice inside the TTL path only:
-- once with the original types and once with widened `Date32`/`DateTime64` inputs.
-- If the timestamps differ, TTL arithmetic overflow is detected and the operation
-- is aborted before TTL metadata is updated or rows are deleted.

-- Case 1: `MODIFY TTL` should fail for overflowing `Date` arithmetic.
DROP TABLE IF EXISTS t_ttl_overflow;
CREATE TABLE t_ttl_overflow (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_overflow VALUES ('2024-01-01', 1), ('2024-06-15', 2), ('2025-12-31', 3);
ALTER TABLE t_ttl_overflow MODIFY TTL day + toIntervalDay(46000); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- Data must remain intact.
SELECT count() FROM t_ttl_overflow;
DROP TABLE t_ttl_overflow;

-- Case 2: materialize_ttl_after_modify = 0, then explicit MATERIALIZE TTL.
DROP TABLE IF EXISTS t_ttl_overflow2;
CREATE TABLE t_ttl_overflow2 (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_overflow2 VALUES ('2024-01-01', 10), ('2024-06-15', 20);
SET materialize_ttl_after_modify = 0;
ALTER TABLE t_ttl_overflow2 MODIFY TTL day + toIntervalDay(99999);
-- Metadata changed but no materialization yet; data is intact.
SELECT count() FROM t_ttl_overflow2;
-- Explicit `MATERIALIZE TTL` should fail with overflow error.
ALTER TABLE t_ttl_overflow2 MATERIALIZE TTL; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT count() FROM t_ttl_overflow2;
SET materialize_ttl_after_modify = 1;
DROP TABLE t_ttl_overflow2;

-- Case 3: Normal (non-overflowing) TTL still works correctly.
DROP TABLE IF EXISTS t_ttl_normal;
CREATE TABLE t_ttl_normal (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_normal VALUES ('2000-01-01', 100);
ALTER TABLE t_ttl_normal MODIFY TTL day + INTERVAL 1 DAY;
OPTIMIZE TABLE t_ttl_normal FINAL;
SELECT count() FROM t_ttl_normal;
DROP TABLE t_ttl_normal;
