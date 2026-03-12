-- Regression test: server must enforce type guard settings for INSERT queries
-- regardless of client version or client-side settings.
-- A TCP native client with different defaults could previously bypass the server's
-- type restrictions by evaluating CAST client-side and sending a binary block.

-- Setup: create tables while types are allowed
SET enable_time_time64_type = 1;
CREATE TABLE t_time (t Time, t64 Time64(3)) ENGINE = Memory;

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_lc (x LowCardinality(Int64)) ENGINE = Memory;

-- With Time type disabled, INSERT VALUES must be rejected by the server
SET enable_time_time64_type = 0;
INSERT INTO t_time VALUES ('12:00:00', '12:00:00.000'); -- { serverError ILLEGAL_COLUMN }

-- INSERT SELECT must also be rejected (purely server-side path)
INSERT INTO t_time SELECT toTime(3600), toTime64(3600, 3); -- { serverError ILLEGAL_COLUMN }

-- Re-enabling the setting allows INSERT to succeed
SET enable_time_time64_type = 1;
INSERT INTO t_time VALUES ('12:00:00', '12:00:00.000');
SELECT t, t64 FROM t_time;

-- Same enforcement applies to allow_suspicious_low_cardinality_types
SET allow_suspicious_low_cardinality_types = 0;
INSERT INTO t_lc VALUES (42); -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }

-- Re-enabling the setting allows INSERT to succeed
SET allow_suspicious_low_cardinality_types = 1;
INSERT INTO t_lc VALUES (42);
SELECT x FROM t_lc;

DROP TABLE t_time;
DROP TABLE t_lc;
