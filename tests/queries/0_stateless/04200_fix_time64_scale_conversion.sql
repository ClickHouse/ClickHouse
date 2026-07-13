-- Test: exercises `convertFieldToTypeImpl` Time64-from-Decimal64 cross-scale path
-- Covers: src/Interpreters/convertFieldToType.cpp:406-423 — `isTime64() && Decimal64`
-- branch with `scale_from != scale_to`. Mirrors the DateTime64 fix in this PR.
-- Without the conversion at 419-421, `INSERT INTO Time64(N) VALUES (toTime64(..., M))`
-- would silently produce wrong values when N != M (raw decimal interpreted at wrong scale).

SET enable_time_time64_type = 1;
SET session_timezone = 'UTC';

-- Target Time64(0): downscale from various source scales.
DROP TABLE IF EXISTS t_time64_target_0;
CREATE TABLE t_time64_target_0 (a Time64(0)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_time64_target_0 VALUES (toTime64('01:02:03', 0));
INSERT INTO t_time64_target_0 VALUES (toTime64('01:02:03.123', 3));
INSERT INTO t_time64_target_0 VALUES (toTime64('01:02:03.123456', 6));
INSERT INTO t_time64_target_0 VALUES (toTime64('01:02:03.123456789', 9));
SELECT toTypeName(a), a FROM t_time64_target_0 ORDER BY a;
DROP TABLE t_time64_target_0;

-- Target Time64(3): mix of upscale (from 0) and downscale (from 6, 9).
DROP TABLE IF EXISTS t_time64_target_3;
CREATE TABLE t_time64_target_3 (a Time64(3)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_time64_target_3 VALUES (toTime64('01:02:03', 0));
INSERT INTO t_time64_target_3 VALUES (toTime64('01:02:03.123', 3));
INSERT INTO t_time64_target_3 VALUES (toTime64('01:02:03.123456', 6));
INSERT INTO t_time64_target_3 VALUES (toTime64('01:02:03.123456789', 9));
SELECT toTypeName(a), a FROM t_time64_target_3 ORDER BY a;
DROP TABLE t_time64_target_3;

-- Target Time64(6): upscale from lower scales (3, 0) and downscale from 9.
DROP TABLE IF EXISTS t_time64_target_6;
CREATE TABLE t_time64_target_6 (a Time64(6)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_time64_target_6 VALUES (toTime64('01:02:03', 0));
INSERT INTO t_time64_target_6 VALUES (toTime64('01:02:03.123', 3));
INSERT INTO t_time64_target_6 VALUES (toTime64('01:02:03.123456', 6));
INSERT INTO t_time64_target_6 VALUES (toTime64('01:02:03.123456789', 9));
SELECT toTypeName(a), a FROM t_time64_target_6 ORDER BY a;
DROP TABLE t_time64_target_6;

-- Target Time64(9): pure upscale (early-return when source is also 9).
DROP TABLE IF EXISTS t_time64_target_9;
CREATE TABLE t_time64_target_9 (a Time64(9)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_time64_target_9 VALUES (toTime64('01:02:03', 0));
INSERT INTO t_time64_target_9 VALUES (toTime64('01:02:03.123', 3));
INSERT INTO t_time64_target_9 VALUES (toTime64('01:02:03.123456', 6));
INSERT INTO t_time64_target_9 VALUES (toTime64('01:02:03.123456789', 9));
SELECT toTypeName(a), a FROM t_time64_target_9 ORDER BY a;
DROP TABLE t_time64_target_9;
