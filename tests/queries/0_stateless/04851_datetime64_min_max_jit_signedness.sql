-- The JIT-compiled min/max must compare DateTime64 tick counts as signed, so timestamps before
-- 1970 (negative ticks) do not win max. Both settings are randomized in CI, so pin them here.
SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

-- The GROUP BY key is what reaches the compiled path at all (aggregation without a key never
-- compiles), and it must be wider than 8 bits or the 8-bit lookup table bypasses the compiled add.
DROP TABLE IF EXISTS t_jit_dt64;
CREATE TABLE t_jit_dt64 (k Int64, dt0 DateTime64(0), dt3 DateTime64(3), dt9 DateTime64(9))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_jit_dt64 VALUES (1, '1994-04-23 09:17:42', '1994-04-23 09:17:42', '1994-04-23 09:17:42'), (1, '2001-03-15 11:23:37', '2001-03-15 11:23:37', '2001-03-15 11:23:37'), (1, '1945-04-25 16:24:14', '1945-04-25 16:24:14', '1945-04-25 16:24:14');

SELECT 'scale 0', min(dt0), max(dt0) FROM t_jit_dt64 GROUP BY k ORDER BY k;
SELECT 'scale 3', min(dt3), max(dt3) FROM t_jit_dt64 GROUP BY k ORDER BY k;
SELECT 'scale 9', min(dt9), max(dt9) FROM t_jit_dt64 GROUP BY k ORDER BY k;

-- The Nullable and If combinators delegate the compiled comparison to the same nested state.
SELECT 'nullable', min(ndt), max(ndt) FROM (SELECT k, toNullable(dt3) AS ndt FROM t_jit_dt64) GROUP BY k ORDER BY k;
SELECT 'minIf', minIf(dt3, k = 1), maxIf(dt3, k = 1) FROM t_jit_dt64 GROUP BY k ORDER BY k;

-- Many groups over two-level hash tables, spanning the epoch. Whether a given run also merges
-- partial states across threads depends on how the reader splits the part, so this asserts the
-- result, not the path.
DROP TABLE IF EXISTS t_jit_dt64_merge;
CREATE TABLE t_jit_dt64_merge (k Int64, dt DateTime64(3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_jit_dt64_merge
SELECT number % 8, toDateTime64('1970-01-01 00:00:00', 3) + toIntervalSecond(number - 5000) FROM numbers(10000);

WITH toDateTime64('1970-01-01 00:00:00', 3) AS epoch
SELECT 'merge', countIf(mn > mx),
       min(mn) = epoch - toIntervalSecond(5000),
       max(mx) = epoch + toIntervalSecond(4999)
FROM
(
    SELECT k, min(dt) AS mn, max(dt) AS mx FROM t_jit_dt64_merge GROUP BY k
    SETTINGS max_threads = 16, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1
);

-- Time64 shares the trait gap but is not JIT compilable today, so this only guards it staying right.
DROP TABLE IF EXISTS t_jit_time64;
CREATE TABLE t_jit_time64 (k Int64, tm Time64(3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_jit_time64 VALUES (1, '-100:00:00'), (1, '050:00:00'), (1, '-999:00:00');

SELECT 'time64', min(tm), max(tm) FROM t_jit_time64 GROUP BY k ORDER BY k;

DROP TABLE t_jit_dt64;
DROP TABLE t_jit_dt64_merge;
DROP TABLE t_jit_time64;
