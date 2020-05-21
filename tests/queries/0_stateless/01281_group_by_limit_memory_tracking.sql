DROP TABLE IF EXISTS trace_log_01281;
DROP TABLE IF EXISTS trace_log_01281_mv;
DROP TABLE IF EXISTS trace_log_01281_assert;

-- better alternative will be to TRUNCATE TABLE system.*_log
-- but this will be a separate issue
CREATE TABLE trace_log_01281 AS system.trace_log Engine=Memory();
CREATE MATERIALIZED VIEW trace_log_01281_mv TO trace_log_01281 AS SELECT * FROM system.trace_log WHERE trace_type = 'MemorySample';
CREATE VIEW trace_log_01281_assert AS SELECT
        *,
        throwIf(cnt < 0,                   'no memory profile captured'),
-- this check is disabled because it's not reliable: throwIf(queries > 1, 'too many queries'),
        throwIf(alloc < 100e6,             'minimal allocation had not been done'),
        throwIf((alloc+free)/alloc > 0.05, 'memory accounting leaked more than 5%')
    FROM (
        SELECT
            count() cnt,
            uniq(query_id) queries,
            sumIf(size, size > 0) alloc,
            sumIf(size, size < 0) free
        FROM trace_log_01281
    );

--
-- Basic
-- NOTE: 0 (and even 1e6) is too small, will make SYSTEM FLUSH LOGS too slow
-- (in debug build at least)
--
SET max_untracked_memory=4e6;

TRUNCATE TABLE trace_log_01281;
-- single {
SET max_threads=1;
SET memory_profiler_sample_probability=1;
SELECT uniqExactState(number) FROM numbers(toUInt64(2e6)) GROUP BY number % 2e5 FORMAT Null;
SET memory_profiler_sample_probability=0;
SYSTEM FLUSH LOGS;
-- }
SELECT * FROM trace_log_01281_assert FORMAT Null;

SYSTEM FLUSH LOGS;
TRUNCATE TABLE trace_log_01281;
-- single limit {
SET max_threads=1;
SET memory_profiler_sample_probability=1;
SELECT uniqExactState(number) FROM numbers(toUInt64(2e6)) GROUP BY number % 2e5 LIMIT 10 FORMAT Null;
SET memory_profiler_sample_probability=0;
SYSTEM FLUSH LOGS;
-- }
SELECT * FROM trace_log_01281_assert FORMAT Null;

SYSTEM FLUSH LOGS;
TRUNCATE TABLE trace_log_01281;
-- two-level {
-- need to have multiple threads for two-level aggregation
SET max_threads=2;
SET memory_profiler_sample_probability=1;
SELECT uniqExactState(number) FROM numbers_mt(toUInt64(2e6)) GROUP BY number % 2e5 FORMAT Null;
SET memory_profiler_sample_probability=0;
SYSTEM FLUSH LOGS;
-- }
SELECT * FROM trace_log_01281_assert FORMAT Null;

SYSTEM FLUSH LOGS;
TRUNCATE TABLE trace_log_01281;
-- two-level limit {
-- need to have multiple threads for two-level aggregation
SET max_threads=2;
SET memory_profiler_sample_probability=1;
SELECT uniqExactState(number) FROM numbers_mt(toUInt64(2e6)) GROUP BY number % 2e5 LIMIT 10 FORMAT Null;
SET memory_profiler_sample_probability=0;
SYSTEM FLUSH LOGS;
-- }
SELECT * FROM trace_log_01281_assert FORMAT Null;

SYSTEM FLUSH LOGS;
TRUNCATE TABLE trace_log_01281;
-- two-level MEMORY_LIMIT_EXCEEDED {
-- need to have multiple threads for two-level aggregation
SET max_threads=2;
SET memory_profiler_sample_probability=1;
SET max_memory_usage='150M';
SELECT uniqExactState(number) FROM numbers_mt(toUInt64(10e6)) GROUP BY number % 1e6 FORMAT Null; -- { serverError 241; }
SET memory_profiler_sample_probability=0;
SET max_memory_usage=0;
SYSTEM FLUSH LOGS;
-- }
SELECT * FROM trace_log_01281_assert FORMAT Null;
