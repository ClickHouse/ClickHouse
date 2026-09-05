-- Tags: no-parallel-replicas
-- no-parallel-replicas: settings randomizer may enable parallel replicas which requires a cluster not present here
-- Coverage test for use_streaming_marks_compression = 1
-- Exercises MergeTreeMarksLoader::loadMarksImpl streaming code path
-- (MergeTreeMarksLoader.cpp lines 194-195, 215-227, 249-271, 284-286).
-- Without the setting the whole streaming branch is never taken in any test.

-- -----------------------------------------------------------------------
-- 1. Wide format, adaptive granularity (default) — adaptive streaming path
-- -----------------------------------------------------------------------
CREATE TABLE wide_adaptive (a UInt64, b String)
ENGINE = MergeTree ORDER BY a;

INSERT INTO wide_adaptive SELECT number, toString(number) FROM numbers(10000);

SYSTEM DROP MARK CACHE;
SELECT count(), sum(a) FROM wide_adaptive SETTINGS use_streaming_marks_compression = 1;

-- Baseline without streaming (must match)
SYSTEM DROP MARK CACHE;
SELECT count(), sum(a) FROM wide_adaptive;

DROP TABLE wide_adaptive;

-- -----------------------------------------------------------------------
-- 2. Wide format, constant granularity (index_granularity_bytes = 0) — non-adaptive streaming path
-- -----------------------------------------------------------------------
CREATE TABLE wide_constant (a UInt64, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO wide_constant SELECT number, toString(number) FROM numbers(10000);

SYSTEM DROP MARK CACHE;
SELECT count(), sum(a) FROM wide_constant SETTINGS use_streaming_marks_compression = 1;

SYSTEM DROP MARK CACHE;
SELECT count(), sum(a) FROM wide_constant;

DROP TABLE wide_constant;

-- -----------------------------------------------------------------------
-- 3. Compact format — compact marks streaming path (many columns in one mark)
-- -----------------------------------------------------------------------
CREATE TABLE compact_multi (a UInt64, b UInt64, c UInt64, d UInt64, e String)
ENGINE = MergeTree ORDER BY a
SETTINGS min_rows_for_wide_part = 100000, min_bytes_for_wide_part = 10000000;

INSERT INTO compact_multi SELECT number, number * 2, number * 3, number * 4, toString(number) FROM numbers(1000);

-- Verify part type is Compact
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 'compact_multi' AND active
ORDER BY part_type;

SYSTEM DROP MARK CACHE;
SELECT count(), sum(a), sum(b) FROM compact_multi SETTINGS use_streaming_marks_compression = 1;

SYSTEM DROP MARK CACHE;
SELECT count(), sum(a), sum(b) FROM compact_multi;

DROP TABLE compact_multi;
