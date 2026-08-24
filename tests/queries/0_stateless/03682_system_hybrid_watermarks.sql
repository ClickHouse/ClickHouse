-- Tags: no-parallel
-- Tag no-parallel: uses a process-wide failpoint

SET allow_experimental_hybrid_table = 1;

DROP TABLE IF EXISTS local_hot SYNC;
DROP TABLE IF EXISTS local_warm SYNC;
DROP TABLE IF EXISTS local_cold SYNC;
DROP TABLE IF EXISTS t SYNC;
DROP TABLE IF EXISTS t3 SYNC;
DROP TABLE IF EXISTS t_no_params SYNC;
DROP TABLE IF EXISTS dist_plain SYNC;
DROP TABLE IF EXISTS mt SYNC;

CREATE TABLE local_hot  (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;
CREATE TABLE local_warm (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;
CREATE TABLE local_cold (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;

-- =====================================================
-- 1. Healthy N=1 case
-- =====================================================
SELECT '--- Test 1: Healthy N=1';
CREATE TABLE t
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
)
SETTINGS hybrid_watermark_hot = '2025-09-01'
AS local_hot;

SELECT database = currentDatabase(), table, name, value, type, last_exception
FROM system.hybrid_watermarks
WHERE database = currentDatabase() AND table = 't'
ORDER BY name;

SELECT count() FROM system.hybrid_watermarks WHERE database = currentDatabase() AND table = 't';

-- =====================================================
-- 2. ALTER refreshes value, row count stays at 1
-- =====================================================
SELECT '--- Test 2: ALTER refresh';
ALTER TABLE t MODIFY SETTING hybrid_watermark_hot = '2025-10-01';
SELECT name, value, type
FROM system.hybrid_watermarks
WHERE database = currentDatabase() AND table = 't'
ORDER BY name;

SELECT count() FROM system.hybrid_watermarks WHERE database = currentDatabase() AND table = 't';

-- =====================================================
-- 3. Multi-watermark ordering is stable (sorted by name)
-- =====================================================
SELECT '--- Test 3: Multi-watermark ordering';
CREATE TABLE t3
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_warm'),
        ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
        AND ts > hybridParam('hybrid_watermark_cold', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_cold', 'DateTime')
)
SETTINGS hybrid_watermark_hot = '2025-10-01', hybrid_watermark_cold = '2025-01-01'
AS local_hot;

SELECT name, value, type
FROM system.hybrid_watermarks
WHERE database = currentDatabase() AND table = 't3'
ORDER BY name;

SELECT count() FROM system.hybrid_watermarks WHERE database = currentDatabase() AND table = 't3';

-- =====================================================
-- 4. Zero-declared case: Hybrid with no hybridParam() emits zero rows
-- =====================================================
SELECT '--- Test 4: Zero-declared Hybrid emits zero rows';
CREATE TABLE t_no_params
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > toDateTime('2025-09-01'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= toDateTime('2025-09-01')
)
AS local_hot;

SELECT count() FROM system.hybrid_watermarks WHERE database = currentDatabase() AND table = 't_no_params';

-- =====================================================
-- 5. Non-Hybrid exclusion: MergeTree and plain Distributed never appear
-- =====================================================
SELECT '--- Test 5: Non-Hybrid exclusion';
CREATE TABLE mt (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;
CREATE TABLE dist_plain AS remote('localhost:9000', currentDatabase(), 'local_hot');

SELECT count() FROM system.hybrid_watermarks
WHERE database = currentDatabase() AND table IN ('mt', 'dist_plain', 'local_hot', 'local_warm', 'local_cold');

-- =====================================================
-- 6. Baseline: no stuck diagnostic rows on a healthy cluster
-- =====================================================
SELECT '--- Test 6: No diagnostic rows on healthy cluster';
SELECT count() FROM system.hybrid_watermarks
WHERE database = currentDatabase() AND last_exception != '';

-- =====================================================
-- 7. Diagnostic row path: enable failpoint, see exactly one row with last_exception populated
-- =====================================================
SELECT '--- Test 7: last_exception path via failpoint';
SYSTEM ENABLE FAILPOINT hybrid_watermarks_read_fail;

-- Every in-scope Hybrid table collapses to a single diagnostic row.
-- t: 1 diagnostic row; t3: 1 diagnostic row; t_no_params: still 1 diagnostic row
-- because the failpoint fires before the zero-declared check.
SELECT table, name, value, type, last_exception != '' AS has_exception
FROM system.hybrid_watermarks
WHERE database = currentDatabase() AND table IN ('t', 't3', 't_no_params')
ORDER BY table;

SELECT count() FROM system.hybrid_watermarks
WHERE database = currentDatabase() AND table = 't' AND last_exception != '';

SELECT count() FROM system.hybrid_watermarks
WHERE database = currentDatabase() AND table = 't' AND name = '' AND value = '' AND type = '';

SYSTEM DISABLE FAILPOINT hybrid_watermarks_read_fail;

-- =====================================================
-- 8. Back to healthy state after failpoint disabled
-- =====================================================
SELECT '--- Test 8: Healthy after failpoint disabled';
SELECT name, value, type, last_exception
FROM system.hybrid_watermarks
WHERE database = currentDatabase() AND table = 't'
ORDER BY name;

SELECT count() FROM system.hybrid_watermarks
WHERE database = currentDatabase() AND last_exception != '';

-- =====================================================
-- 9. Temporary Hybrid tables are visible (emitted with database = '')
-- =====================================================
SELECT '--- Test 9: Temporary Hybrid table visibility';
CREATE TEMPORARY TABLE tmp_hybrid
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
)
SETTINGS hybrid_watermark_hot = '2025-11-15'
AS local_hot;

SELECT database, table, name, value, type, last_exception
FROM system.hybrid_watermarks
WHERE database = '' AND table = 'tmp_hybrid'
ORDER BY name;

SELECT count() FROM system.hybrid_watermarks WHERE database = '' AND table = 'tmp_hybrid';

DROP TEMPORARY TABLE tmp_hybrid;

-- =====================================================
-- Cleanup
-- =====================================================
DROP TABLE IF EXISTS t SYNC;
DROP TABLE IF EXISTS t3 SYNC;
DROP TABLE IF EXISTS t_no_params SYNC;
DROP TABLE IF EXISTS dist_plain SYNC;
DROP TABLE IF EXISTS mt SYNC;
DROP TABLE IF EXISTS local_hot SYNC;
DROP TABLE IF EXISTS local_warm SYNC;
DROP TABLE IF EXISTS local_cold SYNC;
