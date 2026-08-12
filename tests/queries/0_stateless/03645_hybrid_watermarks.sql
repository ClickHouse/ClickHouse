SET allow_experimental_hybrid_table = 1;

DROP TABLE IF EXISTS local_hot SYNC;
DROP TABLE IF EXISTS local_cold SYNC;
DROP TABLE IF EXISTS local_warm SYNC;
DROP TABLE IF EXISTS t SYNC;
DROP TABLE IF EXISTS t3 SYNC;
DROP TABLE IF EXISTS t_bad_param SYNC;
DROP TABLE IF EXISTS t_missing SYNC;
DROP TABLE IF EXISTS t_dist_setting SYNC;
DROP TABLE IF EXISTS t_settings_only SYNC;
DROP TABLE IF EXISTS t_conflict SYNC;
DROP TABLE IF EXISTS dist SYNC;

CREATE TABLE local_hot (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;
CREATE TABLE local_cold (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;
CREATE TABLE local_warm (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;

INSERT INTO local_hot VALUES ('2025-10-15', 1), ('2025-11-01', 2);
INSERT INTO local_cold VALUES ('2025-08-01', 3), ('2025-06-15', 4);
INSERT INTO local_warm VALUES ('2025-09-01', 5), ('2025-09-15', 6);

-- =====================================================
-- 1. CREATE with watermarks — basic two-segment case
-- =====================================================
SELECT '--- Test 1: CREATE with watermarks';
CREATE TABLE t
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
)
SETTINGS hybrid_watermark_hot = '2025-09-01'
AS local_hot;

-- =====================================================
-- 2. Verify SHOW CREATE TABLE shows template + setting
-- =====================================================
SELECT '--- Test 2: SHOW CREATE TABLE';
SHOW CREATE TABLE t;

-- =====================================================
-- 3. First query after CREATE works
-- =====================================================
SELECT '--- Test 3: First query after CREATE';
SELECT count() FROM t WHERE ts = '2025-10-15';
SELECT count() FROM t WHERE ts = '2025-08-01';

-- =====================================================
-- 4. Move the watermark
-- =====================================================
SELECT '--- Test 4: ALTER watermark';
ALTER TABLE t MODIFY SETTING hybrid_watermark_hot = '2025-10-01';

-- =====================================================
-- 5. New queries use updated boundary
-- =====================================================
SELECT '--- Test 5: Query with updated boundary';
SELECT count() FROM t WHERE ts = '2025-10-15';

-- =====================================================
-- 6. SHOW CREATE TABLE reflects the update
-- =====================================================
SELECT '--- Test 6: SHOW CREATE after ALTER';
SHOW CREATE TABLE t;

-- =====================================================
-- 7. Restart persistence (DETACH / ATTACH)
-- =====================================================
SELECT '--- Test 7: DETACH/ATTACH persistence';
DETACH TABLE t;
ATTACH TABLE t;
SHOW CREATE TABLE t;

-- =====================================================
-- 8. Three segments, two independent watermarks
-- =====================================================
SELECT '--- Test 8: Three segments, two watermarks';
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

ALTER TABLE t3 MODIFY SETTING hybrid_watermark_cold = '2025-07-01';
SHOW CREATE TABLE t3;

ALTER TABLE t3 MODIFY SETTING
    hybrid_watermark_hot  = '2025-11-01',
    hybrid_watermark_cold = '2025-08-01';

-- =====================================================
-- 9. Non-watermark parameter name is rejected at CREATE
-- =====================================================
SELECT '--- Test 9: Reject non-watermark parameter';
CREATE TABLE t_bad_param
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('foo', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('foo', 'DateTime')
)
SETTINGS foo = '2025-09-01'
AS local_hot; -- { serverError BAD_ARGUMENTS }

-- =====================================================
-- 10. Missing watermark SETTINGS — CREATE fails
-- =====================================================
SELECT '--- Test 10: Missing watermark SETTINGS rejected at CREATE';
CREATE TABLE t_missing
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_nope', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_nope', 'DateTime')
)
AS local_hot; -- { serverError BAD_ARGUMENTS }

-- =====================================================
-- 11. Invalid typed value — rejected at ALTER time
-- =====================================================
SELECT '--- Test 11: Invalid typed value';
ALTER TABLE t MODIFY SETTING hybrid_watermark_hot = 'not-a-date'; -- { serverError BAD_ARGUMENTS }

-- =====================================================
-- 12. Non-watermark MODIFY SETTING is rejected on Hybrid
-- =====================================================
SELECT '--- Test 12: Reject non-watermark MODIFY SETTING';
ALTER TABLE t MODIFY SETTING bytes_to_delay_insert = 100; -- { serverError BAD_ARGUMENTS }

-- =====================================================
-- 13. RESET SETTING is rejected on Hybrid
-- =====================================================
SELECT '--- Test 13: Reject RESET SETTING on Hybrid';
ALTER TABLE t RESET SETTING hybrid_watermark_hot; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t3 RESET SETTING bytes_to_delay_insert; -- { serverError BAD_ARGUMENTS }

-- =====================================================
-- 14. Alter one watermark preserves the other (metadata seeding)
-- =====================================================
SELECT '--- Test 14: Alter one preserves the other';
ALTER TABLE t3 MODIFY SETTING hybrid_watermark_hot = '2025-12-01';
SHOW CREATE TABLE t3;
SELECT count() FROM t3 WHERE ts = '2025-06-15';

-- =====================================================
-- 15. DistributedSettings are not accepted on Hybrid at CREATE
-- =====================================================
SELECT '--- Test 15: Reject DistributedSettings at CREATE';
CREATE TABLE t_dist_setting
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
)
SETTINGS
    hybrid_watermark_hot  = '2025-09-01',
    bytes_to_delay_insert = 100
AS local_hot; -- { serverError BAD_ARGUMENTS }

-- =====================================================
-- 16. Plain Distributed table is unaffected
-- =====================================================
SELECT '--- Test 16: Plain Distributed unaffected';
CREATE TABLE dist AS remote('localhost:9000', currentDatabase(), 'local_hot');
ALTER TABLE dist MODIFY SETTING hybrid_watermark_hot = '2025-10-01'; -- { serverError NOT_IMPLEMENTED }

-- =====================================================
-- 17. Value provided via SETTINGS
-- =====================================================
SELECT '--- Test 17: Value via SETTINGS';
CREATE TABLE t_settings_only
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
)
SETTINGS hybrid_watermark_hot = '2025-09-01'
AS local_hot;

SELECT count() FROM t_settings_only WHERE ts = '2025-10-15';
SELECT count() FROM t_settings_only WHERE ts = '2025-08-01';
SHOW CREATE TABLE t_settings_only;

-- =====================================================
-- 18. Conflicting types for same watermark name rejected
-- =====================================================
SELECT '--- Test 18: Conflicting types rejected';
CREATE TABLE t_conflict
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_hot', 'UInt64')
)
SETTINGS hybrid_watermark_hot = '2025-09-01'
AS local_hot; -- { serverError BAD_ARGUMENTS }

-- =====================================================
-- 19. Invalid SETTINGS value rejected at CREATE
-- =====================================================
SELECT '--- Test 19: Invalid SETTINGS value rejected at CREATE';
CREATE TABLE t_conflict
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
)
SETTINGS hybrid_watermark_hot = 'not-a-date'
AS local_hot; -- { serverError BAD_ARGUMENTS }

-- =====================================================
-- 20. Typo'd watermark name rejected at CREATE SETTINGS
-- =====================================================
SELECT '--- Test 20: Typo in CREATE SETTINGS rejected';
CREATE TABLE t_conflict
ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('localhost:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
)
SETTINGS hybrid_watermark_hott = '2025-09-01'
AS local_hot; -- { serverError BAD_ARGUMENTS }

-- =====================================================
-- 21. Typo'd watermark name rejected at ALTER
-- =====================================================
SELECT '--- Test 21: Typo in ALTER MODIFY SETTING rejected';
ALTER TABLE t MODIFY SETTING hybrid_watermark_hott = '2025-10-01'; -- { serverError BAD_ARGUMENTS }

-- =====================================================
-- Cleanup
-- =====================================================
DROP TABLE IF EXISTS t SYNC;
DROP TABLE IF EXISTS t3 SYNC;
DROP TABLE IF EXISTS t_bad_param SYNC;
DROP TABLE IF EXISTS t_missing SYNC;
DROP TABLE IF EXISTS t_dist_setting SYNC;
DROP TABLE IF EXISTS t_settings_only SYNC;
DROP TABLE IF EXISTS t_conflict SYNC;
DROP TABLE IF EXISTS dist SYNC;
DROP TABLE IF EXISTS local_hot SYNC;
DROP TABLE IF EXISTS local_cold SYNC;
DROP TABLE IF EXISTS local_warm SYNC;
