-- Verify that session_timezone is respected for bare string literals in
-- WHERE, BETWEEN, and IN expressions against DateTime/DateTime64 columns
-- without explicit timezone. PR #100647 fixed doGetSerialization to call
-- DateLUT::instance() which respects session_timezone; convertFieldToType
-- uses getDefaultSerialization, so all implicit string coercion should work.
--
-- Server timezone: UTC
-- Session timezone: America/Denver (UTC-7 in winter)
-- Reference: '2000-01-01 10:00:00' parsed as Denver = epoch 946746000

SET session_timezone = 'America/Denver';

-- ============================================================================
-- Section A: WHERE with bare string literals (DateTime)
-- ============================================================================

DROP TABLE IF EXISTS tz_where SYNC;
CREATE TABLE tz_where (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO tz_where VALUES (1, '2000-01-01 08:00:00'), (2, '2000-01-01 10:00:00'), (3, '2000-01-01 12:00:00');
-- Epochs (Denver): id=1 → 946738800, id=2 → 946746000, id=3 → 946753200

SELECT 'A1: eq bare string', id FROM tz_where WHERE time = '2000-01-01 10:00:00' ORDER BY id;
SELECT 'A2: gte bare string', id FROM tz_where WHERE time >= '2000-01-01 10:00:00' ORDER BY id;
SELECT 'A3: lt bare string', id FROM tz_where WHERE time < '2000-01-01 10:00:00' ORDER BY id;

-- Verify toDateTime gives the same result (consistency check)
SELECT 'A4: eq toDateTime', id FROM tz_where WHERE time = toDateTime('2000-01-01 10:00:00') ORDER BY id;
SELECT 'A5: gte toDateTime', id FROM tz_where WHERE time >= toDateTime('2000-01-01 10:00:00') ORDER BY id;

-- ============================================================================
-- Section B: BETWEEN with bare string literals
-- ============================================================================

SELECT 'B1: between', id FROM tz_where WHERE time BETWEEN '2000-01-01 09:00:00' AND '2000-01-01 11:00:00' ORDER BY id;
SELECT 'B2: between wide', id FROM tz_where WHERE time BETWEEN '2000-01-01 08:00:00' AND '2000-01-01 10:00:00' ORDER BY id;

-- ============================================================================
-- Section C: IN with bare string literals
-- ============================================================================

SELECT 'C1: in single', id FROM tz_where WHERE time IN ('2000-01-01 10:00:00') ORDER BY id;
SELECT 'C2: in multi', id FROM tz_where WHERE time IN ('2000-01-01 08:00:00', '2000-01-01 12:00:00') ORDER BY id;

-- ============================================================================
-- Section D: DateTime64 — same tests
-- ============================================================================

DROP TABLE IF EXISTS tz_where64 SYNC;
CREATE TABLE tz_where64 (id UInt32, time DateTime64(3)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tz_where64 VALUES (1, '2000-01-01 08:00:00.000'), (2, '2000-01-01 10:00:00.000'), (3, '2000-01-01 12:00:00.000');

SELECT 'D1: dt64 eq', id FROM tz_where64 WHERE time = '2000-01-01 10:00:00' ORDER BY id;
SELECT 'D2: dt64 gte', id FROM tz_where64 WHERE time >= '2000-01-01 10:00:00' ORDER BY id;
SELECT 'D3: dt64 between', id FROM tz_where64 WHERE time BETWEEN '2000-01-01 09:00:00' AND '2000-01-01 11:00:00' ORDER BY id;
SELECT 'D4: dt64 in', id FROM tz_where64 WHERE time IN ('2000-01-01 08:00:00', '2000-01-01 12:00:00') ORDER BY id;

-- ============================================================================
-- Section E: Round-trip — INSERT string, SELECT back, verify same string
-- ============================================================================

DROP TABLE IF EXISTS tz_roundtrip SYNC;
CREATE TABLE tz_roundtrip (time DateTime) ENGINE = MergeTree ORDER BY time;
INSERT INTO tz_roundtrip VALUES ('2000-01-01 10:00:00');
SELECT 'E1: roundtrip', time FROM tz_roundtrip;

DROP TABLE IF EXISTS tz_roundtrip64 SYNC;
CREATE TABLE tz_roundtrip64 (time DateTime64(3)) ENGINE = MergeTree ORDER BY time;
INSERT INTO tz_roundtrip64 VALUES ('2000-01-01 10:00:00.123');
SELECT 'E2: roundtrip dt64', time FROM tz_roundtrip64;

-- ============================================================================
-- Section F: Explicit-tz column — session_tz should NOT override column tz
-- ============================================================================

DROP TABLE IF EXISTS tz_explicit SYNC;
CREATE TABLE tz_explicit (id UInt32, time DateTime('UTC')) ENGINE = MergeTree ORDER BY id;
-- session_tz is Denver but column is explicit UTC, so '10:00' should parse as UTC
INSERT INTO tz_explicit VALUES (1, '2000-01-01 10:00:00');
SELECT 'F1: explicit tz epoch', toUnixTimestamp(time) FROM tz_explicit;
-- 946720800 = 10:00 UTC (NOT Denver)

-- WHERE on explicit-tz column: bare string should use column tz (UTC)
SELECT 'F2: explicit where', count() FROM tz_explicit WHERE time = '2000-01-01 10:00:00';

-- ============================================================================
-- Section G: Partition pruning — verify index is used with session_tz
-- ============================================================================

DROP TABLE IF EXISTS tz_prune SYNC;
CREATE TABLE tz_prune (time DateTime, val UInt32) ENGINE = MergeTree ORDER BY time;
INSERT INTO tz_prune VALUES ('2000-01-01 10:00:00', 1);
INSERT INTO tz_prune VALUES ('2000-01-02 10:00:00', 2);

SELECT 'G1: prune result', val FROM tz_prune WHERE time = '2000-01-01 10:00:00';

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE tz_where SYNC;
DROP TABLE tz_where64 SYNC;
DROP TABLE tz_roundtrip SYNC;
DROP TABLE tz_roundtrip64 SYNC;
DROP TABLE tz_explicit SYNC;
DROP TABLE tz_prune SYNC;
