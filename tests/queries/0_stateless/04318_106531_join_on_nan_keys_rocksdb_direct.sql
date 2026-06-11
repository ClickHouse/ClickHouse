-- Tags: no-fasttest, use-rocksdb
-- Regression test for issue #106531 (companion to 04318_106531_join_on_nan_keys).
--
-- The generic `direct` (key-value) JOIN path runs `DirectKeyValueJoin::joinBlock`,
-- which forwards the probe key to `IKeyValueEntity::getByKeys`. For `EmbeddedRocksDB`
-- the key `Field` is serialized bitwise, so a probe `NaN` matched a build-side `NaN`
-- with the same bit pattern, unlike `hash` / `full_sorting_merge` which fold `NaN`
-- to a non-matching key. The fix masks `NaN` probe rows to the not-found path inside
-- `DirectKeyValueJoin::joinBlock` for the generic backends (those returning empty
-- offsets), so every join kind matches IEEE 754 / SQL `JOIN ON` `NaN != NaN`.
-- This file is split out from 04318 because `EmbeddedRocksDB` is not in the fast-test
-- build; the parent 04318 stays dependency-free.

DROP TABLE IF EXISTS rdb_nan;
CREATE TABLE rdb_nan (k Float64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO rdb_nan VALUES (1.0, 'one'), (nan, 'rocks_nan'), (2.0, 'two');

SET join_algorithm = 'direct';
SET enable_analyzer = 1;

SELECT '--- direct INNER: NaN probe must not match (only 1,2) ---';
SELECT l.x, r.v FROM (SELECT arrayJoin([toFloat64(1.0), nan, toFloat64(2.0)]) AS x) AS l
INNER JOIN rdb_nan AS r ON l.x = r.k ORDER BY l.x;

SELECT '--- direct LEFT: NaN probe -> default empty ---';
SELECT l.x, r.v FROM (SELECT arrayJoin([toFloat64(1.0), nan, toFloat64(2.0)]) AS x) AS l
LEFT JOIN rdb_nan AS r ON l.x = r.k ORDER BY l.x;

SELECT '--- direct LEFT join_use_nulls: NaN probe -> NULL ---';
SELECT l.x, r.v FROM (SELECT arrayJoin([toFloat64(1.0), nan, toFloat64(2.0)]) AS x) AS l
LEFT JOIN rdb_nan AS r ON l.x = r.k ORDER BY l.x
SETTINGS join_use_nulls = 1;

SELECT '--- direct LEFT SEMI: NaN probe not semi-matched ---';
SELECT l.x FROM (SELECT arrayJoin([toFloat64(1.0), nan, toFloat64(2.0)]) AS x) AS l
LEFT SEMI JOIN rdb_nan AS r ON l.x = r.k ORDER BY l.x;

SELECT '--- direct LEFT ANTI: NaN probe kept (no match) ---';
SELECT l.x FROM (SELECT arrayJoin([toFloat64(1.0), nan, toFloat64(2.0)]) AS x) AS l
LEFT ANTI JOIN rdb_nan AS r ON l.x = r.k ORDER BY l.x;

SELECT '--- direct INNER: non-NaN float keys still match (no regression) ---';
SELECT l.x, r.v FROM (SELECT arrayJoin([toFloat64(1.0), toFloat64(2.0)]) AS x) AS l
INNER JOIN rdb_nan AS r ON l.x = r.k ORDER BY l.x;

DROP TABLE rdb_nan;

-- Float32 keys take the same masking path.
DROP TABLE IF EXISTS rdb_nan_f32;
CREATE TABLE rdb_nan_f32 (k Float32, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO rdb_nan_f32 VALUES (1.0, 'f1'), (nan, 'fnan');

SELECT '--- direct INNER Float32: NaN probe must not match ---';
SELECT l.x, r.v FROM (SELECT arrayJoin([toFloat32(1.0), toFloat32(nan)]) AS x) AS l
INNER JOIN rdb_nan_f32 AS r ON l.x = r.k ORDER BY l.x;

DROP TABLE rdb_nan_f32;

-- A String key literally equal to 'nan' must still match: the masking only applies to
-- float payloads, never to the string 'nan'.
DROP TABLE IF EXISTS sdb_nan;
CREATE TABLE sdb_nan (k String, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO sdb_nan VALUES ('a', 'va'), ('nan', 'vstr'), ('b', 'vb');

SELECT '--- direct INNER String key "nan" still matches (not a float NaN) ---';
SELECT l.x, r.v FROM (SELECT arrayJoin(['a', 'nan', 'b']) AS x) AS l
INNER JOIN sdb_nan AS r ON l.x = r.k ORDER BY l.x;

DROP TABLE sdb_nan;

SET join_algorithm = 'default';
