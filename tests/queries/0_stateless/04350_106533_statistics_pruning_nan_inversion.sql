-- Tags: no-random-settings, no-random-merge-tree-settings
-- The auto minmax/basic statistics built at insert time (materialize_statistics_on_insert = 1)
-- compute their bounds with getExtremes, which skips NaN. A part mixing finite floats with a NaN
-- therefore stored a finite [min, max] that hid the NaN, and statistics-based part pruning wrongly
-- dropped that part under a negated float range (issue #106533 / #106948). This is a third layer of
-- the same NaN hazard, independent of the minmax skip index and KeyCondition inversion: it is reached
-- even with use_skip_indexes = 0. The fix records a has_nan flag in the statistics and widens the
-- pruning range over NaN, mirroring the skip index.

SET materialize_statistics_on_insert = 1;
SET allow_experimental_statistics = 1;

DROP TABLE IF EXISTS t_106533_stats;

-- `minmax` is requested through `auto_statistics_types` rather than a column `STATISTICS(minmax)`
-- clause: both build the same StatisticsMinMax, but an explicit clause is deprecated and logs a
-- warning that the Fast test harness treats as a failure (it runs with send_logs_level=warning).
CREATE TABLE t_106533_stats (id UInt64, val Float64)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'minmax', index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- The `Parts: 1/2` assertion below counts active parts, so a background merge collapsing the two
-- inserts would make it read `Parts: 1/1` and fail regardless of the fix.
SYSTEM STOP MERGES t_106533_stats;

-- Part 1 mixes a NaN with finite values; part 2 is entirely finite.
INSERT INTO t_106533_stats VALUES (1, 1.0), (2, nan), (3, 3.0);
INSERT INTO t_106533_stats VALUES (4, 100.0), (5, 150.0), (6, 200.0);

-- NOT (val BETWEEN 0 AND 3) keeps the NaN row (IEEE-754: NOT(NaN >= 0 AND NaN <= 3) = NOT(false) = true)
-- plus the three finite rows above 3. Expected 4. Before the fix, statistics pruning dropped part 1
-- (finite stored bounds [1, 3] fully inside [0, 3]), losing the NaN row -> 3.
SELECT count() FROM t_106533_stats WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT count() FROM t_106533_stats WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;

-- Statistics pruning must still fire when it is correct: a finite-only lookup keeps just part 1.
SELECT count() FROM t_106533_stats WHERE val < 10.;
SELECT countIf(explain LIKE '%Parts: 1/2%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_106533_stats WHERE val < 10. SETTINGS use_skip_indexes = 0);

DROP TABLE t_106533_stats;

-- Basic statistics carry the same has_nan flag over their numeric min/max.
DROP TABLE IF EXISTS t_106533_stats_basic;

CREATE TABLE t_106533_stats_basic (id UInt64, val Float32 STATISTICS(basic))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_106533_stats_basic VALUES (1, 1.0), (2, nan), (3, 3.0);
INSERT INTO t_106533_stats_basic VALUES (4, 100.0), (5, 150.0), (6, 200.0);

SELECT count() FROM t_106533_stats_basic WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_106533_stats_basic;

-- Auto statistics left at the default auto_statistics_types ('basic, uniq_v2'): materialize_statistics_on_insert = 1
-- builds them without any statistics clause on the column, so the same bug must not recur out of the box.
DROP TABLE IF EXISTS t_106533_stats_auto;

CREATE TABLE t_106533_stats_auto (id UInt64, val Float64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_106533_stats_auto VALUES (1, 1.0), (2, nan), (3, 3.0);
INSERT INTO t_106533_stats_auto VALUES (4, 100.0), (5, 150.0), (6, 200.0);

SELECT count() FROM t_106533_stats_auto WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_106533_stats_auto;
