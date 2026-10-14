-- The auto minmax/basic statistics built at insert time (materialize_statistics_on_insert = 1) aggregate
-- their bounds the same way getExtremes does, so a part mixing finite floats with a NaN stores a finite
-- [min, max] that hides it. This is a third pruning layer with the same hazard, and it is reached even
-- with use_skip_indexes = 0.

SET materialize_statistics_on_insert = 1;
SET allow_experimental_statistics = 1;

-- The statistics pruner is the layer under test, so its two gates are pinned rather than randomized.
SET use_statistics = 1;
SET use_statistics_for_part_pruning = 1;

-- The cache is keyed on the condition and not on the settings, so without this the second arm of each
-- pair would reuse the first arm's part verdict and stop being an independent oracle.
SET use_query_condition_cache = 0;

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

-- NOT (val BETWEEN 0 AND 3) keeps the NaN row, because NaN >= 0 is false, plus the three finite rows
-- above 3. Expected 4.
SELECT count() FROM t_106533_stats WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT count() FROM t_106533_stats WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;

-- Statistics pruning must still fire when it is correct: a finite-only lookup keeps just part 1.
SELECT count() FROM t_106533_stats WHERE val < 10.;
SELECT countIf(explain LIKE '%Parts: 1/2%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_106533_stats WHERE val < 10. SETTINGS use_skip_indexes = 0);

-- The negated arms above never reach the range analysis at all: a float column under a negation is
-- dropped from the statistics map before any of it is built, so each implementation below is also
-- queried with a non-negated shape that leaves `val` prunable and still matches the NaN row.
-- `sign(-val)` is a chain of two monotonic functions, so the analysis maps part 1's stored [1, 3] to
-- sign([-3, -1]) = [-1, -1] and reads `= 1` as definitely false, while row-wise `sign(-nan)` is `sign(nan)`,
-- which is 1 and matches. A positive `IN` holding a NaN is not the shape to use here: a set whose
-- elements contain a NaN is already declined outright in `tryPrepareSetIndexForIn`, so it never reaches
-- this relaxation. Expected 1.
SELECT count() FROM t_106533_stats WHERE sign(-val) = 1 SETTINGS use_skip_indexes = 0;
-- A NaN-free lookup must still prune, or the assertion above could hold merely by the pruner never
-- pruning anything.
SELECT countIf(explain LIKE '%Parts: 1/2%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_106533_stats WHERE val IN (150.) SETTINGS use_skip_indexes = 0);

DROP TABLE t_106533_stats;

-- Basic statistics aggregate a numeric min/max the same way, so they hide a NaN the same way.
DROP TABLE IF EXISTS t_106533_stats_basic;

CREATE TABLE t_106533_stats_basic (id UInt64, val Float32 STATISTICS(basic))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Two parts are what make the pruning unsafe: a merged part's finite bound is kept by the unfixed code too.
SYSTEM STOP MERGES t_106533_stats_basic;

INSERT INTO t_106533_stats_basic VALUES (1, 1.0), (2, nan), (3, 3.0);
INSERT INTO t_106533_stats_basic VALUES (4, 100.0), (5, 150.0), (6, 200.0);

SELECT count() FROM t_106533_stats_basic WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_106533_stats_basic WHERE sign(-val) = 1 SETTINGS use_skip_indexes = 0;
SELECT countIf(explain LIKE '%Parts: 1/2%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_106533_stats_basic WHERE val IN (150.) SETTINGS use_skip_indexes = 0);

DROP TABLE t_106533_stats_basic;

-- Auto statistics with auto_statistics_types pinned to its default ('basic, uniq_v2') so randomization
-- cannot remove it: built with no statistics clause on the column, the bug must not recur out of the box.
DROP TABLE IF EXISTS t_106533_stats_auto;

CREATE TABLE t_106533_stats_auto (id UInt64, val Float64)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic, uniq_v2', index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_106533_stats_auto;

INSERT INTO t_106533_stats_auto VALUES (1, 1.0), (2, nan), (3, 3.0);
INSERT INTO t_106533_stats_auto VALUES (4, 100.0), (5, 150.0), (6, 200.0);

SELECT count() FROM t_106533_stats_auto WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_106533_stats_auto WHERE sign(-val) = 1 SETTINGS use_skip_indexes = 0;
SELECT countIf(explain LIKE '%Parts: 1/2%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_106533_stats_auto WHERE val IN (150.) SETTINGS use_skip_indexes = 0);

DROP TABLE t_106533_stats_auto;
