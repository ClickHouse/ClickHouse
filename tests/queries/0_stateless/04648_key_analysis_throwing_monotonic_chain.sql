-- Tags: no-random-merge-tree-settings

-- Key analysis speculatively applies a monotonic function chain over the whole in-memory index column, so a value that
-- no analysed range contains can make the chain throw. Every query below is valid: the `ENGINE = Memory` table beside
-- each MergeTree one is the oracle, and both counts must agree.

SET allow_suspicious_low_cardinality_types = 1;

-- `query_plan_merge_filters = 0` makes an unrelated pre-existing defect throw at row-evaluation time (it reproduces
-- identically on a table with no key at all, and identically before this fix), which would mask the analysis-time
-- behaviour this test is about. Pinned at its default; the CI runner randomizes it.
SET query_plan_merge_filters = 1;

DROP TABLE IF EXISTS t_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_part_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_part_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_multikey_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_multikey_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_zero_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_zero_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_dt_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_dt_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_lc_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_lc_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_wide_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_wide_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_lcwide_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_lcwide_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_gran_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_gran_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_nonmono_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_nonmono_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_prune_04648 SETTINGS ignore_drop_queries_probability = 0;

CREATE TABLE t_mt_04648 (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_04648 (a Int8) ENGINE = Memory;
INSERT INTO t_mt_04648 VALUES (-128), (-100), (-50), (50);
INSERT INTO t_mem_04648 VALUES (-128), (-100), (-50), (50);

SELECT 'intDiv by an effective -1, minimum excluded by the predicate';
SELECT
    (SELECT count() FROM t_mt_04648 WHERE a > -50 AND intDiv(a, toInt8(-1)) = 0) AS mergetree,
    (SELECT count() FROM t_mem_04648 WHERE a > -50 AND intDiv(a, toInt8(-1)) = 0) AS oracle;

SELECT 'same predicate spelled with IN, so the set index applies the chain';
SELECT
    (SELECT count() FROM t_mt_04648 WHERE a > -50 AND intDiv(a, toInt8(-1)) IN (0)) AS mergetree,
    (SELECT count() FROM t_mem_04648 WHERE a > -50 AND intDiv(a, toInt8(-1)) IN (0)) AS oracle;

CREATE TABLE t_part_mt_04648 (a Int8) ENGINE = MergeTree PARTITION BY a ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_part_mem_04648 (a Int8) ENGINE = Memory;
INSERT INTO t_part_mt_04648 VALUES (-128), (5), (60);
INSERT INTO t_part_mem_04648 VALUES (-128), (5), (60);

SELECT 'partition pruning, where the chain is applied to a single point';
SELECT
    (SELECT count() FROM t_part_mt_04648 WHERE a > 0 AND intDiv(a, toInt8(-1)) = -5) AS mergetree,
    (SELECT count() FROM t_part_mem_04648 WHERE a > 0 AND intDiv(a, toInt8(-1)) = -5) AS oracle;

CREATE TABLE t_multikey_mt_04648 (k UInt8, a Int8) ENGINE = MergeTree ORDER BY (k, a) SETTINGS index_granularity = 1;
CREATE TABLE t_multikey_mem_04648 (k UInt8, a Int8) ENGINE = Memory;
INSERT INTO t_multikey_mt_04648 VALUES (1, -128), (1, -127), (1, -126), (2, 50), (2, 51), (2, 52);
INSERT INTO t_multikey_mem_04648 VALUES (1, -128), (1, -127), (1, -126), (2, 50), (2, 51), (2, 52);

SELECT 'offending granule excluded by a preceding key column';
SELECT
    (SELECT count() FROM t_multikey_mt_04648 WHERE k = 2 AND intDiv(a, toInt8(-1)) = -50) AS mergetree,
    (SELECT count() FROM t_multikey_mem_04648 WHERE k = 2 AND intDiv(a, toInt8(-1)) = -50) AS oracle;

CREATE TABLE t_zero_mt_04648 (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_zero_mem_04648 (a Int8) ENGINE = Memory;
INSERT INTO t_zero_mt_04648 VALUES (0), (1), (2), (50);
INSERT INTO t_zero_mem_04648 VALUES (0), (1), (2), (50);

SELECT 'division by zero, so the defect is not specific to the signed minimum';
SELECT
    (SELECT count() FROM t_zero_mt_04648 WHERE a > 2 AND intDiv(100, a) = 0) AS mergetree,
    (SELECT count() FROM t_zero_mem_04648 WHERE a > 2 AND intDiv(100, a) = 0) AS oracle;

CREATE TABLE t_dt_mt_04648 (a DateTime64(3)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_dt_mem_04648 (a DateTime64(3)) ENGINE = Memory;
INSERT INTO t_dt_mt_04648 VALUES ('2262-04-12 00:00:00'), ('2020-01-01 00:00:00'), ('2020-01-02 00:00:00'), ('2020-01-03 00:00:00'), ('2020-01-04 00:00:00'), ('2020-01-05 00:00:00'), ('2020-01-06 00:00:00'), ('2020-01-07 00:00:00');
INSERT INTO t_dt_mem_04648 VALUES ('2262-04-12 00:00:00'), ('2020-01-01 00:00:00'), ('2020-01-02 00:00:00'), ('2020-01-03 00:00:00'), ('2020-01-04 00:00:00'), ('2020-01-05 00:00:00'), ('2020-01-06 00:00:00'), ('2020-01-07 00:00:00');

-- `toUnixTimestamp64Nano` reports `is_always_monotonic` correctly and still throws `DECIMAL_OVERFLOW` near the
-- `DateTime64` maximum, so this case proves the fix is not about `intDiv` nor about inaccurate monotonicity.
SELECT 'DECIMAL_OVERFLOW from a correctly always-monotonic function';
SELECT
    (SELECT count() FROM t_dt_mt_04648 WHERE a < toDateTime64('2020-01-06 00:00:00', 3) AND toUnixTimestamp64Nano(a) = 1578268800000000000) AS mergetree,
    (SELECT count() FROM t_dt_mem_04648 WHERE a < toDateTime64('2020-01-06 00:00:00', 3) AND toUnixTimestamp64Nano(a) = 1578268800000000000) AS oracle;

CREATE TABLE t_lc_mt_04648 (a LowCardinality(Int8)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_lc_mem_04648 (a LowCardinality(Int8)) ENGINE = Memory;
INSERT INTO t_lc_mt_04648 VALUES (-128), (-100), (-50), (50);
INSERT INTO t_lc_mem_04648 VALUES (-128), (-100), (-50), (50);

SELECT 'LowCardinality key column';
SELECT
    (SELECT count() FROM t_lc_mt_04648 WHERE a > -50 AND intDiv(a, toInt8(-1)) = 0) AS mergetree,
    (SELECT count() FROM t_lc_mem_04648 WHERE a > -50 AND intDiv(a, toInt8(-1)) = 0) AS oracle;

-- The cases below keep more than two marks in the surviving range, so key analysis reaches the exact-range machinery.
-- That machinery is promised an exact continuous range by `matchesExactContinuousRange`, which only asks whether the
-- chain is always monotonic - never whether it could be applied.

CREATE TABLE t_wide_mt_04648 (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_wide_mem_04648 (a Int8) ENGINE = Memory;
INSERT INTO t_wide_mt_04648 VALUES (-128), (-127), (-100), (-60), (-50), (-40), (50), (60);
INSERT INTO t_wide_mem_04648 VALUES (-128), (-127), (-100), (-60), (-50), (-40), (50), (60);

SELECT 'exact-range candidate spanning several marks';
SELECT
    (SELECT count() FROM t_wide_mt_04648 WHERE a > -60 AND intDiv(a, toInt8(-1)) = -50) AS mergetree,
    (SELECT count() FROM t_wide_mem_04648 WHERE a > -60 AND intDiv(a, toInt8(-1)) = -50) AS oracle;

SELECT 'the same via the set index';
SELECT
    (SELECT count() FROM t_wide_mt_04648 WHERE a > -60 AND intDiv(a, toInt8(-1)) IN (-50)) AS mergetree,
    (SELECT count() FROM t_wide_mem_04648 WHERE a > -60 AND intDiv(a, toInt8(-1)) IN (-50)) AS oracle;

-- A set with more than one element relaxes the condition, so this takes the generic exclusion search instead of the
-- binary search. It must stay unaffected.
SELECT 'generic exclusion search instead of binary search';
SELECT
    (SELECT count() FROM t_wide_mt_04648 WHERE a > -60 AND intDiv(a, toInt8(-1)) IN (-50, -40)) AS mergetree,
    (SELECT count() FROM t_wide_mem_04648 WHERE a > -60 AND intDiv(a, toInt8(-1)) IN (-50, -40)) AS oracle;

-- The chain is applied repeatedly for one part: the binary search probes many mark ranges. Everything after the first
-- failure must see the cached failure rather than a half-built cache entry.
SELECT 'repeated probes over one part after the first failure';
SELECT
    (SELECT count() FROM t_wide_mt_04648 WHERE a > -128 AND intDiv(a, toInt8(-1)) < 100) AS mergetree,
    (SELECT count() FROM t_wide_mem_04648 WHERE a > -128 AND intDiv(a, toInt8(-1)) < 100) AS oracle;

CREATE TABLE t_lcwide_mt_04648 (a LowCardinality(Int8)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_lcwide_mem_04648 (a LowCardinality(Int8)) ENGINE = Memory;
INSERT INTO t_lcwide_mt_04648 VALUES (-128), (-127), (-100), (-60), (-50), (-40), (50), (60);
INSERT INTO t_lcwide_mem_04648 VALUES (-128), (-127), (-100), (-60), (-50), (-40), (50), (60);

SELECT 'exact-range candidate on a LowCardinality key column';
SELECT
    (SELECT count() FROM t_lcwide_mt_04648 WHERE a > -60 AND intDiv(a, toInt8(-1)) = -50) AS mergetree,
    (SELECT count() FROM t_lcwide_mem_04648 WHERE a > -60 AND intDiv(a, toInt8(-1)) = -50) AS oracle;

-- No exotic granularity: the default one, with enough rows to span the signed minimum.
CREATE TABLE t_gran_mt_04648 (a Int16) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_gran_mem_04648 (a Int16) ENGINE = Memory;
INSERT INTO t_gran_mt_04648 SELECT -32768 + number FROM numbers(40000);
INSERT INTO t_gran_mem_04648 SELECT -32768 + number FROM numbers(40000);

SELECT 'default index granularity';
SELECT
    (SELECT count() FROM t_gran_mt_04648 WHERE a > -30000 AND intDiv(a, toInt16(-1)) = -50) AS mergetree,
    (SELECT count() FROM t_gran_mem_04648 WHERE a > -30000 AND intDiv(a, toInt16(-1)) = -50) AS oracle;

-- Negative controls.

-- An honestly non-monotonic chain must keep behaving exactly as before: it is not an application failure, so it must
-- not switch off the exact-range consistency check.
CREATE TABLE t_nonmono_mt_04648 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_nonmono_mem_04648 (a UInt64) ENGINE = Memory;
INSERT INTO t_nonmono_mt_04648 VALUES (1), (9223372036854775807), (9223372036854775808), (18446744073709551615);
INSERT INTO t_nonmono_mem_04648 VALUES (1), (9223372036854775807), (9223372036854775808), (18446744073709551615);

SELECT 'an honestly non-monotonic chain is not an application failure';
SELECT
    (SELECT count() FROM t_nonmono_mt_04648 WHERE intDiv(a, toInt64(3)) = 0) AS mergetree,
    (SELECT count() FROM t_nonmono_mem_04648 WHERE intDiv(a, toInt64(3)) = 0) AS oracle;

-- A query whose selected rows really do divide by zero must still fail.
SELECT 'rows that really divide by zero still throw';
SELECT count() FROM t_zero_mt_04648 WHERE intDiv(100, a) = 0; -- { serverError ILLEGAL_DIVISION }

-- A healthy chain must keep pruning granules, and must keep producing an exact range for the count optimization.
CREATE TABLE t_prune_04648 (a UInt8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_prune_04648 SELECT number FROM numbers(100);

SELECT 'a healthy chain still prunes granules';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_prune_04648 WHERE intDiv(a, 10) = 5) WHERE explain ILIKE '%Granules: 11/100%';

-- `optimize_use_projections` and `optimize_use_implicit_projections` gate the exact-count optimization itself, so pin
-- them at their defaults for this assertion only; the CI runner randomizes both.
SELECT 'a healthy range still yields an exact row count';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1, projections = 1 SELECT count() FROM t_prune_04648 WHERE a > 10 AND a < 90
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1
) WHERE explain ILIKE '%Exact count optimization is applied%';

DROP TABLE t_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_part_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_part_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_multikey_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_multikey_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_zero_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_zero_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_dt_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_dt_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_lc_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_lc_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_wide_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_wide_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_lcwide_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_lcwide_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_gran_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_gran_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_nonmono_mt_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_nonmono_mem_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_prune_04648 SETTINGS ignore_drop_queries_probability = 0;
