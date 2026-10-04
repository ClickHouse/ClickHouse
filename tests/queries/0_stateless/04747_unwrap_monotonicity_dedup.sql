-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the pruning assertions read exact granule counts, which
-- `index_granularity` randomization moves.

-- Key analysis strips `LowCardinality` and then `Nullable` before it inspects a key type, in
-- `negate`, in the week transforms and in the date transforms. A wrong unwrap silently disables
-- pruning, or, for `negate` over a nullable key, returns the wrong rows. The `prunes` rows below
-- read exact granule counts because that is the only observable a lost monotonicity verdict moves;
-- the `answers` rows beside them are oracle comparisons that stay correct either way.
SET allow_suspicious_low_cardinality_types = 1;

-- `negate` over a `Nullable` key. `negate(NULL) = NULL` stays at the bottom of the sort order
-- instead of flipping to the top, so `negate` must not be reported monotonic when the range can
-- start at NULL. The unwrap must still recover that the ORIGINAL type was nullable.
-- `optimize_read_in_order` is pinned because the sort optimization it gates is the only observable
-- of that verdict, and the runner randomizes it. The pruning rows below likewise pin
-- `use_lightweight_primary_key_index_analysis`: at 0 the index analysis hands the transform a key
-- type that `recursiveRemoveLowCardinality` has already stripped, so a wrong unwrap of the
-- `LowCardinality` half is unobservable there. Both pins read the same values on
-- unmodified code, so they remove an escape hatch rather than weakening an assertion.
DROP TABLE IF EXISTS t_neg_null_04747;
DROP TABLE IF EXISTS t_mem_null_04747;
CREATE TABLE t_neg_null_04747 (a Nullable(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE t_mem_null_04747 (a Nullable(Int64)) ENGINE = Log;
INSERT INTO t_neg_null_04747 SELECT if(number % 10 = 0, NULL, number * 100) FROM numbers(100);
INSERT INTO t_mem_null_04747 SELECT if(number % 10 = 0, NULL, number * 100) FROM numbers(100);
SELECT 'negate Nullable ORDER BY', (SELECT arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT negate(a) AS x FROM t_neg_null_04747 ORDER BY negate(a) ASC LIMIT 5)) AS keyed, (SELECT arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT negate(a) AS x FROM t_mem_null_04747 ORDER BY negate(a) ASC LIMIT 5)) AS oracle SETTINGS optimize_read_in_order = 1;

-- The same shape under `LowCardinality(Nullable(...))`: the nullability sits inside the dictionary,
-- so recovering it requires looking through the LowCardinality wrapper too.
DROP TABLE IF EXISTS t_neg_lcn_04747;
DROP TABLE IF EXISTS t_mem_lcn_04747;
CREATE TABLE t_neg_lcn_04747 (a LowCardinality(Nullable(Int64))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE t_mem_lcn_04747 (a LowCardinality(Nullable(Int64))) ENGINE = Log;
INSERT INTO t_neg_lcn_04747 SELECT if(number % 10 = 0, NULL, number * 100) FROM numbers(100);
INSERT INTO t_mem_lcn_04747 SELECT if(number % 10 = 0, NULL, number * 100) FROM numbers(100);
SELECT 'negate LC(Nullable) ORDER BY', (SELECT arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT negate(a) AS x FROM t_neg_lcn_04747 ORDER BY negate(a) ASC LIMIT 5)) AS keyed, (SELECT arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT negate(a) AS x FROM t_mem_lcn_04747 ORDER BY negate(a) ASC LIMIT 5)) AS oracle SETTINGS optimize_read_in_order = 1;

-- Control: `LowCardinality` delegates `isValueRepresentedByNumber` to its dictionary, so this row
-- holds whether or not the unwrap happens. It guards against over-declining, not against a wrong unwrap.
DROP TABLE IF EXISTS t_neg_lc_04747;
DROP TABLE IF EXISTS t_mem_lc_04747;
CREATE TABLE t_neg_lc_04747 (a LowCardinality(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_lc_04747 (a LowCardinality(Int64)) ENGINE = Log;
INSERT INTO t_neg_lc_04747 SELECT number * 100 FROM numbers(100);
INSERT INTO t_mem_lc_04747 SELECT number * 100 FROM numbers(100);
SELECT 'negate LC still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_neg_lc_04747 WHERE negate(a) = toInt64(-5000)) WHERE explain ILIKE '%Granules: 2/100%' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT 'negate LC answers', (SELECT count() FROM t_neg_lc_04747 WHERE negate(a) = toInt64(-5000)) AS keyed, (SELECT count() FROM t_mem_lc_04747 WHERE negate(a) = toInt64(-5000)) AS oracle;

-- Week transforms. `toDayOfWeek` has a non-trivial factor transform, so its verdict depends on the
-- unwrapped key type being recognised as `Date`; a wrong unwrap loses the pruning below.
DROP TABLE IF EXISTS t_week_lc_04747;
DROP TABLE IF EXISTS t_week_mem_04747;
CREATE TABLE t_week_lc_04747 (a LowCardinality(Date)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_week_mem_04747 (a LowCardinality(Date)) ENGINE = Log;
INSERT INTO t_week_lc_04747 SELECT toDate('2020-01-01') + number FROM numbers(100);
INSERT INTO t_week_mem_04747 SELECT toDate('2020-01-01') + number FROM numbers(100);
SELECT 'toDayOfWeek LC(Date) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_week_lc_04747 WHERE toDayOfWeek(a) >= 3) WHERE explain ILIKE '%Granules: 86/100%' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT 'toDayOfWeek LC(Date) answers', (SELECT count() FROM t_week_lc_04747 WHERE toDayOfWeek(a) >= 3) AS keyed, (SELECT count() FROM t_week_mem_04747 WHERE toDayOfWeek(a) >= 3) AS oracle;

DROP TABLE IF EXISTS t_week_lcn_04747;
DROP TABLE IF EXISTS t_week_memn_04747;
CREATE TABLE t_week_lcn_04747 (a LowCardinality(Nullable(Date))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE t_week_memn_04747 (a LowCardinality(Nullable(Date))) ENGINE = Log;
INSERT INTO t_week_lcn_04747 SELECT toDate('2020-01-01') + number FROM numbers(100);
INSERT INTO t_week_memn_04747 SELECT toDate('2020-01-01') + number FROM numbers(100);
SELECT 'toDayOfWeek LC(Nullable(Date)) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_week_lcn_04747 WHERE toDayOfWeek(a) >= 3) WHERE explain ILIKE '%Granules: 86/100%' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT 'toDayOfWeek LC(Nullable(Date)) answers', (SELECT count() FROM t_week_lcn_04747 WHERE toDayOfWeek(a) >= 3) AS keyed, (SELECT count() FROM t_week_memn_04747 WHERE toDayOfWeek(a) >= 3) AS oracle;

-- Date transforms. `toMonth`'s factor transform likewise needs the unwrapped `Date`.
SELECT 'toMonth LC(Date) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_week_lc_04747 WHERE toMonth(a) = 2) WHERE explain ILIKE '%Granules: 30/100%' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT 'toMonth LC(Date) answers', (SELECT count() FROM t_week_lc_04747 WHERE toMonth(a) = 2) AS keyed, (SELECT count() FROM t_week_mem_04747 WHERE toMonth(a) = 2) AS oracle;
SELECT 'toMonth LC(Nullable(Date)) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_week_lcn_04747 WHERE toMonth(a) = 2) WHERE explain ILIKE '%Granules: 30/100%' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT 'toMonth LC(Nullable(Date)) answers', (SELECT count() FROM t_week_lcn_04747 WHERE toMonth(a) = 2) AS keyed, (SELECT count() FROM t_week_memn_04747 WHERE toMonth(a) = 2) AS oracle;
-- Control: `toStartOfMonth` has a `ZeroTransform` factor, so it answers before the unwrap runs and
-- is insensitive to it. It guards the always-monotonic shortcut, not the unwrap.
SELECT 'toStartOfMonth LC(Date) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_week_lc_04747 WHERE toStartOfMonth(a) = toDate('2020-02-01')) WHERE explain ILIKE '%Granules: 30/100%' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT 'toStartOfMonth LC(Date) answers', (SELECT count() FROM t_week_lc_04747 WHERE toStartOfMonth(a) = toDate('2020-02-01')) AS keyed, (SELECT count() FROM t_week_mem_04747 WHERE toStartOfMonth(a) = toDate('2020-02-01')) AS oracle;

-- The compound site the helper was extracted from keeps its own coverage in
-- `04652_compound_key_monotonicity.sql`, which already pins `Nullable(Tuple(Int64, Int64))`.

DROP TABLE IF EXISTS t_neg_null_04747;
DROP TABLE IF EXISTS t_mem_null_04747;
DROP TABLE IF EXISTS t_neg_lcn_04747;
DROP TABLE IF EXISTS t_mem_lcn_04747;
DROP TABLE IF EXISTS t_neg_lc_04747;
DROP TABLE IF EXISTS t_mem_lc_04747;
DROP TABLE IF EXISTS t_week_lc_04747;
DROP TABLE IF EXISTS t_week_mem_04747;
DROP TABLE IF EXISTS t_week_lcn_04747;
DROP TABLE IF EXISTS t_week_memn_04747;
