-- Tests that granule pruning stays correct when a narrowing integer cast sits in the index condition's
-- monotonic function chain, i.e. when the index expression is wider than the column the query names.

DROP TABLE IF EXISTS t_unsigned;
DROP TABLE IF EXISTS t_signed;
DROP TABLE IF EXISTS t_partition;
DROP TABLE IF EXISTS t_widening;
DROP TABLE IF EXISTS t_chain;
DROP TABLE IF EXISTS t_bool_narrowing;
DROP TABLE IF EXISTS t_bool_same_size;

-- The index expression is `UInt64` while the queried alias is `UInt16`, so `_CAST(w, 'UInt16')` is in the
-- chain. Rows 65530..65541 do not fit `UInt16`, so the cast changes their values and must be applied.
CREATE TABLE t_unsigned (w UInt64, m UInt16 ALIAS w, INDEX i_m (w) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_unsigned SELECT number FROM numbers(64);
INSERT INTO t_unsigned SELECT 65530 + number FROM numbers(12);

SELECT sum(w) FROM t_unsigned WHERE m >= 10 AND m <= 20;
SELECT sum(w) FROM t_unsigned WHERE m <= 5;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(w) FROM t_unsigned WHERE m >= 10 AND m <= 20) WHERE explain ILIKE '%Granules: 3/10%';

CREATE TABLE t_signed (w Int64, m Int16 ALIAS w, INDEX i_m (w) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_signed SELECT number - 32 FROM numbers(64);
INSERT INTO t_signed SELECT 65536 + number FROM numbers(16);
INSERT INTO t_signed SELECT -65536 - number FROM numbers(16);
INSERT INTO t_signed SELECT arrayJoin([-32768, 32767]);

SELECT sum(w) FROM t_signed WHERE m >= -10 AND m <= -1;
SELECT sum(w) FROM t_signed WHERE m >= 0 AND m <= 15;
SELECT sum(w) FROM t_signed WHERE m = -32768 OR m = 32767;

-- Partition pruning analyses each key value as a single point, so it takes a different path through
-- the index condition.
CREATE TABLE t_partition (w UInt64, m UInt16 ALIAS w)
ENGINE = MergeTree PARTITION BY intDiv(toUInt16(w), 16) ORDER BY tuple()
SETTINGS index_granularity = 8;
INSERT INTO t_partition SELECT number FROM numbers(64);
INSERT INTO t_partition SELECT 65536 + number FROM numbers(64);

SELECT sum(w) FROM t_partition WHERE m >= 10 AND m <= 20;

-- A widening cast keeps every value of the source type, so it needs no value test.
CREATE TABLE t_widening (w UInt16, m UInt64 ALIAS w, INDEX i_m (w) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_widening SELECT number FROM numbers(64);

SELECT sum(w) FROM t_widening WHERE m >= 10 AND m <= 20;

-- A chain of monotonic functions over the primary key must still analyse correctly when a cast sits
-- in the middle of it.
CREATE TABLE t_chain (x Int64) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 8;
INSERT INTO t_chain SELECT number FROM numbers(128);

SELECT sum(x) FROM t_chain WHERE abs(negate(toInt16(x))) >= 10 AND abs(negate(toInt16(x))) <= 20;

-- A cast into `Bool` maps every nonzero value to 1, so it changes values even between types of the same
-- width. Values 8..15 fit one byte and 300..307 do not, so both are covered. The all-zero granule is the
-- only prunable one, so the `Granules` assertion also fails if a `Bool` condition stops pruning entirely.
CREATE TABLE t_bool_narrowing (w UInt64, f Bool ALIAS w, INDEX i_f (w) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_bool_narrowing SELECT number + 8 FROM numbers(8);
INSERT INTO t_bool_narrowing SELECT number + 300 FROM numbers(8);
INSERT INTO t_bool_narrowing SELECT 0 FROM numbers(4);

SELECT count() FROM t_bool_narrowing WHERE f = true;

-- Automatic `basic` statistics are on by default and would prune the all-zero part before the index
-- does, so the assertion below would report the statistics pruner's line instead of `i_f`'s.
SET use_statistics_for_part_pruning = 0;

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(w) FROM t_bool_narrowing WHERE f = true) WHERE explain ILIKE '%Granules: 4/5%';

CREATE TABLE t_bool_same_size (u UInt8, f Bool ALIAS u, INDEX i_f (u) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_bool_same_size SELECT number + 8 FROM numbers(8);
INSERT INTO t_bool_same_size SELECT 0 FROM numbers(4);

SELECT count() FROM t_bool_same_size WHERE f = true;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(u) FROM t_bool_same_size WHERE f = true) WHERE explain ILIKE '%Granules: 2/3%';

DROP TABLE t_unsigned;
DROP TABLE t_signed;
DROP TABLE t_partition;
DROP TABLE t_widening;
DROP TABLE t_chain;
DROP TABLE t_bool_narrowing;
DROP TABLE t_bool_same_size;
