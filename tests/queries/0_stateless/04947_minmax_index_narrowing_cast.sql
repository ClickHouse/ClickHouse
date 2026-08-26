-- Tests that granule pruning stays correct when a narrowing integer cast sits in the index condition's
-- monotonic function chain, i.e. when the index expression is wider than the column the query names.

DROP TABLE IF EXISTS t_unsigned;
DROP TABLE IF EXISTS t_signed;
DROP TABLE IF EXISTS t_partition;
DROP TABLE IF EXISTS t_widening;
DROP TABLE IF EXISTS t_chain;

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

-- Partition pruning analyses every key coordinate as a single point, which bypasses the monotonicity check.
CREATE TABLE t_partition (w UInt64, m UInt16 ALIAS w)
ENGINE = MergeTree PARTITION BY intDiv(toUInt16(w), 16) ORDER BY tuple()
SETTINGS index_granularity = 8;
INSERT INTO t_partition SELECT number FROM numbers(64);
INSERT INTO t_partition SELECT 65536 + number FROM numbers(64);

SELECT sum(w) FROM t_partition WHERE m >= 10 AND m <= 20;

-- A widening cast is the identity on every value of the source type, so it is skipped unconditionally.
CREATE TABLE t_widening (w UInt16, m UInt64 ALIAS w, INDEX i_m (w) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_widening SELECT number FROM numbers(64);

SELECT sum(w) FROM t_widening WHERE m >= 10 AND m <= 20;

-- A chain over the primary key holds its bounds as references into the index block, and every function in
-- the chain must receive the argument type it was built for.
CREATE TABLE t_chain (x Int64) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 8;
INSERT INTO t_chain SELECT number FROM numbers(128);

SELECT sum(x) FROM t_chain WHERE abs(negate(toInt16(x))) >= 10 AND abs(negate(toInt16(x))) <= 20;

DROP TABLE t_unsigned;
DROP TABLE t_signed;
DROP TABLE t_partition;
DROP TABLE t_widening;
DROP TABLE t_chain;
