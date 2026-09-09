DROP TABLE IF EXISTS t_index_hint;
DROP TABLE IF EXISTS t_index_hint_part;

CREATE TABLE t_index_hint (g UInt16, id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
INSERT INTO t_index_hint SELECT number % 10, number FROM numbers(1000);

SELECT 'no hint', count() FROM t_index_hint WHERE id >= 1 AND id <= 3;

-- A truthy constant that does not fit UInt8 must not exclude any row.
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(256);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(512);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(65536);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(-256);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(0.5);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(-0.5);
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toUInt16(256));
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toNullable(256));
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toLowCardinality(256));
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toLowCardinality(toNullable(256)));
SELECT 'truthy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(256, 512);

-- A type with no boolean interpretation contributes no filter. Each carrier appears twice, holding
-- its type default and not, to tell "no filter" apart from "converted, then compared against zero".
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint('');
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toDecimal32(0, 0));
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toUInt256(0));
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toUInt256(256));
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toInt128(256));
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toDecimal32(256, 0));
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint('x');
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint([1, 2]);
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint((1, 2));
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint('x', 256);
SELECT 'no boolean', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toDecimal32(256, 0), 256);

-- A NULL hint is not true, so pruning everything is correct; it must not throw.
SELECT 'null', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(NULL);
SELECT 'null', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(CAST(NULL, 'Nullable(UInt8)'));
SELECT 'null', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(NULL, 1);

-- The declared type, not the value, decides whether a NULL reads as a condition. system.tables
-- prunes only through this code path; on a MergeTree table the key condition prunes either way.
SELECT 'typed null', count() > 0 FROM system.tables
WHERE database = currentDatabase() AND indexHint(CAST(NULL, 'Nullable(UInt8)'));
SELECT 'typed null', count() > 0 FROM system.tables
WHERE database = currentDatabase() AND indexHint(CAST(NULL, 'Nullable(String)'));

-- A falsy hint must still prune everything (pinned by 02841/02892/02962), so each carrier here is
-- the falsy twin of a truthy one above.
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(0);
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toNullable(0));
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toLowCardinality(0));
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(toLowCardinality(toNullable(0)));
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(0.0);
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint(0, 256);
SELECT 'falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND indexHint('x', 0);

-- Under GROUPING SETS a wrongly emptied scan still emits the ()-set row, so the wrong answer is a
-- single non-empty row rather than an empty result.
SELECT 'grouping', g, count(), grouping(g) FROM t_index_hint
WHERE (id >= 1 AND id <= 3) AND indexHint(toNullable(256))
GROUP BY GROUPING SETS ((g), ()) ORDER BY g;

-- Granule pruning still applies for a hint carrying a usable condition, and not for one that does
-- not. The range sits inside the hint so the hint is the only possible source of pruning. Either
-- setting would let count() skip the index, leaving EXPLAIN with no Granules line. countIf rather
-- than a WHERE: without the analyzer extract runs before the LIKE and toUInt64 throws.
SELECT 'granules pruned', countIf(
    toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'))) > 0
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_index_hint WHERE indexHint(id >= 1 AND id <= 3)
    SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0
);

SELECT 'granules kept', countIf(explain LIKE '%Granules: %/%'
    AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) = toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'))) > 0
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_index_hint WHERE indexHint(1)
    SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0
);

-- Part pruning by a virtual column still applies. indexHint is row-level TRUE, so a count below the
-- table total can only come from parts dropped during analysis.
CREATE TABLE t_index_hint_part (p UInt8, id UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_index_hint_part SELECT number % 4, number FROM numbers(400);

SELECT 'parts pruned', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint(_partition_id = '0');
SELECT 'parts pruned', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint(_partition_id = '0', 256);
SELECT 'parts pruned', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint(_partition_id = '0', 'x');
SELECT 'parts kept', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint(256);
SELECT 'parts kept', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint('x');
-- The partition id is one character, so these are non-constant expressions worth 256 and 0. A
-- truncating cast answers 0 for both; a dropped atom answers 400 for both.
SELECT 'parts kept', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint(length(_partition_id) + 255);
SELECT 'all parts pruned', count() FROM t_index_hint_part WHERE id < 1000 AND indexHint(length(_partition_id) - 1);

-- Two hints on an allowed input keep two rewritten children under the enclosing `and`, which reuses
-- its UInt8 result type. 200 rather than 300 or 400 proves both survived.
SELECT 'two hints', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(_partition_id != '0') AND indexHint(_partition_id != '1');
SELECT 'two hints', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(_partition_id != '0') AND indexHint(toNullable(_partition_id) != '1');
SELECT 'two hints', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(_partition_id != '0') AND indexHint(toLowCardinality(toNullable(_partition_id)) != '1');
SELECT 'two hints', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(_partition_id != '0') AND indexHint(toNullable(_partition_id) != '1', 256);
SELECT 'three hints', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(_partition_id != '0') AND indexHint(toNullable(_partition_id) != '1')
  AND indexHint(_partition_id != '2');

-- A hint that is really NULL for some parts: NULL is not true, so those parts are pruned, and the
-- conversion must not throw.
SELECT 'nullable column', count() FROM t_index_hint_part
WHERE id < 1000 AND indexHint(if(_partition_id = '0', NULL, 1));

-- The sibling `and` branch of the same code path must be unaffected.
SELECT 'and branch', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND 256;
SELECT 'and branch', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND toNullable(256);
SELECT 'and branch', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND toLowCardinality(toNullable(256));
SELECT 'and branch', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND 0.5;
SELECT 'and branch falsy', count() FROM t_index_hint WHERE (id >= 1 AND id <= 3) AND 0;

-- Every analyzer has to read an argument the same way, otherwise adding an index to a table
-- changes the answer. The primary key, a `minmax` index and a `bloom_filter` index go through
-- `KeyCondition`; a `set` index has its own evaluator, which used to read a wide integer as false
-- and throw on a String.
DROP TABLE IF EXISTS t_index_hint_set;
DROP TABLE IF EXISTS t_index_hint_minmax;
DROP TABLE IF EXISTS t_index_hint_bloom;

CREATE TABLE t_index_hint_set (id UInt32, v UInt32, INDEX i v TYPE set(8) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
CREATE TABLE t_index_hint_minmax (id UInt32, v UInt32, INDEX i v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
CREATE TABLE t_index_hint_bloom (id UInt32, v UInt32, INDEX i v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
INSERT INTO t_index_hint_set SELECT number, intDiv(number, 100) FROM numbers(1000);
INSERT INTO t_index_hint_minmax SELECT number, number % 7 FROM numbers(1000);
INSERT INTO t_index_hint_bloom SELECT number, number % 7 FROM numbers(1000);

SELECT 'indexed no boolean', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(toUInt256(0));
SELECT 'indexed no boolean', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(toUInt256(256));
SELECT 'indexed no boolean', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint('x');
SELECT 'indexed no boolean', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(toDecimal32(0, 0));
SELECT 'indexed no boolean', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(CAST(NULL, 'Nullable(String)'));
SELECT 'indexed no boolean', count() FROM t_index_hint_minmax WHERE (id >= 1 AND id <= 3) AND indexHint(CAST(NULL, 'Nullable(String)'));
SELECT 'indexed no boolean', count() FROM t_index_hint_bloom WHERE (id >= 1 AND id <= 3) AND indexHint(CAST(NULL, 'Nullable(String)'));
SELECT 'indexed no boolean', count() FROM t_index_hint_minmax WHERE (id >= 1 AND id <= 3) AND indexHint(toUInt256(0));

-- A hint the analyzers do read still prunes on an indexed table.
SELECT 'indexed falsy', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(0);
SELECT 'indexed falsy', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(NULL);
SELECT 'indexed falsy', count() FROM t_index_hint_minmax WHERE (id >= 1 AND id <= 3) AND indexHint(0);
SELECT 'indexed falsy', count() FROM t_index_hint_bloom WHERE (id >= 1 AND id <= 3) AND indexHint(NULL);
SELECT 'indexed truthy', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(256);
SELECT 'indexed truthy', count() FROM t_index_hint_minmax WHERE (id >= 1 AND id <= 3) AND indexHint(256);

-- The rule is about the declared type, so a non-constant argument is dropped as well. A wide integer
-- is an integer, so the `set` index used to read `toUInt256(v)` as `v != 0` through
-- `__bitWrapperFunc` and prune away exactly the granules the query wanted.
SELECT 'indexed no boolean nonconst', count() FROM t_index_hint_set WHERE v = 0 AND indexHint(toUInt256(v));
SELECT 'indexed no boolean nonconst', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(toUInt256(v));
SELECT 'indexed no boolean nonconst', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(toInt128(v));
SELECT 'indexed no boolean nonconst', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(toDecimal64(v, 0));
SELECT 'indexed no boolean nonconst', count() FROM t_index_hint_minmax WHERE v = 0 AND indexHint(toUInt256(v));
SELECT 'indexed no boolean nonconst', count() FROM t_index_hint_bloom WHERE v = 0 AND indexHint(toUInt256(v));
-- The same expression under a type that does read as a condition still prunes: `v` is 0 for these
-- rows, so the hint excludes them.
SELECT 'indexed nonconst bool', count() FROM t_index_hint_set WHERE (id >= 1 AND id <= 3) AND indexHint(v);

-- The set index itself must still prune on a condition it understands, and a constant false in an
-- OR must still be skipped rather than making the whole OR look unusable.
SELECT 'set index prunes', count() FROM t_index_hint_set WHERE indexHint(v = 3)
SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0;
SELECT 'set index prunes', count() FROM t_index_hint_set WHERE v = 3 OR 0;

DROP TABLE t_index_hint_set;
DROP TABLE t_index_hint_minmax;
DROP TABLE t_index_hint_bloom;

DROP TABLE t_index_hint;
DROP TABLE t_index_hint_part;
