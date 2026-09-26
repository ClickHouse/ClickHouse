-- { echo }
-- A subcolumn extracted from `Nullable(Tuple(...))` with a non-nullable `LowCardinality(T)` element gets
-- its type promoted to `LowCardinality(Nullable(T))`. On a Compact part read the parent-null-map wrapper
-- must still deserialize into the on-disk `LowCardinality(T)` representation, because the substreams it
-- publishes to the shared substreams cache are adopted directly by a reader of the parent subcolumn.

SET enable_nullable_tuple_type = 1;
SET allow_suspicious_low_cardinality_types = 1;

-- Reading the child `x.a.b` must not change what the parent `x.a` reads: the parent's dictionary must stay
-- non-nullable and must not gain a NULL placeholder. Before the fix it gained one, with no NULL in the data.
DROP TABLE IF EXISTS t_04651_no_nulls;
CREATE TABLE t_04651_no_nulls (x Tuple(a Nullable(Tuple(b LowCardinality(UInt32))))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1000000000, write_marks_for_substreams_in_compact_parts = 1;
INSERT INTO t_04651_no_nulls SELECT tuple(tuple(number)) FROM numbers(4);
SELECT dumpColumnStructure(x.a) FROM t_04651_no_nulls LIMIT 1;
SELECT x.a.b, dumpColumnStructure(x.a) FROM t_04651_no_nulls LIMIT 1;
SELECT x.a.b, x.a FROM t_04651_no_nulls;
DROP TABLE t_04651_no_nulls;

-- The reported shape: child, whole column, parent, and `assumeNotNull` over the parent in one query.
-- `assumeNotNull` has an unspecified result on a NULL input, so its value is never asserted here: it is
-- wrapped in `ignore` so that it is still evaluated, because what the bug breaks is its return-type
-- post-condition and not the value it returns.
DROP TABLE IF EXISTS t_04651_compact;
CREATE TABLE t_04651_compact (x Tuple(a Nullable(Tuple(b LowCardinality(UInt32))))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1000000000, write_marks_for_substreams_in_compact_parts = 1;
INSERT INTO t_04651_compact SELECT number % 2 ? tuple(NULL) : tuple(tuple(number)) FROM numbers(4);
SELECT toTypeName(x.a.b), toTypeName(x.a) FROM t_04651_compact LIMIT 1;
SELECT x.a.b, x, x.a, isNull(x.a.b), ignore(assumeNotNull(x.a)) FROM t_04651_compact;
SELECT x.a.b, x.a FROM t_04651_compact;
SELECT x.a, x.a.b FROM t_04651_compact;
SELECT x.a.b, x.a FROM t_04651_compact SETTINGS max_block_size = 1;
DROP TABLE t_04651_compact;

-- Leaf types other than UInt32 read through the same path.
DROP TABLE IF EXISTS t_04651_leaves;
CREATE TABLE t_04651_leaves
(
    s Tuple(a Nullable(Tuple(b LowCardinality(String)))),
    f Tuple(a Nullable(Tuple(b LowCardinality(FixedString(3))))),
    d Tuple(a Nullable(Tuple(b LowCardinality(Date)))),
    m Tuple(a Nullable(Tuple(b LowCardinality(UInt32), c UInt8)))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1000000000, write_marks_for_substreams_in_compact_parts = 1;
INSERT INTO t_04651_leaves SELECT
    number % 2 ? tuple(NULL) : tuple(tuple(concat('s', toString(number)))),
    number % 2 ? tuple(NULL) : tuple(tuple(toFixedString(concat('s', toString(number)), 3))),
    number % 2 ? tuple(NULL) : tuple(tuple(toDate(number))),
    number % 2 ? tuple(NULL) : tuple(tuple(number, number))
FROM numbers(4);
SELECT s.a.b, s.a, f.a.b, f.a, d.a.b, d.a, m.a.b, m.a FROM t_04651_leaves;
SELECT ignore(assumeNotNull(s.a)), ignore(assumeNotNull(m.a)) FROM t_04651_leaves;
DROP TABLE t_04651_leaves;

-- A leaf that is `LowCardinality(Nullable(T))` on disk must NOT be de-nullabilised: its stream really is
-- nullable, so the buffer has to stay as it is.
DROP TABLE IF EXISTS t_04651_lc_nullable;
CREATE TABLE t_04651_lc_nullable (x Tuple(a Nullable(Tuple(b LowCardinality(Nullable(UInt32)))))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1000000000, write_marks_for_substreams_in_compact_parts = 1;
INSERT INTO t_04651_lc_nullable VALUES ((NULL)), (((NULL))), (((7))), ((NULL));
SELECT toTypeName(x.a.b), toTypeName(x.a) FROM t_04651_lc_nullable LIMIT 1;
SELECT x.a.b, x, x.a, isNull(x.a.b) FROM t_04651_lc_nullable;
SELECT x.a.b, x.a FROM t_04651_lc_nullable;
DROP TABLE t_04651_lc_nullable;

-- Wide part control: measured as unaffected.
DROP TABLE IF EXISTS t_04651_wide;
CREATE TABLE t_04651_wide (x Tuple(a Nullable(Tuple(b LowCardinality(UInt32))))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_04651_wide SELECT number % 2 ? tuple(NULL) : tuple(tuple(number)) FROM numbers(4);
SELECT x.a.b, x.a, ignore(assumeNotNull(x.a)) FROM t_04651_wide;
DROP TABLE t_04651_wide;

-- Top-level `Nullable(Tuple(...))` control: the element is read without the outer `Tuple` in the path.
DROP TABLE IF EXISTS t_04651_top_level;
CREATE TABLE t_04651_top_level (id UInt8, x Nullable(Tuple(b LowCardinality(UInt32)))) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1000000000;
INSERT INTO t_04651_top_level SELECT number, number % 2 ? NULL : tuple(number) FROM numbers(4);
SELECT x.b, x, ignore(assumeNotNull(x)) FROM t_04651_top_level ORDER BY id;
DROP TABLE t_04651_top_level;

-- Several granule ranges per block, so the cached substream carries rows of more than one range.
DROP TABLE IF EXISTS t_04651_ranges;
CREATE TABLE t_04651_ranges (x Tuple(a Nullable(Tuple(b LowCardinality(String))))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 3, min_bytes_for_wide_part = 1000000000, write_marks_for_substreams_in_compact_parts = 1;
INSERT INTO t_04651_ranges SELECT number % 3 = 0 ? tuple(NULL) : tuple(tuple(concat('s', toString(number % 5)))) FROM numbers(30);
SELECT count(), countIf(x.a IS NULL), uniqExact(x.a.b), sum(cityHash64(x.a.b, x.a)) FROM t_04651_ranges SETTINGS max_block_size = 4;
SELECT groupArray(x.a) FROM (SELECT x.a.b, x.a FROM t_04651_ranges SETTINGS max_block_size = 2);
DROP TABLE t_04651_ranges;
