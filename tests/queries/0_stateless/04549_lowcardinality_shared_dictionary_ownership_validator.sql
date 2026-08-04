-- Exercises the coverage of `ColumnsOwnershipValidator` (active in debug and sanitizer builds):
-- a shared `LowCardinality` dictionary is hidden from `IColumn::forEachSubcolumn`, and
-- deserialize states nested inside composite serializations (e.g. the `LowCardinality` state
-- inside a `Variant` state) are not registered in any deserialize-states cache, so both are
-- reachable only through the dedicated validator traversals.

SET allow_experimental_variant_type = 1;

DROP TABLE IF EXISTS t_lc_shared_dict_wide;

-- A Wide part with a single LowCardinality dictionary: the reader shares the dictionary from the
-- deserialize state with the result columns (ColumnLowCardinality with is_shared = true).
CREATE TABLE t_lc_shared_dict_wide
(
    key UInt64,
    lc LowCardinality(String),
    v Variant(LowCardinality(String), UInt64),
    t Tuple(a LowCardinality(String), b UInt64)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1024;

INSERT INTO t_lc_shared_dict_wide
SELECT
    number,
    toString(number % 10),
    multiIf(number % 3 = 0, NULL, number % 3 = 1, CAST(concat('s', toString(number % 10)), 'Variant(LowCardinality(String), UInt64)'), CAST(number, 'Variant(LowCardinality(String), UInt64)')),
    (toString(number % 10), number)
FROM numbers(100000);

SELECT count(), uniqExact(lc), sum(length(lc)) FROM t_lc_shared_dict_wide;
SELECT count() FROM t_lc_shared_dict_wide WHERE lc = '5';

-- Read the whole Variant together with its element subcolumns and an element null map,
-- so the discriminators and the variant streams go through the substreams cache.
SELECT count(), uniqExact(v.`LowCardinality(String)`), countIf(v.UInt64.null) FROM t_lc_shared_dict_wide;
SELECT v, v.`LowCardinality(String)`, v.UInt64 FROM t_lc_shared_dict_wide ORDER BY key LIMIT 5;

SELECT t.a, t.b FROM t_lc_shared_dict_wide ORDER BY key LIMIT 3;

DROP TABLE t_lc_shared_dict_wide;

DROP TABLE IF EXISTS t_lc_shared_dict_compact;

-- The same reads from a Compact part: MergeTreeReaderCompactSingleBuffer validates per granule.
CREATE TABLE t_lc_shared_dict_compact
(
    key UInt64,
    lc LowCardinality(String),
    v Variant(LowCardinality(String), UInt64),
    t Tuple(a LowCardinality(String), b UInt64)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 1000000000, index_granularity = 1024;

INSERT INTO t_lc_shared_dict_compact
SELECT
    number,
    toString(number % 10),
    multiIf(number % 3 = 0, NULL, number % 3 = 1, CAST(concat('s', toString(number % 10)), 'Variant(LowCardinality(String), UInt64)'), CAST(number, 'Variant(LowCardinality(String), UInt64)')),
    (toString(number % 10), number)
FROM numbers(100000);

SELECT count(), uniqExact(lc), sum(length(lc)) FROM t_lc_shared_dict_compact;
SELECT count(), uniqExact(v.`LowCardinality(String)`), countIf(v.UInt64.null) FROM t_lc_shared_dict_compact;
SELECT v, v.`LowCardinality(String)`, v.UInt64 FROM t_lc_shared_dict_compact ORDER BY key LIMIT 5;
SELECT t.a, t.b FROM t_lc_shared_dict_compact ORDER BY key LIMIT 3;

DROP TABLE t_lc_shared_dict_compact;
