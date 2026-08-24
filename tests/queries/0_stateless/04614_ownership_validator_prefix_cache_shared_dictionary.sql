-- Regression coverage for the ColumnsOwnershipValidator (debug/sanitizer builds only) over the
-- DeserializationPrefixesCache. On a prefix-cached Wide read the reader operates on clones of the
-- prefix states while the cache keeps the originals; both share the same LowCardinality `global_dictionary`
-- (including the dictionary held by a LowCardinality state nested inside a Variant state). The validator
-- must count those cache-held holders too, otherwise a broken reference count on a reader-local clone
-- could stay invisible on prefix-cached reads. See https://github.com/ClickHouse/ClickHouse/issues/105626.

DROP TABLE IF EXISTS t_lc_prefix_cache_wide;

CREATE TABLE t_lc_prefix_cache_wide
(
    id UInt64,
    lc LowCardinality(String),
    v Variant(LowCardinality(String), UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192;

-- A single dictionary for the whole part makes each LowCardinality prefix state carry a shared global
-- dictionary that the deserialization prefixes cache keeps alongside the reader clones.
INSERT INTO t_lc_prefix_cache_wide
SELECT
    number,
    concat('v', toString(number % 10)),
    CAST(if(number % 2 = 0, concat('s', toString(number % 7)), toString(number)), 'Variant(LowCardinality(String), UInt64)')
FROM numbers(100000)
SETTINGS low_cardinality_use_single_dictionary_for_part = 1;

-- All active parts must be Wide (independent of how many the insert produced).
SELECT count() > 0, countIf(part_type = 'Wide') = count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't_lc_prefix_cache_wide' AND active;

-- Read whole columns and subcolumns with the deserialization prefixes cache enabled, exercising the
-- Wide reader's ownership validator over the cache-held prefix states and their shared dictionaries.
SELECT count(), uniqExact(lc)
FROM t_lc_prefix_cache_wide
SETTINGS merge_tree_use_deserialization_prefixes_cache = 1, max_threads = 4;

SELECT sum(length(lc))
FROM t_lc_prefix_cache_wide
SETTINGS merge_tree_use_deserialization_prefixes_cache = 1, max_threads = 4;

SELECT
    count(variantElement(v, 'UInt64')),
    sum(variantElement(v, 'UInt64')),
    uniqExact(variantElement(v, 'LowCardinality(String)'))
FROM t_lc_prefix_cache_wide
SETTINGS merge_tree_use_deserialization_prefixes_cache = 1, max_threads = 4;

DROP TABLE t_lc_prefix_cache_wide;
