-- Tags: no-fasttest
-- Regression test: reading a Tuple(m Dynamic) column and its subcolumn t.m
-- in the same query caused a crash because SerializationMap::deserializeBinaryBulkWithMultipleStreams
-- used a local copy of the nested column pointer. When the substreams cache
-- reassigned the pointer to a cached column, the change was not written back
-- to the Map column, leaving it empty while the Variant discriminators expected data.

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

DROP TABLE IF EXISTS t_map_dynamic_cache;

CREATE TABLE t_map_dynamic_cache
(
    `id` UInt64,
    `t` Tuple(m Dynamic(max_types = 8))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_map_dynamic_cache SELECT number, tuple(map(toString(number), toString(number * 2))) FROM numbers(1000);

-- This query reads both `t` (via *) and `t.m` (via mapKeys(t.m)) from the same MergeTree part.
-- Before the fix, the second read of the Dynamic/Variant column would crash with:
-- "Variant Map(LowCardinality(String), LowCardinality(String)) is empty, but expected to be read N values"
SELECT count() FROM t_map_dynamic_cache WHERE NOT ignore(*, mapKeys(t.m));

-- Also test with explicit column + subcolumn references
SELECT count() FROM t_map_dynamic_cache WHERE NOT ignore(t, mapKeys(t.m));

-- Test with mapValues
SELECT count() FROM t_map_dynamic_cache WHERE NOT ignore(*, mapValues(t.m));

DROP TABLE t_map_dynamic_cache;
