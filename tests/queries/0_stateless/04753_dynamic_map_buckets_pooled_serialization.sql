-- Tags: no-fasttest
-- The pooled `Dynamic` serialization is keyed on a hash of `SerializationInfoSettings`. When that
-- hash ignores `map_serialization_version`, a table declaring `with_buckets` reuses the pooled
-- object of a table declaring `basic`, writes the basic stream layout into a part whose
-- serialization.json says `with_buckets`, and reading that part later asks for the missing
-- `.buckets_info` stream and raises LOGICAL_ERROR.

DROP TABLE IF EXISTS t_dyn_map_basic;
DROP TABLE IF EXISTS t_dyn_map_buckets;

-- Constructs the pooled `Dynamic(max_types=128)` object with `map_serialization_version = basic`.
CREATE TABLE t_dyn_map_basic (id UInt64, y Dynamic(max_types=128))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
         serialization_info_version = 'with_types',
         propagate_types_serialization_versions_to_nested_types = 1,
         map_serialization_version = 'basic',
         map_serialization_version_for_zero_level_parts = 'basic';

INSERT INTO t_dyn_map_basic SELECT number, map(number, number + 1) FROM numbers(8);

-- Same type, but every part must use the bucketed Map layout.
CREATE TABLE t_dyn_map_buckets (id UInt64, y Dynamic(max_types=128))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
         serialization_info_version = 'with_types',
         propagate_types_serialization_versions_to_nested_types = 1,
         map_serialization_version = 'with_buckets',
         map_serialization_version_for_zero_level_parts = 'with_buckets',
         max_buckets_in_map = 4,
         map_buckets_strategy = 'constant',
         map_buckets_min_avg_size = 0;

INSERT INTO t_dyn_map_buckets SELECT number, map(number, number + 1) FROM numbers(64);

-- The part must carry the bucketed streams its serialization.json declares. This is the write-side
-- assertion: with a colliding pool key the part gets the basic layout and all three counts are 0.
SELECT 'bucketed streams', countIf(s LIKE '%buckets_info'), countIf(s LIKE '%bucket_indexes'), countIf(s LIKE '%\%29.3%')
FROM (
    SELECT arrayJoin(substreams) AS s FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_dyn_map_buckets' AND column = 'y' AND active
);

-- Detaching releases the pooled object, so the reload rebuilds it from this table's own settings.
-- Pre-fix that rebuilt object expects `.buckets_info`, which the part above never wrote.
DETACH TABLE t_dyn_map_buckets;
ATTACH TABLE t_dyn_map_buckets;

SELECT 'read after reattach', count(), sum(length(y::Map(UInt64, UInt64))) FROM t_dyn_map_buckets;
SELECT 'values', y FROM t_dyn_map_buckets ORDER BY id LIMIT 2;

-- The basic table stays readable in the same session.
SELECT 'basic table', count(), sum(length(y::Map(UInt64, UInt64))) FROM t_dyn_map_basic;

DROP TABLE t_dyn_map_buckets;
DROP TABLE t_dyn_map_basic;
