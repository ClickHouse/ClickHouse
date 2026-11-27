-- Tags: no-fasttest
-- Exercises two extra destruction points now covered by `ColumnsOwnershipValidator`
-- (active in debug and sanitizer builds):
--   1. `LogSource::fillPhysicalColumns` for the `Log` engine, where a single `SubstreamsCache`
--      is shared across the subcolumns of a `Nested` group (shared array offsets), and the
--      caches die at the end of the function.
--   2. `SerializationObjectSharedData::deserializePathsData`, where the local subcolumn caches
--      and deserialize states of a shared-data path are dropped before the outer cache element
--      that later holds the produced subcolumns is created.
-- The queries only need to run without tripping the validator (issue #105626).

SET enable_json_type = 1;

DROP TABLE IF EXISTS t_log_nested_ownership;

CREATE TABLE t_log_nested_ownership
(
    key UInt64,
    n Nested(a UInt64, b String, lc LowCardinality(String))
)
ENGINE = Log;

INSERT INTO t_log_nested_ownership
SELECT
    number,
    range(number % 5),
    arrayMap(x -> toString(x), range(number % 5)),
    arrayMap(x -> toString(x % 3), range(number % 5))
FROM numbers(1000);

-- Reading several subcolumns of the same `Nested` group makes `getCacheKey` share one
-- `SubstreamsCache` across them, so the shared array offsets are read once and referenced
-- from every subcolumn; the ownership validator runs when those caches are dropped.
SELECT count(), sum(length(n.a)), sum(length(n.b)), sum(length(n.lc)) FROM t_log_nested_ownership;
SELECT n.a, n.b, n.lc, n.a.size0 FROM t_log_nested_ownership ORDER BY key LIMIT 5;

DROP TABLE t_log_nested_ownership;

DROP TABLE IF EXISTS t_json_shared_data_ownership;

-- A JSON column with a tiny `max_dynamic_paths` so most paths overflow into the shared-data
-- stream, stored in a Compact part. `object_shared_data_serialization_version_for_zero_level_parts`
-- is forced to `advanced` so the inserted parts use the ADVANCED shared-data serialization whose
-- read path (`deserializePathsData`) is the one instrumented here (it is `map_with_buckets` by
-- default for zero-level parts, which would not exercise this destruction point).
CREATE TABLE t_json_shared_data_ownership
(
    id UInt64,
    json JSON(max_dynamic_paths = 2)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 10000000000,
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced';

INSERT INTO t_json_shared_data_ownership
SELECT
    number,
    toJSONString(map(
        'p' || toString(number % 8), range(number % 4 + 1)::Array(UInt32),
        'q' || toString(number % 8), 'str_' || toString(number),
        'r' || toString(number % 8), number))
FROM numbers(1000);

-- Read typed subcolumns of shared-data paths, including several subcolumns of the same path,
-- so `deserializePathsData` follows the `requested_paths_subcolumns` branch and orders multiple
-- subcolumn reads that share substreams.
SELECT
    count(),
    sum(length(json.p0.:`Array(Nullable(Int64))`)),
    sum(length(json.q0.:String)),
    sum(json.r0.:Int64)
FROM t_json_shared_data_ownership;

SELECT
    json.p1.:`Array(Nullable(Int64))`,
    json.p1.:String,
    json.q1.:String,
    json.r1.:Int64
FROM t_json_shared_data_ownership ORDER BY id LIMIT 5;

DROP TABLE t_json_shared_data_ownership;
