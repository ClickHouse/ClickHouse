-- Tags: no-fasttest
-- Exercises the whole-path branch of `SerializationObjectSharedData::deserializePathsData`
-- (the sibling of the subcolumns branch covered by 04551): reading a whole shared-data path
-- deserializes it through a local deserialize state that is dropped right after the produced
-- column is moved into the granule data, before the outer `SubstreamsCachePathsDataElement`
-- is created. The path values are arrays of nested `JSON` objects, so the dynamic
-- serialization builds nested `Object` states holding column references that
-- `ColumnsOwnershipValidator` (active in debug and sanitizer builds) must account for at that
-- destruction point. The queries only need to run without tripping the validator (issue #105626).

SET enable_json_type = 1;

DROP TABLE IF EXISTS t_json_whole_path_ownership;

-- A JSON column with a tiny `max_dynamic_paths` so most paths overflow into the shared-data
-- stream, stored in a Compact part. `object_shared_data_serialization_version_for_zero_level_parts`
-- is forced to `advanced` so the inserted parts use the ADVANCED shared-data serialization whose
-- read path (`deserializePathsData`) is the one instrumented here (it is `map_with_buckets` by
-- default for zero-level parts, which would not exercise this destruction point).
CREATE TABLE t_json_whole_path_ownership
(
    id UInt64,
    json JSON(max_dynamic_paths = 2)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 10000000000,
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced';

INSERT INTO t_json_whole_path_ownership
SELECT
    number,
    '{"p' || toString(number % 8) || '":[{"a":' || toString(number) || ',"s":"v' || toString(number % 3) || '"},{"b":[' || toString(number % 5) || ']}],'
        || '"q' || toString(number % 8) || '":"str_' || toString(number) || '",'
        || '"r' || toString(number % 8) || '":' || toString(number) || '}'
FROM numbers(1000);

-- Every row has three distinct paths and at most two can be dynamic, so every row keeps at
-- least one path in shared data and the whole-path reads below are served by
-- `deserializePathsData` (the exact dynamic-path split across parts depends on insert
-- settings, so it is not pinned here).
SELECT countIf(length(JSONSharedDataPaths(json)) >= 1) FROM t_json_whole_path_ownership;

-- Requesting whole paths (not typed subcolumns) puts them into `requested_paths`, so
-- `deserializePathsData` follows the whole-path branch that reads each path into a `Dynamic`
-- column through a short-lived deserialize state. `p*` paths hold `Array(JSON)` values, so the
-- state recurses into nested `Object` deserialize states.
SELECT json.p1, json.q1, json.r1 FROM t_json_whole_path_ownership WHERE json.p1 IS NOT NULL ORDER BY id LIMIT 5;
SELECT count(), countIf(json.p0 IS NULL), countIf(json.q0 IS NULL), countIf(json.r0 IS NULL) FROM t_json_whole_path_ownership;

-- Mix a whole path and a typed subcolumn of another path in one query, so both branches run in
-- the same `deserializePathsData` call.
SELECT json.p2, json.q2.:String FROM t_json_whole_path_ownership WHERE json.p2 IS NOT NULL ORDER BY id LIMIT 5;

DROP TABLE t_json_whole_path_ownership;
