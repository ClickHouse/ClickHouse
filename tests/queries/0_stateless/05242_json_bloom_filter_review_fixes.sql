-- Regression tests for `jsonbf_v1`: each query must return the same result as without the index.

SET allow_experimental_json_bloom_filter_index = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 0;

-- Structural subcolumns below typed paths, such as `size` of a `String`, are not JSON paths.
-- `j.s = ''` is rewritten to `empty(j.s)`, which reads `j.s.size`.
DROP TABLE IF EXISTS json_bf_structural;
CREATE TABLE json_bf_structural
(
    id UInt64,
    j JSON(s String, t Tuple(b String), a Array(String), ns Nullable(String)),
    INDEX bf j TYPE jsonbf_v1 GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_bf_structural VALUES (1, '{"t":{"b":"abc"},"a":["abc"],"ns":"abc"}'), (2, '{"s":""}');
SELECT 'empty', arraySort(groupArray(id)) FROM json_bf_structural WHERE j.s = '';
SELECT 'tuple length', arraySort(groupArray(id)) FROM json_bf_structural WHERE length(j.t.b) = 3;
SELECT 'array sizes', arraySort(groupArray(id)) FROM json_bf_structural WHERE has(j.a.size, 3);
SELECT 'nullable size', arraySort(groupArray(id)) FROM json_bf_structural WHERE j.ns.size = 3;
SELECT 'length in', arraySort(groupArray(id)) FROM json_bf_structural WHERE length(j.s) IN (0, 4);
DROP TABLE json_bf_structural;

-- A column named like a subcolumn of the indexed column is read instead of the subcolumn.
DROP TABLE IF EXISTS json_bf_shadow;
CREATE TABLE json_bf_shadow (id UInt64, j JSON, `j.x` Int64, INDEX bf j TYPE jsonbf_v1 GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_bf_shadow VALUES (1, '{"x":5}', 7);
SELECT 'shadow', arraySort(groupArray(id)) FROM json_bf_shadow WHERE `j.x` = 7;
DROP TABLE json_bf_shadow;

-- The values stream of an index `v` must not collide with the files of an index named `v_values`.
DROP TABLE IF EXISTS json_bf_file_names;
CREATE TABLE json_bf_file_names
(
    id UInt64,
    j JSON,
    INDEX v j TYPE jsonbf_v1 GRANULARITY 1,
    INDEX v_values id TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0;
INSERT INTO json_bf_file_names SELECT number, toJSONString(map('a', toString(number))) FROM numbers(10000);
SELECT 'file names', count() FROM json_bf_file_names WHERE j.a = '5555';
DROP TABLE json_bf_file_names;

-- A missing path casts to the default of `Bool`, which equals `false`.
DROP TABLE IF EXISTS json_bf_bool_default;
CREATE TABLE json_bf_bool_default (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1 GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_bf_bool_default VALUES (1, '{"b":1}'), (2, '{"a":2}'), (3, '{"b":0}');
SELECT 'bool default', arraySort(groupArray(id)) FROM json_bf_bool_default WHERE j.b::Bool = false;
DROP TABLE json_bf_bool_default;

-- Index analysis must not throw for constants that do not convert to the path type.
DROP TABLE IF EXISTS json_bf_conversions;
CREATE TABLE json_bf_conversions (id UInt64, j JSON(d Date, i Int128), INDEX bf j TYPE jsonbf_v1 GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_bf_conversions VALUES (1, '{"d":"2020-01-01","i":1}');
SELECT 'datetime64', arraySort(groupArray(id)) FROM json_bf_conversions WHERE j.d = toDateTime64('2020-01-01 00:00:00', 3, 'UTC');
SELECT 'decimal', arraySort(groupArray(id)) FROM json_bf_conversions WHERE j.i = toDecimal32(1, 2);
DROP TABLE json_bf_conversions;

-- Skipping granules must not hide exceptions that the query throws without the index.
DROP TABLE IF EXISTS json_bf_exceptions;
CREATE TABLE json_bf_exceptions (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1 GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_bf_exceptions VALUES (1, '{"x":1}'), (2, '{}');
SELECT arraySort(groupArray(id)) FROM json_bf_exceptions WHERE CAST(j.x.:Int64 AS String) = '1'; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
TRUNCATE TABLE json_bf_exceptions;
INSERT INTO json_bf_exceptions VALUES (1, '{"x":"a"}'), (2, '{"x":5}');
SELECT arraySort(groupArray(id)) FROM json_bf_exceptions WHERE j.x.:String = 1.5; -- { serverError NO_COMMON_TYPE }
DROP TABLE json_bf_exceptions;

-- `EXPLAIN WHATIF` evaluates granules built in memory, including `Dynamic` and cast predicates.
DROP TABLE IF EXISTS json_bf_whatif;
CREATE TABLE json_bf_whatif (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_bf_whatif VALUES (1, '{"a":5}'), (2, '{"a":6}');
CREATE HYPOTHETICAL INDEX hi ON json_bf_whatif (j) TYPE jsonbf_v1 GRANULARITY 1;
SELECT 'whatif dynamic', trim(explain) FROM (EXPLAIN WHATIF SELECT count() FROM json_bf_whatif WHERE j.a = 5)
WHERE trim(explain) LIKE 'empirical_status:%' OR trim(explain) LIKE 'skip_ratio:%';
SELECT 'whatif cast', trim(explain) FROM (EXPLAIN WHATIF SELECT count() FROM json_bf_whatif WHERE j.a::String = '5')
WHERE trim(explain) LIKE 'empirical_status:%';
DROP TABLE json_bf_whatif;

-- The index is experimental. Changes to existing tables that do not add the index keep working without the setting.
SET allow_experimental_json_bloom_filter_index = 0;
DROP TABLE IF EXISTS json_bf_experimental;
CREATE TABLE json_bf_experimental (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1 GRANULARITY 1) ENGINE = MergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE json_bf_experimental (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id;
ALTER TABLE json_bf_experimental ADD INDEX bf j TYPE jsonbf_v1 GRANULARITY 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE json_bf_experimental ADD INDEX bf j TYPE jsonbf_v1 GRANULARITY 1 SETTINGS allow_experimental_json_bloom_filter_index = 1;
ALTER TABLE json_bf_experimental ADD COLUMN c UInt8;
SELECT 'experimental', name, type FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'json_bf_experimental';
DROP TABLE json_bf_experimental;
