-- Tags: no-random-merge-tree-settings
-- Regression tests for `map_serialization_version = 'with_key_columns'`.

SET allow_experimental_map_key_columns = 1;
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

SELECT '-- experimental gate';
CREATE TABLE t_kc_gate (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns' SETTINGS allow_experimental_map_key_columns = 0; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_kc_gate (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_kc_gate MODIFY SETTING map_serialization_version = 'with_key_columns' SETTINGS allow_experimental_map_key_columns = 0; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE t_kc_gate;

SELECT '-- compact parts require substream marks';
CREATE TABLE t_kc_marks (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', write_marks_for_substreams_in_compact_parts = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- key count limit';
CREATE TABLE t_kc_limit (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', max_map_key_columns_keys = 3;
INSERT INTO t_kc_limit SELECT number, map(toString(number), number) FROM numbers(4); -- { serverError LIMIT_EXCEEDED }
INSERT INTO t_kc_limit VALUES (1, {'a': 1, 'b': 2});
INSERT INTO t_kc_limit VALUES (2, {'c': 3, 'd': 4});
OPTIMIZE TABLE t_kc_limit FINAL; -- { serverError LIMIT_EXCEEDED }
SELECT id, m FROM t_kc_limit ORDER BY id;
DROP TABLE t_kc_limit;

SELECT '-- subcolumn rewrites';
CREATE TABLE t_kc_explain (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_kc_explain VALUES (1, {'a': 1}), (2, {});
CREATE VIEW v_kc_explain AS SELECT * FROM t_kc_explain;
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT m['a'] FROM t_kc_explain) WHERE explain LIKE '%m.key_a%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT mapContainsKey(m, 'a') FROM t_kc_explain) WHERE explain LIKE '%m.keys%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT mapContains(m, 'a') FROM t_kc_explain) WHERE explain LIKE '%m.keys%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT length(m) FROM v_kc_explain) WHERE explain LIKE '%m.size0%';
SELECT length(m), empty(m) FROM v_kc_explain ORDER BY id;
DROP VIEW v_kc_explain;
DROP TABLE t_kc_explain;

SELECT '-- Map inside Tuple';
CREATE TABLE t_kc_tuple (id UInt64, t Tuple(m Map(String, UInt64))) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types',
    propagate_types_serialization_versions_to_nested_types = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_kc_tuple VALUES (1, tuple(map('a', 1, 'b', 2))), (2, tuple(map()));
SELECT length(t.m), empty(t.m), notEmpty(t.m) FROM t_kc_tuple ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT length(t.m), empty(t.m), notEmpty(t.m) FROM t_kc_tuple ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
DROP TABLE t_kc_tuple;

SELECT '-- values of a Map added by ALTER';
CREATE TABLE t_kc_added (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types', min_bytes_for_wide_part = 0;
INSERT INTO t_kc_added VALUES (7);
ALTER TABLE t_kc_added ADD COLUMN m Map(String, UInt64) DEFAULT map('a', id);
SELECT m.values, mapValues(m), m.keys, m['a'] FROM t_kc_added SETTINGS use_columns_cache = 0;
SELECT m.values FROM t_kc_added SETTINGS use_columns_cache = 0;
DROP TABLE t_kc_added;

SELECT '-- merge applying lightweight updates with new keys';
CREATE TABLE t_kc_patch (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', min_bytes_for_wide_part = 0, index_granularity = 4,
    merge_max_block_size = 4, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_kc_patch SELECT number, map('old', number) FROM numbers(100);
UPDATE t_kc_patch SET m = map('new', id) WHERE id >= 50 SETTINGS allow_experimental_lightweight_update = 1;
OPTIMIZE TABLE t_kc_patch FINAL;
SELECT count(), countIf(mapContains(m, 'new')), sum(m['new']), sum(m['old']) FROM t_kc_patch;
DROP TABLE t_kc_patch;

SELECT '-- TTL defaults with new keys';
CREATE TABLE t_kc_ttl_keys (id UInt64, ts DateTime, m Map(String, UInt64) DEFAULT map(toString(id), id) TTL ts + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', min_bytes_for_wide_part = 0, index_granularity = 4,
    merge_max_block_size = 4, merge_with_ttl_timeout = 0;
INSERT INTO t_kc_ttl_keys SELECT number, if(number % 2, toDateTime('2000-01-01'), toDateTime('2100-01-01')), map('x', number) FROM numbers(40);
OPTIMIZE TABLE t_kc_ttl_keys FINAL;
SELECT count(), sum(length(m)), countIf(mapContains(m, '39')), countIf(mapContains(m, 'x')) FROM t_kc_ttl_keys;
DROP TABLE t_kc_ttl_keys;

SELECT '-- TTL defaults and skip index';
CREATE TABLE t_kc_ttl_index
(
    id UInt64,
    ts DateTime,
    m Map(String, UInt64) DEFAULT map('z', 1, 'a', 1) TTL ts,
    INDEX im toString(m) TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', min_bytes_for_wide_part = 0, index_granularity = 1, merge_with_ttl_timeout = 0;
INSERT INTO t_kc_ttl_index VALUES (1, '2000-01-01', map('b', 1)), (2, '2100-01-01', map('b', 2));
OPTIMIZE TABLE t_kc_ttl_index FINAL;
SELECT id, m FROM t_kc_ttl_index ORDER BY id;
SELECT count() FROM t_kc_ttl_index WHERE toString(m) = '{\'a\':1,\'z\':1}' SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_kc_ttl_index WHERE toString(m) = '{\'a\':1,\'z\':1}' SETTINGS use_skip_indexes = 0;
DROP TABLE t_kc_ttl_index;

SELECT '-- computed Map in a projection';
CREATE TABLE t_kc_projection (id UInt64, m Map(String, UInt64), PROJECTION p (SELECT mapFromArrays(['b', 'a'], [id, id]) AS pm, count() GROUP BY pm))
ENGINE = MergeTree ORDER BY id SETTINGS map_serialization_version = 'with_key_columns', min_bytes_for_wide_part = 0;
INSERT INTO t_kc_projection VALUES (1, {'y': 1, 'x': 2});
INSERT INTO t_kc_projection VALUES (2, {'y': 1});
SELECT mapFromArrays(['b', 'a'], [id, id]) AS pm, count() FROM t_kc_projection GROUP BY pm ORDER BY pm SETTINGS force_optimize_projection_name = 'p';
SELECT mapFromArrays(['b', 'a'], [id, id]) AS pm, count() FROM t_kc_projection GROUP BY pm ORDER BY pm SETTINGS optimize_use_projections = 0;
OPTIMIZE TABLE t_kc_projection FINAL;
SELECT mapFromArrays(['b', 'a'], [id, id]) AS pm, count() FROM t_kc_projection GROUP BY pm ORDER BY pm SETTINGS force_optimize_projection_name = 'p';
DROP TABLE t_kc_projection;

SELECT '-- sparse keys read and written per key';
CREATE TABLE t_kc_sparse (id UInt64, m Map(String, Nullable(UInt64))) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', min_bytes_for_wide_part = 0;
INSERT INTO t_kc_sparse SELECT number, if(number % 3 = 0, map(toString(number % 7), number, 'n', NULL), map(toString(number % 5), number)) FROM numbers(1000);
SELECT sum(length(m)), sum(m['3']), countIf(mapContains(m, 'n')), countIf(isNull(m['n'])) FROM t_kc_sparse;
SELECT id, m FROM t_kc_sparse WHERE id IN (0, 1, 3, 6, 999) ORDER BY id;
DROP TABLE t_kc_sparse;

SELECT '-- keys and values subcolumns of nested Maps';
CREATE TABLE t_kc_nested_sub (id UInt64, a Array(Map(String, UInt64)), t Tuple(x Map(String, UInt64))) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types',
    propagate_types_serialization_versions_to_nested_types = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_kc_nested_sub VALUES (1, [{'a': 1}, {'b': 2}], tuple(map('b', 1, 'a', 2)));
INSERT INTO t_kc_nested_sub VALUES (2, [{'c': 3}], tuple(map()));
SELECT id, a.keys, a.values, t.x.keys, t.x.values FROM t_kc_nested_sub ORDER BY id;
SELECT id FROM t_kc_nested_sub PREWHERE has(a.keys[1], 'a') ORDER BY id;
OPTIMIZE TABLE t_kc_nested_sub FINAL;
SELECT id, a.keys, a.values, t.x.keys, t.x.values FROM t_kc_nested_sub ORDER BY id;
DROP TABLE t_kc_nested_sub;
