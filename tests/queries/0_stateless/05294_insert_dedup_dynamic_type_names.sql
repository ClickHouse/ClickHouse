-- Insert deduplication must keep an insert whose `Dynamic` values differ from an earlier one only in type,
-- even when the bytes of the values are equal, and must still deduplicate a retried insert whose `Dynamic`
-- column holds a different set of other types.

SET max_insert_threads = 1;

DROP TABLE IF EXISTS t_bool_int8;
DROP TABLE IF EXISTS t_datetime_ipv4;
DROP TABLE IF EXISTS t_rows;
DROP TABLE IF EXISTS t_same;
DROP TABLE IF EXISTS t_same_rows;
DROP TABLE IF EXISTS t_other_value;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_json_typed;
DROP TABLE IF EXISTS t_json_same;
DROP TABLE IF EXISTS t_replicated;
DROP TABLE IF EXISTS t_json_src;
DROP TABLE IF EXISTS t_json_dst;
DROP TABLE IF EXISTS t_dynamic_src;
DROP TABLE IF EXISTS t_dynamic_dst;

-- Values of different types are different data, even with equal bytes.
CREATE TABLE t_bool_int8 (id UInt64, v Dynamic) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_bool_int8 VALUES (1, true::Bool);
INSERT INTO t_bool_int8 VALUES (1, 1::Int8);
SELECT 'Bool and Int8 kept', count(), arraySort(groupArray(dynamicType(v))) FROM t_bool_int8;

CREATE TABLE t_datetime_ipv4 (id UInt64, v Dynamic) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_datetime_ipv4 VALUES (1, toDateTime(42, 'UTC'));
INSERT INTO t_datetime_ipv4 VALUES (1, toIPv4('0.0.0.42'));
SELECT 'DateTime and IPv4 kept', count() FROM t_datetime_ipv4;

-- Several rows, one of them NULL, one value changes type.
CREATE TABLE t_rows (id UInt64, v Dynamic) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_rows SETTINGS deduplicate_insert_select = 'enable_even_for_bad_queries'
SELECT number, multiIf(number = 0, true::Bool::Dynamic, number = 1, NULL::Dynamic, 'a'::String::Dynamic) FROM numbers(3);
INSERT INTO t_rows SETTINGS deduplicate_insert_select = 'enable_even_for_bad_queries'
SELECT number, multiIf(number = 0, 1::Int8::Dynamic, number = 1, NULL::Dynamic, 'a'::String::Dynamic) FROM numbers(3);
SELECT 'rows with a changed type kept', count() FROM t_rows;

-- The same values twice are still duplicates.
CREATE TABLE t_same (id UInt64, v Dynamic) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_same VALUES (1, 1::Int8);
INSERT INTO t_same VALUES (1, 1::Int8);
SELECT 'identical value deduplicated', count() FROM t_same;

CREATE TABLE t_same_rows (id UInt64, v Dynamic) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_same_rows SETTINGS deduplicate_insert_select = 'enable_even_for_bad_queries'
SELECT number, multiIf(number = 0, true::Bool::Dynamic, number = 1, NULL::Dynamic, 'a'::String::Dynamic) FROM numbers(3);
INSERT INTO t_same_rows SETTINGS deduplicate_insert_select = 'enable_even_for_bad_queries'
SELECT number, multiIf(number = 0, true::Bool::Dynamic, number = 1, NULL::Dynamic, 'a'::String::Dynamic) FROM numbers(3);
SELECT 'identical rows deduplicated', count() FROM t_same_rows;

CREATE TABLE t_other_value (id UInt64, v Dynamic) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_other_value VALUES (1, 1::Int8);
INSERT INTO t_other_value VALUES (1, 2::Int8);
SELECT 'other value of the same type kept', count() FROM t_other_value;

-- `Dynamic` nested in other types.
CREATE TABLE t_array (id UInt64, v Array(Dynamic)) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_array VALUES (1, [true::Bool]);
INSERT INTO t_array VALUES (1, [1::Int8]);
SELECT 'Array(Dynamic) kept', count() FROM t_array;

CREATE TABLE t_tuple (id UInt64, v Tuple(x Dynamic)) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_tuple VALUES (1, tuple(true::Bool));
INSERT INTO t_tuple VALUES (1, tuple(1::Int8));
SELECT 'Tuple(Dynamic) kept', count() FROM t_tuple;

CREATE TABLE t_map (id UInt64, v Map(String, Dynamic)) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_map VALUES (1, map('k', true::Bool));
INSERT INTO t_map VALUES (1, map('k', 1::Int8));
SELECT 'Map(String, Dynamic) kept', count() FROM t_map;

-- `JSON` paths: `1.0` and the integer with the same 8 bytes.
CREATE TABLE t_json (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_json VALUES (1, '{"a": 1.0}');
INSERT INTO t_json VALUES (1, '{"a": 4607182418800017408}');
SELECT 'JSON path kept', count(), arraySort(groupArray(dynamicType(data.a))) FROM t_json;

CREATE TABLE t_json_typed (id UInt64, data JSON(a Dynamic)) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_json_typed VALUES (1, '{"a": 1.0}');
INSERT INTO t_json_typed VALUES (1, '{"a": 4607182418800017408}');
SELECT 'JSON typed Dynamic path kept', count() FROM t_json_typed;

CREATE TABLE t_json_same (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_json_same VALUES (1, '{"a": 1.0}');
INSERT INTO t_json_same VALUES (1, '{"a": 1.0}');
SELECT 'identical JSON deduplicated', count() FROM t_json_same;

-- `ReplicatedMergeTree` deduplicates with default settings.
CREATE TABLE t_replicated (id UInt64, v Dynamic) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated', '1') ORDER BY id;
INSERT INTO t_replicated VALUES (1, true::Bool);
INSERT INTO t_replicated VALUES (1, 1::Int8);
SELECT 'ReplicatedMergeTree kept', count() FROM t_replicated;

-- A retried INSERT SELECT deduplicates although a merge of the source added another type to the column.
CREATE TABLE t_json_src (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_json_dst (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_json_src VALUES (1, '{"a": "x"}');
INSERT INTO t_json_dst SELECT * FROM t_json_src WHERE id = 1 ORDER BY ALL;
INSERT INTO t_json_dst SELECT * FROM t_json_src WHERE id = 1 ORDER BY ALL;
SELECT 'JSON retry before the merge', count() FROM t_json_dst;
INSERT INTO t_json_src VALUES (2, '{"a": 5}');
OPTIMIZE TABLE t_json_src FINAL;
INSERT INTO t_json_dst SELECT * FROM t_json_src WHERE id = 1 ORDER BY ALL;
SELECT 'JSON retry after the merge', count() FROM t_json_dst;

CREATE TABLE t_dynamic_src (id UInt64, v Dynamic) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_dynamic_dst (id UInt64, v Dynamic) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_dynamic_src VALUES (1, 'x'::String);
INSERT INTO t_dynamic_dst SETTINGS deduplicate_insert_select = 'enable_even_for_bad_queries' SELECT * FROM t_dynamic_src WHERE id = 1;
INSERT INTO t_dynamic_src VALUES (2, 5::Int64);
OPTIMIZE TABLE t_dynamic_src FINAL;
INSERT INTO t_dynamic_dst SETTINGS deduplicate_insert_select = 'enable_even_for_bad_queries' SELECT * FROM t_dynamic_src WHERE id = 1;
SELECT 'Dynamic retry after the merge', count() FROM t_dynamic_dst;

DROP TABLE t_dynamic_dst;
DROP TABLE t_dynamic_src;
DROP TABLE t_json_dst;
DROP TABLE t_json_src;
DROP TABLE t_replicated;
DROP TABLE t_json_same;
DROP TABLE t_json_typed;
DROP TABLE t_json;
DROP TABLE t_map;
DROP TABLE t_tuple;
DROP TABLE t_array;
DROP TABLE t_other_value;
DROP TABLE t_same_rows;
DROP TABLE t_same;
DROP TABLE t_rows;
DROP TABLE t_datetime_ipv4;
DROP TABLE t_bool_int8;
