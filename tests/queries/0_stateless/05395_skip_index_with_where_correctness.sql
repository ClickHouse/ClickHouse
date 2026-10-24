-- Tags: no-random-settings, no-parallel-replicas, no-replicated-database
-- Partial skip indexes must never change query results. Each case compares the result without skip indexes
-- with the results when the index is analysed before and during reading.

SET allow_experimental_partial_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_type;
DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_layout;
DROP TABLE IF EXISTS t_gate;
DROP TABLE IF EXISTS t_patch;
DROP TABLE IF EXISTS t_tuple_type;
DROP TABLE IF EXISTS t_format_row;
DROP TABLE IF EXISTS t_format_row_alias;
DROP TABLE IF EXISTS t_format_row_names;
DROP TABLE IF EXISTS t_format_row_marker;
DROP TABLE IF EXISTS t_legacy_time;
DROP TABLE IF EXISTS t_json_tuple;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_array_compact;
DROP TABLE IF EXISTS t_ddl_tz;
DROP TABLE IF EXISTS t_enc;
DROP TABLE IF EXISTS t_set;
DROP TABLE IF EXISTS t_ren;
DROP TABLE IF EXISTS t_tz;
DROP TABLE IF EXISTS t_pred;
DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_copy;
DROP TABLE IF EXISTS t_clone;

SELECT 'predicate column type change';
CREATE TABLE t_type (k UInt64, v UInt8, INDEX idx k TYPE minmax WHERE toString(v) = 'true') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_type VALUES (1, 1);
ALTER TABLE t_type MODIFY COLUMN v Bool;
SELECT count() FROM t_type WHERE k = 1 AND toString(v) = 'true' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_type WHERE k = 1 AND toString(v) = 'true' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_type WHERE k = 1 AND toString(v) = 'true' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'lambda in the condition';
CREATE TABLE t_lambda (a Array(UInt8), y UInt8, k UInt8, INDEX idx k TYPE minmax WHERE arrayExists(x -> x = y, a)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT 'function that captures a setting';
CREATE TABLE t_json (k UInt64, j String, INDEX idx k TYPE minmax WHERE JSON_VALUE(j, '$.a') = '[1]') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json VALUES (1, '{"a":[1]}');
SELECT count() FROM t_json WHERE k = 1 AND JSON_VALUE(j, '$.a') = '[1]' SETTINGS use_skip_indexes = 0, function_json_value_return_type_allow_complex = 1;
SELECT count() FROM t_json WHERE k = 1 AND JSON_VALUE(j, '$.a') = '[1]' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, function_json_value_return_type_allow_complex = 1;
SELECT count() FROM t_json WHERE k = 1 AND JSON_VALUE(j, '$.a') = '[1]' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, function_json_value_return_type_allow_complex = 1;
-- With the setting the index was built with, the index is still usable.
SELECT count() FROM t_json WHERE k = 1 AND JSON_VALUE(j, '$.a') = '[1]' SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx', function_json_value_return_type_allow_complex = 0;

SELECT 'ordinary index replaced by a partial one under the same name';
CREATE TABLE t_layout (id UInt64, flag UInt8, INDEX idx id TYPE minmax) ENGINE = MergeTree ORDER BY tuple() PARTITION BY intDiv(id, 1000);
INSERT INTO t_layout VALUES (256, 1), (300, 0);
ALTER TABLE t_layout DETACH PARTITION ID '0';
ALTER TABLE t_layout DROP INDEX idx;
ALTER TABLE t_layout ADD INDEX idx id TYPE minmax WHERE flag = 1;
ALTER TABLE t_layout ATTACH PARTITION ID '0';
SELECT count() FROM t_layout WHERE flag = 1 AND id = 256 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_layout WHERE flag = 1 AND id = 256 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_layout WHERE flag = 1 AND id = 256 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'partial index replaced by an ordinary one under the same name';
TRUNCATE TABLE t_layout;
INSERT INTO t_layout VALUES (256, 1), (300, 0);
ALTER TABLE t_layout DETACH PARTITION ID '0';
ALTER TABLE t_layout DROP INDEX idx;
ALTER TABLE t_layout ADD INDEX idx id TYPE minmax;
ALTER TABLE t_layout ATTACH PARTITION ID '0';
SELECT count() FROM t_layout WHERE id = 300 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_layout WHERE id = 300 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_layout WHERE id = 300 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'condition evaluated under another session time zone when the part is written';
CREATE TABLE t_tz (id UInt64, s String, INDEX idx id TYPE minmax WHERE toUnixTimestamp(toDateTime(s)) = 1714953600) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_tz SELECT 1, '2024-05-06 00:00:00' SETTINGS session_timezone = 'Asia/Tokyo';
SET session_timezone = 'UTC';
SELECT count() FROM t_tz WHERE id = 1 AND toUnixTimestamp(toDateTime(s)) = 1714953600 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_tz WHERE id = 1 AND toUnixTimestamp(toDateTime(s)) = 1714953600 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_tz WHERE id = 1 AND toUnixTimestamp(toDateTime(s)) = 1714953600 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
-- A query that evaluates the condition the way the part was written may use the index.
SELECT count() FROM t_tz WHERE id = 1 AND toUnixTimestamp(toDateTime(s)) = 1714953600 SETTINGS session_timezone = 'Asia/Tokyo', use_skip_indexes = 0;
SELECT count() FROM t_tz WHERE id = 1 AND toUnixTimestamp(toDateTime(s)) = 1714953600 SETTINGS session_timezone = 'Asia/Tokyo', use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_tz WHERE id = 1 AND toUnixTimestamp(toDateTime(s)) = 1714953600 SETTINGS session_timezone = 'Asia/Tokyo', use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
-- A merge rebuilds the index under the time zone it runs with, and the index is usable again.
INSERT INTO t_tz SELECT 2, '2024-05-06 00:00:00' SETTINGS session_timezone = 'UTC';
OPTIMIZE TABLE t_tz FINAL;
SELECT count() FROM t_tz WHERE id = 1 AND toUnixTimestamp(toDateTime(s)) = 1714953600 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_tz WHERE id = 1 AND toUnixTimestamp(toDateTime(s)) = 1714953600 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_tz WHERE id = 1 AND toUnixTimestamp(toDateTime(s)) = 1714953600 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SELECT count() FROM t_tz WHERE id = 2 AND toUnixTimestamp(toDateTime(s)) = 1714953600 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SET session_timezone = '';

SELECT 'one partial condition replaced by another under the same name';
CREATE TABLE t_pred (id UInt64, flag UInt8, INDEX idx id TYPE minmax WHERE flag = 0) ENGINE = MergeTree ORDER BY tuple() PARTITION BY intDiv(id, 1000);
INSERT INTO t_pred VALUES (256, 1);
ALTER TABLE t_pred DETACH PARTITION ID '0';
ALTER TABLE t_pred DROP INDEX idx;
ALTER TABLE t_pred ADD INDEX idx id TYPE minmax WHERE flag = 1;
ALTER TABLE t_pred ATTACH PARTITION ID '0';
SELECT count() FROM t_pred WHERE id = 256 AND flag = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_pred WHERE id = 256 AND flag = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_pred WHERE id = 256 AND flag = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
-- Materializing builds the index for the new condition.
ALTER TABLE t_pred MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT count() FROM t_pred WHERE id = 256 AND flag = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM t_pred WHERE id = 256 AND flag = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'idx';

SELECT 'set index that overflows in some granules';
CREATE TABLE t_set (id UInt64, p UInt8, v UInt32, INDEX b v TYPE set(2) GRANULARITY 1 WHERE p = 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
INSERT INTO t_set SELECT number, 1, if(intDiv(number, 4) % 3 = 1, 7, number) FROM numbers(24);
SELECT count() FROM t_set WHERE p = 1 AND v = 7 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_set WHERE p = 1 AND v = 7 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_set WHERE p = 1 AND v = 7 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SELECT count() FROM t_set WHERE p = 1 AND v < 1000 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_set WHERE p = 1 AND v < 1000 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_set WHERE p = 1 AND v < 1000 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'MATERIALIZE INDEX rebuilds an index left stale by a rename of the condition column';
CREATE TABLE t_ren (id UInt64, p UInt8, v UInt32, INDEX i v TYPE minmax GRANULARITY 1 WHERE p = 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
INSERT INTO t_ren SELECT number, number % 2, number FROM numbers(40);
ALTER TABLE t_ren RENAME COLUMN p TO pp SETTINGS mutations_sync = 2;
SELECT count() FROM t_ren WHERE pp = 1 AND v = 11 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ren WHERE pp = 1 AND v = 11 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
ALTER TABLE t_ren MATERIALIZE INDEX i SETTINGS mutations_sync = 2;
SELECT count() FROM t_ren WHERE pp = 1 AND v = 11 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
SELECT count() FROM t_ren WHERE pp = 1 AND v = 11 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'i';

SELECT 'condition analysed under another time zone than the one a query runs in';
SET session_timezone = 'Asia/Tokyo';
CREATE TABLE t_ddl_tz (id UInt64, INDEX idx id TYPE minmax WHERE id = toUnixTimestamp(toDateTime('2024-05-06 00:00:00'))) ENGINE = MergeTree ORDER BY tuple();
SET session_timezone = '';
-- 1714921200 is what the condition folds to in Tokyo; in the time zone of the server the part is written for a
-- different value, so the stored condition must not be matched by a query in that time zone.
INSERT INTO t_ddl_tz VALUES (1714921200);
SELECT count() FROM t_ddl_tz WHERE id = 1714921200 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ddl_tz WHERE id = 1714921200 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_ddl_tz WHERE id = 1714921200 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'partial minmax index redefined as a set index under the same name';
CREATE TABLE t_enc (id UInt64, flag UInt8, INDEX idx id TYPE minmax WHERE flag = 1) ENGINE = MergeTree ORDER BY tuple() PARTITION BY intDiv(id, 1000);
INSERT INTO t_enc VALUES (1, 1), (2, 1);
ALTER TABLE t_enc DETACH PARTITION ID '0';
ALTER TABLE t_enc DROP INDEX idx;
ALTER TABLE t_enc ADD INDEX idx id TYPE set(10) WHERE flag = 1;
ALTER TABLE t_enc ATTACH PARTITION ID '0';
SELECT count() FROM t_enc WHERE id = 1 AND flag = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_enc WHERE id = 1 AND flag = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_enc WHERE id = 1 AND flag = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
-- The same type with other parameters is another definition too.
ALTER TABLE t_enc MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
ALTER TABLE t_enc DETACH PARTITION ID '0';
ALTER TABLE t_enc DROP INDEX idx;
ALTER TABLE t_enc ADD INDEX idx id TYPE set(1) WHERE flag = 1;
ALTER TABLE t_enc ATTACH PARTITION ID '0';
SELECT count() FROM t_enc WHERE id = 1 AND flag = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_enc WHERE id = 2 AND flag = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'function that captures a setting: named tuple from JSON';
CREATE TABLE t_json_tuple (id UInt64, j String, INDEX i id TYPE minmax WHERE JSONExtract(j, 'Tuple(x String, y String)') = tuple('', '')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_tuple VALUES (1, '["a","b"]');
SELECT count() FROM t_json_tuple WHERE id = 1 AND JSONExtract(j, 'Tuple(x String, y String)') = tuple('', '') SETTINGS json_extract_named_tuples_as_objects = 1, use_skip_indexes = 0;
SELECT count() FROM t_json_tuple WHERE id = 1 AND JSONExtract(j, 'Tuple(x String, y String)') = tuple('', '') SETTINGS json_extract_named_tuples_as_objects = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_json_tuple WHERE id = 1 AND JSONExtract(j, 'Tuple(x String, y String)') = tuple('', '') SETTINGS json_extract_named_tuples_as_objects = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
-- With the setting the index was defined with, the index is used and agrees.
SELECT count() FROM t_json_tuple WHERE id = 1 AND JSONExtract(j, 'Tuple(x String, y String)') = tuple('', '') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_tuple WHERE id = 1 AND JSONExtract(j, 'Tuple(x String, y String)') = tuple('', '') SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
SELECT count() FROM t_json_tuple WHERE id = 1 AND JSONExtract(j, 'Tuple(x String, y String)') = tuple('', '') SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'i';

SELECT 'function that captures format settings: formatRow';
CREATE TABLE t_format_row (k UInt64, n UInt64, INDEX idx k TYPE minmax WHERE formatRow('JSONEachRow', n) = '{"n":"1"}\n') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_format_row VALUES (1, 1);
SELECT count() FROM t_format_row WHERE k = 1 AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 0;
SELECT count() FROM t_format_row WHERE k = 1 AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_format_row WHERE k = 1 AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'formatRow field names given by aliases';
CREATE TABLE t_format_row_alias (k UInt64, n UInt64, INDEX idx k TYPE minmax WHERE formatRow('JSONEachRow', n) = '{"x":1}\n') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_format_row_alias VALUES (1, 1);
SELECT count() FROM t_format_row_alias WHERE k = 1 AND formatRow('JSONEachRow', n AS x) = '{"x":1}\n' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_format_row_alias WHERE k = 1 AND formatRow('JSONEachRow', n AS x) = '{"x":1}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_format_row_alias WHERE k = 1 AND formatRow('JSONEachRow', n AS x) = '{"x":1}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'formatRow field names whose bytes run together the same way';
CREATE TABLE t_format_row_names (k UInt64, ab UInt8, c UInt8, INDEX idx k TYPE minmax WHERE formatRow('JSONEachRow', ab, c) = '{"a":1,"bc":2}\n') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_format_row_names VALUES (1, 1, 2);
SELECT count() FROM t_format_row_names WHERE k = 1 AND formatRow('JSONEachRow', ab AS a, c AS bc) = '{"a":1,"bc":2}\n' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_format_row_names WHERE k = 1 AND formatRow('JSONEachRow', ab AS a, c AS bc) = '{"a":1,"bc":2}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_format_row_names WHERE k = 1 AND formatRow('JSONEachRow', ab AS a, c AS bc) = '{"a":1,"bc":2}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'formatRow field names given by aliases in the index condition are rejected';
CREATE TABLE t_format_row_marker (k UInt64, ab UInt8, c UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_format_row_marker ADD INDEX idx k TYPE minmax WHERE formatRow('JSONEachRow', ab AS a, c AS bc) = '{"a":1,"bc":2}\n'; -- { serverError BAD_ARGUMENTS }

SELECT 'setting read from the query that writes the part: use_legacy_to_time';
SET enable_time_time64_type = 1;
CREATE TABLE t_legacy_time (k UInt64, dt DateTime('UTC'), INDEX idx k TYPE minmax WHERE toString(toTime(dt)) = '10:00:00') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_legacy_time SETTINGS use_legacy_to_time = 1 VALUES (1, '2020-01-01 10:00:00');
SELECT count() FROM t_legacy_time WHERE k = 1 AND toString(toTime(dt)) = '10:00:00' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_legacy_time WHERE k = 1 AND toString(toTime(dt)) = '10:00:00' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_legacy_time WHERE k = 1 AND toString(toTime(dt)) = '10:00:00' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'update of the parent column of a subcolumn the condition reads';
CREATE TABLE t_array (id UInt64, p Array(UInt8), INDEX i id TYPE minmax GRANULARITY 1 WHERE p.size0 > 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_array VALUES (1, [1]);
ALTER TABLE t_array UPDATE p = [1, 2] WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT count() FROM t_array WHERE id = 1 AND p.size0 > 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_array WHERE id = 1 AND p.size0 > 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_array WHERE id = 1 AND p.size0 > 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
CREATE TABLE t_array_compact (id UInt64, p Array(UInt8), INDEX i id TYPE minmax GRANULARITY 1 WHERE p.size0 > 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1, min_bytes_for_wide_part = '100G';
INSERT INTO t_array_compact VALUES (1, [1]);
ALTER TABLE t_array_compact UPDATE p = [1, 2] WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT count() FROM t_array_compact WHERE id = 1 AND p.size0 > 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_array_compact WHERE id = 1 AND p.size0 > 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_array_compact WHERE id = 1 AND p.size0 > 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'subcolumn index, compact part with a patch part and a DELETE';
CREATE TABLE t_patch (id UInt64, w UInt32, p Array(UInt32), INDEX i p.size0 TYPE minmax GRANULARITY 1, INDEX j id TYPE minmax GRANULARITY 1 WHERE p.size0 > 1)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '1G', index_granularity = 2, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_patch SELECT number, 0, range(number % 4) FROM numbers(10);
UPDATE t_patch SET p = [1, 2, 3] WHERE id % 2 = 1;
ALTER TABLE t_patch DELETE WHERE id % 4 = 0 SETTINGS mutations_sync = 2;
SELECT count() FROM t_patch WHERE p.size0 = 3 AND id < 100 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_patch WHERE p.size0 = 3 AND id < 100 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_patch WHERE p.size0 = 3 AND id < 100 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'type of the column a subcolumn index reads is changed';
CREATE TABLE t_tuple_type (id UInt64, tt Tuple(a UInt32, b String), INDEX i tt.a TYPE minmax GRANULARITY 1, INDEX j id TYPE minmax GRANULARITY 1 WHERE tt.a > 2)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 3;
INSERT INTO t_tuple_type SELECT number, (number % 5 + 3, 'x') FROM numbers(40);
ALTER TABLE t_tuple_type MODIFY COLUMN tt Tuple(a Float32, b String) SETTINGS mutations_sync = 2;
SELECT count() FROM t_tuple_type WHERE tt.a = 5 AND tt.a > 2 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_tuple_type WHERE tt.a = 5 AND tt.a > 2 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_tuple_type WHERE tt.a = 5 AND tt.a > 2 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'experimental gate';
SET allow_experimental_partial_skip_indexes = 0;
CREATE TABLE t_gate_create (x UInt64, f UInt8, INDEX i x TYPE minmax WHERE f = 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_gate (x UInt64, f UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_gate ADD INDEX i x TYPE minmax WHERE f = 1; -- { serverError SUPPORT_IS_DISABLED }
CREATE INDEX i ON t_gate (x) TYPE minmax WHERE f = 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_gate ADD INDEX i x TYPE minmax;
ALTER TABLE t_gate DROP INDEX i;
SET allow_experimental_partial_skip_indexes = 1;
ALTER TABLE t_gate ADD INDEX i x TYPE minmax WHERE f = 1;
CREATE INDEX j ON t_gate (x) TYPE set(10) WHERE f = 1;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_gate' ORDER BY name;
-- `IF NOT EXISTS` of a partial index that is already there is not a new definition.
SET allow_experimental_partial_skip_indexes = 0;
ALTER TABLE t_gate ADD INDEX IF NOT EXISTS i x TYPE minmax;
SET allow_experimental_partial_skip_indexes = 1;
SET allow_experimental_partial_skip_indexes = 1;
CREATE TABLE t_src (x UInt64, f UInt8, INDEX i x TYPE minmax WHERE f = 1) ENGINE = MergeTree ORDER BY tuple();
SET allow_experimental_partial_skip_indexes = 0;
CREATE TABLE t_copy AS t_src; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_clone CLONE AS t_src; -- { serverError SUPPORT_IS_DISABLED }
-- Stored definitions are not affected: the table is reattached.
DETACH TABLE t_src;
ATTACH TABLE t_src;
SET allow_experimental_partial_skip_indexes = 1;
CREATE TABLE t_copy AS t_src;
CREATE TABLE t_clone CLONE AS t_src;
SELECT table, name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table IN ('t_src', 't_copy', 't_clone') ORDER BY table;

DROP TABLE t_type;
DROP TABLE t_json;
DROP TABLE t_layout;
DROP TABLE t_gate;
DROP TABLE t_patch;
DROP TABLE t_tuple_type;
DROP TABLE t_format_row;
DROP TABLE t_format_row_alias;
DROP TABLE t_format_row_names;
DROP TABLE t_format_row_marker;
DROP TABLE t_legacy_time;
DROP TABLE t_json_tuple;
DROP TABLE t_array;
DROP TABLE t_array_compact;
DROP TABLE t_ddl_tz;
DROP TABLE t_enc;
DROP TABLE t_set;
DROP TABLE t_ren;
DROP TABLE t_tz;
DROP TABLE t_pred;
DROP TABLE t_src;
DROP TABLE t_copy;
DROP TABLE t_clone;
