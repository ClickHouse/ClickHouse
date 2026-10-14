-- Tags: no-random-settings, no-parallel-replicas
-- Partial skip indexes must never change query results. Each case compares the result without skip indexes
-- with the results when the index is analysed before and during reading.

SET allow_experimental_partial_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_type;
DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_layout;
DROP TABLE IF EXISTS t_gate;
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
DROP TABLE t_set;
DROP TABLE t_ren;
DROP TABLE t_tz;
DROP TABLE t_pred;
DROP TABLE t_src;
DROP TABLE t_copy;
DROP TABLE t_clone;
