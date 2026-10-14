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

DROP TABLE t_type;
DROP TABLE t_json;
DROP TABLE t_layout;
DROP TABLE t_gate;
