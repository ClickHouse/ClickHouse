-- Tags: log-engine
-- log-engine: the defect only exists in the Log family, so replacing the engine with MergeTree makes the test vacuous.

DROP TABLE IF EXISTS t_zero_byte_first;
DROP TABLE IF EXISTS t_exhausted_first;
DROP TABLE IF EXISTS t_zero_byte_later;
DROP TABLE IF EXISTS t_zero_byte_map;
DROP TABLE IF EXISTS t_issue_1;
DROP TABLE IF EXISTS t_issue_1_log;
DROP TABLE IF EXISTS t_issue_2;
DROP TABLE IF EXISTS t_issue_3;
DROP TABLE IF EXISTS t_issue_3_log;
DROP TABLE IF EXISTS t_statements;
DROP TABLE IF EXISTS t_lc_array;
DROP TABLE IF EXISTS t_lc_array_exhausted_first;
DROP TABLE IF EXISTS t_lc_map;
DROP TABLE IF EXISTS t_lc_first;

-- An array of only empty arrays writes no elements at all, so `b.bin` is empty while `b.size0.bin` holds
-- the 10 rows; `b.bin` is also the first data file by name.
CREATE TABLE t_zero_byte_first (id Int64, g Int64, b Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_zero_byte_first (id) SELECT number FROM numbers(10);
SELECT count(), sum(id), sum(length(b)) FROM t_zero_byte_first WHERE NOT ignore(*) SETTINGS max_block_size = 3;
SELECT count() FROM t_zero_byte_first WHERE NOT ignore(b) SETTINGS max_block_size = 3;

-- The elements file is not empty here, it just runs out first: only the leading rows hold elements,
-- so `b.bin` is at its end once row 3 has been read while `b.size0.bin` still holds rows 4 to 10.
CREATE TABLE t_exhausted_first (id Int64, g Int64, b Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_exhausted_first SELECT number, number, if(number < 3, [number], []) FROM numbers(10);
SELECT count(), sum(id), sum(length(b)) FROM t_exhausted_first WHERE NOT ignore(*) SETTINGS max_block_size = 3;

-- Control: the same types, named so that a file with one entry per row comes first.
CREATE TABLE t_zero_byte_later (id Int64, b Int64, g Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_zero_byte_later (id) SELECT number FROM numbers(10);
SELECT count(), sum(id) FROM t_zero_byte_later WHERE NOT ignore(*) SETTINGS max_block_size = 3;

-- A map of only empty maps writes neither keys nor values, and `a%2Ekeys.bin` also comes first by name.
CREATE TABLE t_zero_byte_map (id Int64, a Map(String, UInt64)) ENGINE = TinyLog;
INSERT INTO t_zero_byte_map (id) SELECT number FROM numbers(10);
SELECT count(), sum(id) FROM t_zero_byte_map WHERE NOT ignore(*) SETTINGS max_block_size = 3;

-- The cases from https://github.com/ClickHouse/ClickHouse/issues/120255, with 10 rows read 3 at a time
-- instead of 100000 rows read 65409 at a time.
CREATE TABLE t_issue_1 (id Int64, lifecycle_key Int64, ab_testing_data_object_ids Array(UInt64), ab_testing_proxy_group_ids Array(UInt64), ab_testing_experiment_versions Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_issue_1 (id) SELECT number FROM numbers(10);
SELECT count() FROM t_issue_1 SETTINGS max_block_size = 3;
CREATE TABLE t_issue_1_log AS t_issue_1 ENGINE = Log;
INSERT INTO t_issue_1_log SELECT * FROM t_issue_1 SETTINGS max_block_size = 3;
SELECT count() FROM t_issue_1_log;

CREATE TABLE t_issue_2 (id Int64, ab_testing_data_object_ids Array(UInt64), ab_testing_proxy_group_ids Array(UInt64), ab_testing_experiment_versions Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_issue_2 (id) SELECT number FROM numbers(10);
SELECT count() FROM t_issue_2 SETTINGS max_block_size = 3;

CREATE TABLE t_issue_3 (id Int64, a Int64, ab_testing_data_object_ids Array(UInt64), ab_testing_proxy_group_ids Array(UInt64), ab_testing_experiment_versions Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_issue_3 (id) SELECT number FROM numbers(10);
SELECT count() FROM t_issue_3 SETTINGS max_block_size = 3;
CREATE TABLE t_issue_3_log AS t_issue_3 ENGINE = Log;
INSERT INTO t_issue_3_log SELECT * FROM t_issue_3 SETTINGS max_block_size = 3;
SELECT count() FROM t_issue_3_log;

-- The Log family has no mutations, so DELETE and UPDATE are rejected and every row stays readable.
-- TRUNCATE removes the data files; the two inserts after it append to the same files.
CREATE TABLE t_statements (id Int64, g Int64, b Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_statements (id) SELECT number FROM numbers(10);
DELETE FROM t_statements WHERE id < 5; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_statements DELETE WHERE id < 5; -- { serverError NOT_IMPLEMENTED }
UPDATE t_statements SET g = 1 WHERE 1; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_statements UPDATE g = 1 WHERE 1; -- { serverError NOT_IMPLEMENTED }
SELECT count(), sum(id) FROM t_statements WHERE NOT ignore(*) SETTINGS max_block_size = 3;
TRUNCATE TABLE t_statements;
SELECT count() FROM t_statements WHERE NOT ignore(*);
INSERT INTO t_statements SELECT number, number, if(number < 3, [number], []) FROM numbers(10);
INSERT INTO t_statements SELECT number, number, if(number < 3, [number], []) FROM numbers(10);
SELECT count(), sum(id), sum(length(b)) FROM t_statements WHERE NOT ignore(*) SETTINGS max_block_size = 3;

-- LowCardinality: `b.dict.bin` holds only the dictionary header, which is read before the first row.
CREATE TABLE t_lc_array (id Int64, g Int64, b Array(LowCardinality(String))) ENGINE = TinyLog;
INSERT INTO t_lc_array (id) SELECT number FROM numbers(10);
SELECT count(), sum(id), sum(length(b)) FROM t_lc_array WHERE NOT ignore(*) SETTINGS max_block_size = 3;

CREATE TABLE t_lc_array_exhausted_first (id Int64, g Int64, b Array(LowCardinality(String))) ENGINE = TinyLog;
INSERT INTO t_lc_array_exhausted_first SELECT number, number, if(number < 3, [toString(number)], []) FROM numbers(10);
SELECT count(), countIf(b = if(id < 3, [toString(id)], [])) FROM t_lc_array_exhausted_first WHERE NOT ignore(*) SETTINGS max_block_size = 3;

CREATE TABLE t_lc_map (id Int64, a Map(LowCardinality(String), UInt64)) ENGINE = TinyLog;
INSERT INTO t_lc_map (id) SELECT number FROM numbers(10);
SELECT count(), sum(id) FROM t_lc_map WHERE NOT ignore(*) SETTINGS max_block_size = 3;

-- Control: a LowCardinality column declared and named first, whose index file holds one entry per row.
CREATE TABLE t_lc_first (a LowCardinality(String), id Int64, b Array(UInt64)) ENGINE = TinyLog;
INSERT INTO t_lc_first SELECT toString(number % 3), number, [] FROM numbers(10);
SELECT count(), countIf(a = toString(id % 3)) FROM t_lc_first WHERE NOT ignore(*) SETTINGS max_block_size = 3;

DROP TABLE t_zero_byte_first;
DROP TABLE t_exhausted_first;
DROP TABLE t_zero_byte_later;
DROP TABLE t_zero_byte_map;
DROP TABLE t_issue_1;
DROP TABLE t_issue_1_log;
DROP TABLE t_issue_2;
DROP TABLE t_issue_3;
DROP TABLE t_issue_3_log;
DROP TABLE t_statements;
DROP TABLE t_lc_array;
DROP TABLE t_lc_array_exhausted_first;
DROP TABLE t_lc_map;
DROP TABLE t_lc_first;
