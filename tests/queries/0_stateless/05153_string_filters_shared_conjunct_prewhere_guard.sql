-- Tags: no-fasttest
-- no-fasttest: the `File(Parquet)` case below needs the Parquet format, which the fast-test
-- build does not have.
-- The scan-time string filter (`apply_string_filters_during_scan`) requires all conditions of a column, so with
-- conditions from two predicates a value that matches only one of them is replaced by an empty string, and that
-- predicate becomes false for it. Its result must not be read by anything else in PREWHERE: here `throwIf` would
-- fire, although every value contains 'needle-one'.

DROP TABLE IF EXISTS t_string_filter_shared;
DROP TABLE IF EXISTS t_string_filter_shared_parquet;

-- The string filters are not applied when the read columns may be written to the columns cache
-- (it is randomized in tests), and the test checks that they are applied.
SET use_columns_cache = 0;

-- With parallel replicas the `INSERT INTO ... SELECT` into the `File` table would be executed by several
-- replicas writing the same file, and the reads would be done by the replicas, which makes the
-- `ProfileEvents` assertion at the end depend on where the scan happened.
SET enable_parallel_replicas = 0;

CREATE TABLE t_string_filter_shared (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

-- Every value contains 'needle-one', every tenth value also contains 'needle-two'.
INSERT INTO t_string_filter_shared
SELECT number, concat('needle-one ', repeat('x', 30), ' ', toString(number), if(number % 10 = 0, ' needle-two', ''))
FROM numbers(10000);

CREATE TABLE t_string_filter_shared_parquet (id UInt32, s String) ENGINE = File(Parquet);

-- Without a dictionary the values are decoded one by one, and the filter is applied to them.
INSERT INTO t_string_filter_shared_parquet SETTINGS output_format_parquet_max_dictionary_size = 0
SELECT * FROM t_string_filter_shared;

SET apply_string_filters_during_scan = 1;

SELECT 'shared predicate';
SELECT count() FROM t_string_filter_shared
PREWHERE throwIf(NOT (s LIKE '%needle-one%')) = 0 AND s LIKE '%needle-one%' AND s LIKE '%needle-two%'
SETTINGS log_comment = '05153_shared_conjunct_guard';

SELECT 'shared predicate via an alias';
WITH s LIKE '%needle-one%' AS a
SELECT count() FROM t_string_filter_shared
PREWHERE throwIf(NOT a) = 0 AND a AND s LIKE '%needle-two%'
SETTINGS log_comment = '05153_shared_conjunct_guard';

SELECT 'shared position';
SELECT count() FROM t_string_filter_shared
PREWHERE throwIf(position(s, 'needle-one') = 0) = 0 AND position(s, 'needle-one') > 0 AND s LIKE '%needle-two%'
SETTINGS log_comment = '05153_shared_conjunct_guard';

SELECT 'shared conjunction';
WITH (s LIKE '%needle-one%' AND id >= 0) AS b
SELECT count() FROM t_string_filter_shared
PREWHERE throwIf(NOT b) = 0 AND b AND s LIKE '%needle-two%'
SETTINGS log_comment = '05153_shared_conjunct_guard';

SELECT 'shared predicate, Parquet';
SELECT count() FROM t_string_filter_shared_parquet
PREWHERE throwIf(NOT (s LIKE '%needle-one%')) = 0 AND s LIKE '%needle-one%' AND s LIKE '%needle-two%'
SETTINGS log_comment = '05153_shared_conjunct_guard';

-- Controls: the filter is still applied when the predicates are read only by the conjunction,
-- and when a predicate with a single condition source is read elsewhere (it cannot change on the replaced value).
SELECT 'control, two predicates';
SELECT count() FROM t_string_filter_shared
PREWHERE s LIKE '%needle-one%' AND s LIKE '%needle-two%'
SETTINGS log_comment = '05153_shared_conjunct_control_two';

SELECT 'control, position comparison and another predicate';
SELECT count() FROM t_string_filter_shared
PREWHERE position(s, 'needle-one') > 0 AND s LIKE '%needle-two%'
SETTINGS log_comment = '05153_shared_conjunct_control_position';

SELECT 'control, one predicate read elsewhere';
SELECT count() FROM t_string_filter_shared
PREWHERE position(s, 'needle-two') > 0 AND position(s, 'needle-two') < 1000
SETTINGS log_comment = '05153_shared_conjunct_control_one';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, sum(ProfileEvents['StringValueFilterValuesChecked']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05153_shared_conjunct_%'
GROUP BY log_comment ORDER BY log_comment;

DROP TABLE t_string_filter_shared;
DROP TABLE t_string_filter_shared_parquet;
