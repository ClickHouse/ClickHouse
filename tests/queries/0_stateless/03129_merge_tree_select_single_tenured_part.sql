DROP TABLE IF EXISTS t_single_tenured_part;

CREATE TABLE t_single_tenured_part(s String, v Int64)
ENGINE=ReplacingMergeTree(v) ORDER BY s PARTITION BY length(s);

SYSTEM STOP MERGES t_single_tenured_part;

-- Create two parts that shouldn't be merged
INSERT INTO t_single_tenured_part VALUES ('Hello', 1);
INSERT INTO t_single_tenured_part VALUES ('Hello', 2);

-- With setting only the first part shall be read. Without (default) the setting all parts will be read.
SELECT 'Enable setting';
SELECT s, v FROM t_single_tenured_part ORDER BY v SETTINGS select_only_one_part_with_min_block_per_partition=1;
SELECT 'Default setting';
SELECT s, v FROM t_single_tenured_part ORDER BY v;

-- Optimize table to have just one part
SYSTEM START MERGES t_single_tenured_part;
OPTIMIZE TABLE t_single_tenured_part FINAL;
SELECT count() from system.parts where active and table = 't_single_tenured_part';

-- Make sure that a merged value is returned when setting is enabled
SELECT s, v from t_single_tenured_part ORDER BY v SETTINGS select_only_one_part_with_min_block_per_partition=1;

-- Truncate table and repeat tests to ensure that min_block > 0 is also handled properly
SELECT 'Truncate and repeat';
TRUNCATE TABLE t_single_tenured_part;
SYSTEM STOP MERGES t_single_tenured_part;
INSERT INTO t_single_tenured_part VALUES ('Hello', 3);
INSERT INTO t_single_tenured_part VALUES ('Hello', 4);
SELECT 'Enable setting';
SELECT s, v FROM t_single_tenured_part ORDER BY v SETTINGS select_only_one_part_with_min_block_per_partition=1;
SELECT 'Default setting';
SELECT s, v FROM t_single_tenured_part ORDER BY v;

SYSTEM START MERGES t_single_tenured_part;
SYSTEM START MERGES t_single_tenured_part;
OPTIMIZE TABLE t_single_tenured_part FINAL;
SELECT count() from system.parts where active and table = 't_single_tenured_part';
SELECT s, v from t_single_tenured_part ORDER BY v SETTINGS select_only_one_part_with_min_block_per_partition=1;

DROP TABLE t_single_tenured_part;