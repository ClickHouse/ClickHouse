set send_logs_level = 'error';

DROP TABLE IF EXISTS merge_tree_table1;
CREATE TABLE merge_tree_table1 (`s` LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE buffer_table1 ( `s` String ) ENGINE = Buffer(currentDatabase(), 'merge_tree_table1', 16, 10, 60, 10, 1000, 1048576, 2097152);
SELECT * FROM buffer_table1;

DROP TABLE IF EXISTS merge_tree_table1;
