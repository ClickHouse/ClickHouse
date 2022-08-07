set send_logs_level = 'error';

DROP TABLE IF EXISTS merge_tree_table1;
CREATE TABLE merge_tree_table1 (`s` LowCardinality(String), x UInt32) ENGINE = MergeTree ORDER BY x settings index_granularity = 1;
CREATE TABLE buffer_table1 ( `s` String , x UInt32) ENGINE = Buffer(currentDatabase(), 'merge_tree_table1', 16, 10, 60, 10, 1000, 1048576, 2097152);
SELECT s FROM buffer_table1;

insert into merge_tree_table1 values ('a', 1);
select s from buffer_table1 where x = 1;
select s from buffer_table1 where x = 2;

DROP TABLE buffer_table1;
DROP TABLE IF EXISTS merge_tree_table1;
