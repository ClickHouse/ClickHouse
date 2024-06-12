-- Tags: distributed

SELECT 'distributed_group_by_no_merge=1';
SELECT count(), uniq(dummy) FROM remote('127.0.0.{2,3}', system.one) SETTINGS distributed_group_by_no_merge=1;
SELECT count(), uniq(dummy) FROM remote('127.0.0.{2,3,4,5}', system.one) SETTINGS distributed_group_by_no_merge=1;
SELECT count(), uniq(dummy) FROM remote('127.0.0.{2,3}', system.one) LIMIT 1 SETTINGS distributed_group_by_no_merge=1;

SELECT 'distributed_group_by_no_merge=2';
SET max_distributed_connections=1;
SET max_threads=1;
-- breaks any(_shard_num)
SET optimize_move_functions_out_of_any=0;

SELECT 'LIMIT';
SELECT * FROM (SELECT any(_shard_num) shard_num, count(), uniq(dummy) FROM remote('127.0.0.{2,3}', system.one)) ORDER BY shard_num LIMIT 1 SETTINGS distributed_group_by_no_merge=2;
SELECT 'OFFSET';
SELECT * FROM (SELECT any(_shard_num) shard_num, count(), uniq(dummy) FROM remote('127.0.0.{2,3}', system.one)) ORDER BY shard_num LIMIT 1, 1 SETTINGS distributed_group_by_no_merge=2;

SELECT 'ALIAS';
SELECT dummy AS d FROM remote('127.0.0.{2,3}', system.one) ORDER BY d SETTINGS distributed_group_by_no_merge=2;

DROP TABLE IF EXISTS data_00184;
CREATE TABLE data_00184 Engine=Memory() AS SELECT * FROM numbers(2);
SELECT 'ORDER BY';
SELECT number FROM remote('127.0.0.{2,3}', currentDatabase(), data_00184) ORDER BY number DESC SETTINGS distributed_group_by_no_merge=2;
SELECT 'ORDER BY LIMIT';
SELECT number FROM remote('127.0.0.{2,3}', currentDatabase(), data_00184) ORDER BY number DESC LIMIT 1 SETTINGS distributed_group_by_no_merge=2;

SELECT 'LIMIT BY';
SELECT number FROM remote('127.0.0.{2,3}', currentDatabase(), data_00184) LIMIT 1 BY number SETTINGS distributed_group_by_no_merge=2;
SELECT 'LIMIT BY LIMIT';
SELECT number FROM remote('127.0.0.{2,3}', currentDatabase(), data_00184) LIMIT 1 BY number LIMIT 1 SETTINGS distributed_group_by_no_merge=2;

SELECT 'GROUP BY ORDER BY';
SELECT uniq(number) u FROM remote('127.0.0.{2,3}', currentDatabase(), data_00184) GROUP BY number ORDER BY u DESC SETTINGS distributed_group_by_no_merge=2;

-- cover possible tricky issues
SELECT 'GROUP BY w/ ALIAS';
SELECT n FROM remote('127.0.0.{2,3}', currentDatabase(), data_00184) GROUP BY number AS n SETTINGS distributed_group_by_no_merge=2;

SELECT 'ORDER BY w/ ALIAS';
SELECT n FROM remote('127.0.0.{2,3}', currentDatabase(), data_00184) ORDER BY number AS n LIMIT 1 SETTINGS distributed_group_by_no_merge=2;

SELECT 'func(aggregate function) GROUP BY';
SELECT assumeNotNull(argMax(dummy, 1)) FROM remote('127.1', system.one) SETTINGS distributed_group_by_no_merge=2;

drop table data_00184;
