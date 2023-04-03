-- { echoOn }
-- first row is less than WITH FILL FROM
-- select number from numbers(0, 1) order by number with fill from 1;

-- first row is bigger than WITH FILL FROM
-- select number from numbers(1, 1) order by number with fill from 0

-- sensor table
drop table if exists ts;
create table ts (sensor_id UInt64, timestamp UInt64, value Float64) ENGINE=MergeTree()  ORDER BY (sensor_id, timestamp);
insert into ts VALUES (1, 10, 1), (1, 12, 2), (3, 5, 1), (3, 7, 3), (5, 1, 1), (5, 3, 1);
-- FillingTransform: 6 rows will be processed in 1 chunks
select * from ts order by sensor_id, timestamp with fill step 1;

drop table if exists ts;
create table ts (sensor_id UInt64, timestamp UInt64, value Float64) ENGINE=MergeTree()  ORDER BY (sensor_id, timestamp);
system stop merges ts;
-- FillingTransform: 6 rows will be processed in 2 chunks with 3 rows each
insert into ts VALUES (1, 10, 1), (1, 12, 1), (3, 5, 1);
insert into ts VALUES (3, 7, 1), (5, 1, 1), (5, 3, 1);
select * from ts order by sensor_id, timestamp with fill step 1 settings max_block_size=3;

drop table if exists ts;
create table ts (sensor_id UInt64, timestamp UInt64, value Float64) ENGINE=MergeTree()  ORDER BY (sensor_id, timestamp);
system stop merges ts;
-- FillingTransform: 6 rows will be processed in 3 chunks with 2 rows each
insert into ts VALUES (1, 10, 1), (1, 12, 1);
insert into ts VALUES (3, 5, 1), (3, 7, 1);
insert into ts VALUES (5, 1, 1), (5, 3, 1);
select * from ts order by sensor_id, timestamp with fill step 1 settings max_block_size=2;
