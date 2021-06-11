DROP TABLE IF EXISTS ES;

create table ES(A String) Engine=MergeTree order by tuple();
insert into ES select toString(number) from numbers(10000000);

SET max_execution_time = 100, max_execution_speed = 1000000;
SET max_threads = 1;
SET max_block_size = 1000000;

-- Exception about execution speed is not thrown from these queries.
SELECT * FROM ES LIMIT 1 format Null;
SELECT * FROM ES LIMIT 10 format Null;
SELECT * FROM ES LIMIT 100 format Null;
SELECT * FROM ES LIMIT 1000 format Null;
SELECT * FROM ES LIMIT 10000 format Null;
SELECT * FROM ES LIMIT 100000 format Null;
SELECT * FROM ES LIMIT 1000000 format Null;

DROP TABLE ES;
