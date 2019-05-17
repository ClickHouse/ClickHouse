CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.defaults;
CREATE TABLE IF NOT EXISTS test.defaults
(
    param1 Float64,
    param2 Float64
) ENGINE = Memory;
insert into test.defaults values (0.0, 0.0), (1.0, 1.0), (0.0, 1.0), (1.0, 0.0)

DROP TABLE IF EXISTS test.model;
CREATE TABLE test.model ENGINE = Memory AS SELECT IncrementalClusteringState(4)(param1, param2) AS state FROM test.defaults;

DROP TABLE IF EXISTS test.answer;
create table test.answer engine = Memory as
select rowNumberInAllBlocks() as row_number, ans from
(with (select state from remote('127.0.0.1', test.model)) as model select evalMLMethod(model, param1, param2) as ans from remote('127.0.0.1', test.defaults));

select row_number from test.answer where ans = (select ans from test.answer where row_number = 0) order by row_number;
select row_number from test.answer where ans = (select ans from test.answer where row_number = 1) order by row_number;
select row_number from test.answer where ans = (select ans from test.answer where row_number = 2) order by row_number;
select row_number from test.answer where ans = (select ans from test.answer where row_number = 3) order by row_number;
