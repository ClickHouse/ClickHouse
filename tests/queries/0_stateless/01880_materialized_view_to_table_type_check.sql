DROP TABLE IF EXISTS test_mv;
DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test_input;

CREATE TABLE test_input(id Int32) ENGINE=MergeTree() order by id; 

CREATE TABLE test(`id` Int32, `pv` AggregateFunction(sum, Int32)) ENGINE = AggregatingMergeTree() ORDER BY id;

CREATE MATERIALIZED VIEW test_mv to test(`id` Int32, `pv` AggregateFunction(sum, Int32)) as SELECT id, sumState(1) as pv from test_input group by id; -- { serverError CANNOT_CONVERT_TYPE } 

INSERT INTO test_input SELECT toInt32(number % 1000) AS id FROM numbers(10);
select '----------test--------:';
select * from test;

create MATERIALIZED VIEW test_mv to test(`id` Int32, `pv` AggregateFunction(sum, Int32)) as SELECT id, sumState(toInt32(1)) as pv from test_input group by id; 
INSERT INTO test_input SELECT toInt32(number % 1000) AS id FROM numbers(100,3);

select '----------test--------:';
select * from test;

DROP TABLE test_mv;
DROP TABLE test;
DROP TABLE test_input;
