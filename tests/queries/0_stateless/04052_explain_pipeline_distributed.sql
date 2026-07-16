-- Tags: no-random-settings

-- Serialized query plan is not supported
set serialize_query_plan=0;

set enable_analyzer=1;
set max_threads=2;

-- Basic distributed pipeline explain
explain pipeline distributed=1 select sum(number) from remote('127.0.0.{1,2,3}', numbers(5)) group by bitAnd(number, 3);

select '----------';

-- With header option
explain pipeline header=1, distributed=1 select sum(number) from remote('127.0.0.{1,2}', numbers(5));

select '----------';

-- Verify that distributed + graph is not supported
explain pipeline distributed=1, graph=1 select 1 from remote('127.0.0.{1,2}', numbers(1)); -- { serverError NOT_IMPLEMENTED }

-- Verify that distributed pipeline is not supported with serialized query plan
explain pipeline distributed=1 select sum(number) from remote('127.0.0.{1,2}', numbers(5)) settings serialize_query_plan=1; -- { serverError NOT_IMPLEMENTED }
