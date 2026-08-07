drop table if exists data_01409;
create table data_01409 engine=Memory as select * from numbers(20);

-- easier to check merging via distributed tables
-- but can be done vai topKMerge(topKState()) as well

select 'AggregateFunctionTopK';
select length(topK(20)(number)) from remote('127.{1,1}', currentDatabase(), data_01409);
select length(topKWeighted(20)(number, 1)) from remote('127.{1,1}', currentDatabase(), data_01409);

select 'AggregateFunctionTopKGenericData';
select length(topK(20)((number, ''))) from remote('127.{1,1}', currentDatabase(), data_01409);
select length(topKWeighted(20)((number, ''), 1)) from remote('127.{1,1}', currentDatabase(), data_01409);

drop table data_01409;
