drop table if exists buffer_01733;
drop table if exists data_01733;

create table data_01733   (a UInt64, b AggregateFunction(uniqCombined64, String)) engine=AggregatingMergeTree order by a;
create table buffer_01733 (a UInt64, b AggregateFunction(uniqCombined64, String)) engine=Buffer(currentDatabase(), data_01733, 1, 60, 600, 100000000, 1000000000, 300000000, 1000000000);

-- two INSERTs is enough to trigger the leak in ColumnAggregateFunction,
-- due to not every data will be indexed in copied_data_info:
-- data.size() = 2
-- copied_data_info.size() = 1
insert into buffer_01733 select any(number) a, uniqCombined64State(toString(number)) b from numbers(1e5);
insert into buffer_01733 select any(number) a, uniqCombined64State(toString(number)) b from numbers(1e5);

optimize table buffer_01733;

drop table buffer_01733;
drop table data_01733;
