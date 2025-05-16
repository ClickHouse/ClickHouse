set allow_suspicious_primary_key = 0;

drop table if exists data;

create table data (key Int, value AggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() order by (key, value); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
create table data (key Int, value SimpleAggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() order by (key, value); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }

create table data (key Int, value AggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() primary key value; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
create table data (key Int, value SimpleAggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() primary key value; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }

create table data (key Int, value AggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() primary key value order by (value, key); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
create table data (key Int, value SimpleAggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() primary key value order by (value, key); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }

set allow_suspicious_primary_key = 1;
create table data (key Int, value SimpleAggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() primary key value order by (value, key);

-- ATTACH should work regardless allow_suspicious_primary_key
set allow_suspicious_primary_key = 0;
detach table data;
attach table data;
drop table data;

-- ALTER AggregatingMergeTree
create table data (key Int) engine=AggregatingMergeTree() order by (key);
alter table data add column value SimpleAggregateFunction(sum, UInt64), modify order by (key, value); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
alter table data add column value SimpleAggregateFunction(sum, UInt64), modify order by (key, value) settings allow_suspicious_primary_key=1;
drop table data;

-- ALTER ReplicatedAggregatingMergeTree
create table data_rep (key Int) engine=ReplicatedAggregatingMergeTree('/tables/{database}', 'r1') order by (key);
alter table data_rep add column value SimpleAggregateFunction(sum, UInt64), modify order by (key, value); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
alter table data_rep add column value SimpleAggregateFunction(sum, UInt64), modify order by (key, value) settings allow_suspicious_primary_key=1;
drop table data_rep;
