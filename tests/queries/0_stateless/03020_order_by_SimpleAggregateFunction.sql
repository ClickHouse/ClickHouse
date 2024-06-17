set allow_suspicious_primary_key = 0;

DROP TABLE IF EXISTS data;

create table data (key Int, value AggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() order by (key, value); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
create table data (key Int, value SimpleAggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() order by (key, value); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }

create table data (key Int, value AggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() primary key value; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
create table data (key Int, value SimpleAggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() primary key value; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }

create table data (key Int, value AggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() primary key value order by (value, key); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
create table data (key Int, value SimpleAggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() primary key value order by (value, key); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }

set allow_suspicious_primary_key = 1;

create table data (key Int, value SimpleAggregateFunction(sum, UInt64)) engine=AggregatingMergeTree() primary key value order by (value, key);

DROP TABLE data;
