-- { echo ON }

drop table if exists x;

create table x (dt DateTime, i Int32 default 42 ttl dt + toIntervalDay(1), index idx(i) type set(100)) engine MergeTree partition by indexHint(dt) order by dt settings index_granularity = 8192, min_bytes_for_wide_part = 0;

system stop merges x;

insert into x values (now() - toIntervalDay(30), 1);

select i from x where i = 1;

system start merges x;

optimize table x final;

-- Run OPTIMIZE twice to ensure the second merge is triggered, as the issue occurs during the second merge phase.
optimize table x final;

select i from x where i = 42;

drop table x;

create table x (dt DateTime, i Int32 default 42 ttl dt + toIntervalDay(1)) engine ReplacingMergeTree(i) order by dt settings index_granularity = 8192, min_bytes_for_wide_part = 0;

system stop merges x;

insert into x values (now() - toIntervalDay(30), 1);

select i from x where i = 1;

system start merges x;

optimize table x final;

-- Run OPTIMIZE twice to ensure the second merge is triggered, as the "Not found column i in block" issue occurs during the second merge phase.
optimize table x final;

drop table x;

create table x (dt DateTime, i Int32 default 42 ttl dt + toIntervalDay(1)) engine MergeTree order by dt settings index_granularity = 8192, min_bytes_for_wide_part = 0;

system stop merges x;

insert into x values (now() - toIntervalDay(30), 1), (now() - toIntervalDay(30), 2);

system start merges x;

optimize table x final; -- Ensure that column TTL is applied

optimize table x final deduplicate by dt, i;

select i from x;

drop table x;
