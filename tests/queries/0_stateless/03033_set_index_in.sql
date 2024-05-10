create table a (k UInt64, v UInt64, index i (v) type set(100) granularity 2) engine MergeTree order by k settings index_granularity=8192, index_granularity_bytes=1000000000, min_index_granularity_bytes=0;
insert into a select number, intDiv(number, 4096) from numbers(1000000);
select sum(1+ignore(*)) from a where indexHint(v in (20, 40));
select sum(1+ignore(*)) from a where indexHint(v in (select 20 union all select 40 union all select 60));
