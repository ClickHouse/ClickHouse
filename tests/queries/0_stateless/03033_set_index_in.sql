SET optimize_trivial_insert_select = 1;

create table a (k UInt64, v UInt64, index i (v) type set(100) granularity 2) engine MergeTree order by k settings index_granularity=8192, index_granularity_bytes=1000000000, min_index_granularity_bytes=0;
insert into a select number, intDiv(number, 4096) from numbers(1000000);
select sum(1+ignore(*)) from a where indexHint(v in (20, 40));
select sum(1+ignore(*)) from a where indexHint(v in (select 20 union all select 40 union all select 60));

SELECT 1 FROM a PREWHERE v IN (SELECT 1) WHERE v IN (SELECT 2);

select 1 from a where indexHint(indexHint(materialize(0)));
select sum(1+ignore(*)) from a where indexHint(indexHint(v in (20, 40)));
