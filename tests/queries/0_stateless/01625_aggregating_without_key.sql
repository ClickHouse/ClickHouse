drop table if exists agg;

create table agg engine AggregatingMergeTree order by tuple() as select sumState(10) i, sumState(20) j, sumState(30) k;

insert into agg select sumState(10), sumState(20), sumState(30);

optimize table agg;

select * apply (finalizeAggregation) from agg;

drop table if exists agg;
