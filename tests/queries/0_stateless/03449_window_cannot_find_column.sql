DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t3;

create table t0 (vkey UInt32, primary key(vkey)) engine = MergeTree;
create view t3 as 
select distinct 
    ref_0.vkey as c_2_c16_0
  from 
    t0 as ref_0;
insert into t0 values (4);

select 'It was a bug-triggering query:';
with cte_4 as (select
    rank() over w0 as c_2_c2398_0
  from
    t3 as ref_15
  window w0 as (partition by ref_15.c_2_c16_0 order by ref_15.c_2_c16_0 desc))
select distinct
    ref_39.c_2_c2398_0 as c_9_c2479_0
  from
    cte_4 as ref_39;

DROP TABLE t3;
DROP TABLE t0;
