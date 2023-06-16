DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t3;

create table t1 (pkey UInt32, c8 UInt32, c9 String, c10 Float32, c11 String, primary key(c8)) engine = ReplacingMergeTree;
create table t3 (vkey UInt32, pkey UInt32, c15 UInt32) engine = Log;

SET min_count_to_compile_expression = 0;

with cte_4 as (
  select
    ref_10.c11 as c_2_c2350_1,
    ref_9.c9 as c_2_c2351_2
  from
    t1 as ref_9
          right outer join t1 as ref_10
          on (ref_9.c11 = ref_10.c9)
        inner join t3 as ref_11
        on (ref_10.c8 = ref_11.vkey)
  where ((ref_10.pkey + ref_11.pkey) between ref_11.vkey and (case when (-30.87 >= ref_9.c10) then ref_11.c15 else ref_11.pkey end)))
select
    ref_13.c_2_c2350_1 as c_2_c2357_3
  from
    cte_4 as ref_13
  where (ref_13.c_2_c2351_2) in (
        select
          ref_14.c_2_c2351_2 as c_5_c2352_0
        from 
          cte_4 as ref_14);

DROP TABLE t1;
DROP TABLE t3;
