-- https://github.com/ClickHouse/ClickHouse/issues/50271

drop table if exists t1;
drop table if exists t2;

set enable_analyzer=1;

create table t1 (c3 String, primary key(c3)) engine = MergeTree;
create table t2 (c11 String, primary key(c11)) engine = MergeTree;
insert into t1 values ('succeed');
insert into t2 values ('succeed');

select
    ref_0.c11 as c_2_c30_0
  from
    t2 as ref_0
      cross join (select
            ref_1.c3 as c_6_c28_15
          from
            t1 as ref_1
       ) as subq_0
  where subq_0.c_6_c28_15 = (select c11 from t2 order by c11 limit 1);

drop table if exists t1;
drop table if exists t2;
