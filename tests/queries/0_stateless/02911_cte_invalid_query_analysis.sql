drop table if exists t0;
drop table if exists t1;
drop table if exists t3;

create table t0 (pkey UInt32, c1 UInt32, primary key(pkey)) engine = MergeTree;
create table t1 (vkey UInt32, primary key(vkey)) engine = MergeTree;
create table t3 (c17 String, primary key(c17)) engine = MergeTree;
insert into t1 values (3);

WITH
cte_1 AS (select
    subq_1.c_5_c1698_16 as c_2_c1702_3,
    subq_1.c_5_c1694_12 as c_2_c1703_4
  from
    (select
          covarPop(-0, 74) as c_5_c1686_4,
          sumWithOverflow(0) as c_5_c1694_12,
          covarPop(-53.64, 92.63) as c_5_c1698_16
        from
          t3 as ref_8
        group by ref_8.c17) as subq_1)
select
    ref_15.c_2_c1703_4 as c_2_c1723_6,
    ref_15.c_2_c1702_3 as c_2_c1724_7
  from
    t0 as ref_14
        RIGHT outer join cte_1 as ref_15
        on (ref_14.c1 = ref_15.c_2_c1702_3)
      RIGHT outer join t1 as ref_16
      on (ref_14.pkey = ref_16.vkey);

drop table t0;
drop table t1;
drop table t3;
