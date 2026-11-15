DROP TABLE IF EXISTS t0, t1, t4, t5;

set join_use_nulls =  1;

create table t0 (c2 String, primary key(c2)) engine = MergeTree;
create table t1 (vkey UInt32, c8 String, primary key(vkey))engine = MergeTree;
create view t4 as 
select     
    ref_1.vkey as c_2_c48_2
  from 
    t0 as ref_0
      left outer join t1 as ref_1
      on (ref_0.c2 = ref_1.c8) ;
create table t5 (pkey UInt32, c52 UInt32, c56 String, primary key(pkey))engine = MergeTree;

insert into t0 values (null);
insert into t0 values ('');
insert into t1 values (59, '');
insert into t5 values (12000, null, '');
insert into t5 values  (22000, null, null);
insert into t5 values  (24000, 14, 'YLq?');
insert into t5 values  (30000, 0, '-');
insert into t5 values  (33000, null, 'Wm@c');
insert into t5 values (37000, 0, 'IB');
insert into t5 values (38000,  59, '');
insert into t5 values (56000, 0, null);
insert into t5 values (64000, 74, '');
insert into t5 values (72000, 36, 'q:/');
insert into t5 values (79000, null, '[P');
insert into t5 values (82000, 0, 'V-Qr');
insert into t5 values (88000, 44, '1Z ');
insert into t5 values (94000, 15, 'G]A5');
insert into t5 values (96000, -0, 'C8');
insert into t5 values (97000, 56,  null);

select
    count(*)
  from
    t5 as ref_2
      left outer join (select
            ref_3.c_2_c48_2 as c_6_c185_6
          from
            t4 as ref_3
          ) as subq_1
      on (ref_2.c52 = subq_1.c_6_c185_6 )
  where intExp2(ref_2.pkey) <= 
      (case when ((subq_1.c_6_c185_6 = 1) and (not (subq_1.c_6_c185_6 = 1))) then 0 else hiveHash(ref_2.c56) end);

select
    count(*)
  from
    t5 as ref_2
      left outer join (select
            ref_3.c_2_c48_2 as c_6_c185_6
          from
            t4 as ref_3
          ) as subq_1
      on (ref_2.c52 = subq_1.c_6_c185_6 )
  where intExp2(ref_2.pkey) <= hiveHash(ref_2.c56);

DROP TABLE t0, t1, t4, t5;
