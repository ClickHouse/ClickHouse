create table t(c Int32, d Bool) Engine=MergeTree order by c;
system stop merges t;

insert into t values (1, 0);
insert into t values (1, 0);
insert into t values (1, 1);
insert into t values (1, 0)(1, 1);

SELECT d, c, row_number() over (partition by d order by c) as c8 FROM t qualify c8=1 order by d settings max_threads=2;
SELECT '---';
SELECT d, c, row_number() over (partition by d order by c) as c8 FROM t order by d, c8 settings max_threads=2;
SELECT '---';

drop table t;

create table t (
  c Int32 primary key ,
  s Bool ,
  w Float64
  );

system stop merges t;

insert into t values(439499072,true,0),(1393290072,true,0);
insert into t values(-1317193174,false,0),(1929066636,false,0);
insert into t values(-2,false,0),(1962246186,true,0),(2054878592,false,0);
insert into t values(-1893563136,true,41.55);
insert into t values(-1338380855,true,-0.7),(-991301833,true,0),(-755809149,false,43.18),(-41,true,0),(3,false,0),(255,false,0),(255,false,0),(189195893,false,0),(195550885,false,9223372036854776000);

SELECT * FROM (
SELECT c, min(w) OVER (PARTITION BY s ORDER BY c ASC, s ASC, w ASC)
FROM t limit toUInt64(-1))
WHERE c = -755809149;

SELECT '---';

create table t_vkx4cc (
  c_ylzjpt Int32,
  c_hqfr9 Bool ,
  ) engine = MergeTree order by c_ylzjpt;

system stop merges t_vkx4cc;

insert into t_vkx4cc (c_ylzjpt, c_hqfr9) values (-2081147898, coalesce((NOT NOT(cast( (53 < 539704722) as Nullable(Bool)))), true)), (-1219769753, coalesce((true) and (false), false)), (-1981899149, coalesce(false, false)), (-1646738223, coalesce((NOT NOT(cast( (23.5 <= -26) as Nullable(Bool)))), false));

insert into t_vkx4cc (c_ylzjpt, c_hqfr9) values (255, coalesce(false, false)), (-1317193174, coalesce(false, false)), (-41, coalesce(true, false)), (1929066636, coalesce(false, true));

insert into t_vkx4cc (c_ylzjpt, c_hqfr9) values (-1700730666, coalesce((NOT NOT(cast( (-2022515471055597472 AND -29) as Nullable(Bool)))), false)), (1664059402, coalesce((NOT NOT(cast( (-19643 >= -122) as Nullable(Bool)))), false)), (1688303275, coalesce((NOT NOT(cast( (737275892 < 105) as Nullable(Bool)))), true)), (406615258, coalesce((NOT NOT(cast( (-657730213 = 82.86) as Nullable(Bool)))), false));

insert into t_vkx4cc (c_ylzjpt, c_hqfr9) values (-2, coalesce(false, false)), (1962246186, coalesce(true, false)), (-991301833, coalesce(true, true)), (2054878592, coalesce(false, false));

insert into t_vkx4cc (c_ylzjpt, c_hqfr9) values (643897141, coalesce((NOT NOT(cast( (-60 AND cast(null as Nullable(Int64))) as Nullable(Bool)))), true)), (-2051538534, coalesce(((-1616816511 between 332225780 and -1883087387)) or ((-573375170 between -1427445977 and 615586748)), false)), (77089559, coalesce((NOT NOT(cast( ((true) and (true) != 925456787) as Nullable(Bool)))), false)), (1116921321, coalesce((0 is NULL), true));

insert into t_vkx4cc (c_ylzjpt, c_hqfr9) values (-816935497, coalesce(false, false)), (1207796283, coalesce((-129 between -5 and -5), false)), (-1365934326, coalesce(true, false)), (-1618912877, coalesce((NOT NOT(cast( (false >= 31833) as Nullable(Bool)))), false));

insert into t_vkx4cc (c_ylzjpt, c_hqfr9) values (-331834394, coalesce((NOT NOT(cast( (-63 <= -1822810052) as Nullable(Bool)))), true)), (-1020892864, coalesce((NOT NOT(cast( (40.31 <= 8146037365746019777) as Nullable(Bool)))), true)), (-1150980622, coalesce(((94019304 between -730556489 and 32)) and ((-956354236 is not NULL)), true)), (-1203382363, coalesce(true, true));

insert into t_vkx4cc (c_ylzjpt, c_hqfr9) values (-653505826, coalesce((true) or (true), false)), (-1975508531, coalesce(((-796885845 between 65536 and cast(null as Nullable(Int32)))) or ((NOT NOT(cast( (-7467729336434250795 < 100.20) as Nullable(Bool))))), false)), (-1465484835, coalesce(((NOT NOT(cast( (19209 <= 75.96) as Nullable(Bool))))) or (true), false)), (1968095908, coalesce((NOT NOT(cast( (-1309960412156062327 > 13102) as Nullable(Bool)))), true));

alter table t_vkx4cc add column c_zosphq2t1 Float64;

insert into t_vkx4cc (c_ylzjpt, c_hqfr9, c_zosphq2t1) values (-153185515, coalesce((NOT NOT(cast( (1291639145 >= 30.22) as Nullable(Bool)))), false), -1.8), (-411038390, coalesce(((-762326135 between 16 and 177530758)) or (false), true), 26.34), (914629990, coalesce((-1125832977 is not NULL), true), 59.2), (541758331, coalesce(false, true), -255.1);

insert into t_vkx4cc (c_ylzjpt, c_hqfr9, c_zosphq2t1) values (2125075305, coalesce(false, false), 55.36), (-1176267855, coalesce(true, true), 55.45), (1459407556, coalesce((true) and ((NOT NOT(cast( (95.96 != 65) as Nullable(Bool))))), true), 85.80), (-1098155311, coalesce(false, false), 2147483649.9);

insert into t_vkx4cc (c_ylzjpt, c_hqfr9, c_zosphq2t1) values (6, coalesce((NOT NOT(cast( (1546334968 < -4) as Nullable(Bool)))), true), 57.42), (-5, coalesce((NOT NOT(cast( (59 AND 13) as Nullable(Bool)))), false), 65536.3), (100663045, coalesce((-1190355242 is not NULL), true), 73.80), (-451392958, coalesce((NOT NOT(cast( (false != -443845933) as Nullable(Bool)))), false), -4294967294.0);

insert into t_vkx4cc (c_ylzjpt, c_hqfr9, c_zosphq2t1) values (561061873, coalesce(true, false), 12.17), (-526570556, coalesce(false, false), 64.73), (-1450619195, coalesce(true, true), 54.33), (-3, coalesce(true, true), 52.9);

insert into t_vkx4cc (c_ylzjpt, c_hqfr9, c_zosphq2t1) values (-504713125, coalesce(false, true), 27.58), (897064234, coalesce((836516994 between cast(null as Nullable(Int32)) and -1832647080), true), 9223372036854775809.2), (65535, coalesce(true, true), 4294967297.5), (-599948807, coalesce((false) or ((NOT NOT(cast( (6.52 = 65.49) as Nullable(Bool))))), false), 256.5);

insert into t_vkx4cc (c_ylzjpt, c_hqfr9, c_zosphq2t1) values (-1650266905, coalesce((NOT NOT(cast( (-83 = -218055084) as Nullable(Bool)))), true), 1.9), (-841067875, coalesce(false, true), -126.5), (15, coalesce(((NOT NOT(cast( (cast(null as Nullable(Decimal)) = cast(null as Nullable(Int32))) as Nullable(Bool))))) or (true), true), 33.65), (1913361922, coalesce((NOT NOT(cast( (false AND 0) as Nullable(Bool)))), false), 6.4);

insert into t_vkx4cc (c_ylzjpt, c_hqfr9, c_zosphq2t1) values (1159852204, coalesce((-2057115045 is not NULL), false), 20.61), (-6, coalesce(true, true), 66.33), (-1154269118, coalesce(false, true), 8.89), (1258218855, coalesce(true, false), 19.80);

insert into t_vkx4cc (c_ylzjpt, c_hqfr9, c_zosphq2t1) values (1603772265, coalesce(false, true), 57.87), (-176934810, coalesce(false, true), 128.8), (-1458338029, coalesce((NOT NOT(cast( (20908 != (NOT NOT(cast( (cast(null as Nullable(Decimal)) <= (true)         or ((NOT NOT(cast( (973511022 <= -112) as Nullable(Bool)))))) as Nullable(Bool))))) as Nullable(Bool)))), true), 76.54), (-262516786, coalesce((cast(null as Nullable(Int32)) is NULL), false), 21.49);

insert into t_vkx4cc (c_ylzjpt, c_hqfr9, c_zosphq2t1) values (-1197430632, coalesce(true, false), 45.40), (-685902265, coalesce((NOT NOT(cast( (cast(null as Nullable(Decimal)) < cast(null as Nullable(Decimal))) as Nullable(Bool)))), true), 5.55), (1936334332, coalesce((-1565552735 is not NULL), false), 26.28), (2030467062, coalesce((NOT NOT(cast( (127.3 != cast(null as Nullable(Int32))) as Nullable(Bool)))), true), 89.50);

insert into t_vkx4cc (c_ylzjpt, c_hqfr9, c_zosphq2t1) values (720985423, coalesce((NOT NOT(cast( (-451448940 = cast(null as Nullable(Decimal))) as Nullable(Bool)))), false), 52.65), (-222873194, coalesce(((-20 between -1419620477 and 1616455043)) or ((25624502 between 1312431316 and 1757361651)), false), 127.2), (745669725, coalesce((NOT NOT(cast( ((NOT NOT(cast( (cast(null as Nullable(UInt64)) <= 42) as Nullable(Bool)))) >= 3233811255032796928) as Nullable(Bool)))), false), 7.74), (-74234560, coalesce((NOT NOT(cast( (cast(null as Nullable(Decimal)) >= cast(null as Nullable(Decimal))) as Nullable(Bool)))), true), 19.25);

SELECT DISTINCT
  count(ref_0.c_zosphq2t1) over (partition by ref_0.c_hqfr9 order by ref_0.c_ylzjpt, ref_0.c_hqfr9, ref_0.c_zosphq2t1) as c0,
  ref_0.c_ylzjpt as c1
FROM
  t_vkx4cc as ref_0
  order by c0, c1;
