create table t(c Int32, d Bool) Engine=MergeTree order by c;
system stop merges t;

insert into t values (1, 0);
insert into t values (1, 0);
insert into t values (1, 1);
insert into t values (1, 0)(1, 1);

SELECT d, c, row_number() over (partition by d order by c) as c8 FROM t qualify c8=1 order by d settings max_threads=2, allow_experimental_analyzer = 1;
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
