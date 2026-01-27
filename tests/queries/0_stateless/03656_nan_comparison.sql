DROP TABLE IF EXISTS t7;
create table t7 (c57 UInt32) engine = MergeTree order by c57;
insert into t7 values (1);

SELECT
(
select count(*) 
from t7 as ref_0
where ref_0.c57 <> (case when 1 = 1 then nan else ref_0.c57 end)
)
=
(
select count(*) 
from t7 as ref_0
where ref_0.c57 <> nan
);

DROP TABLE t7;
