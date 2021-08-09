DROP TABLE IF EXISTS t1_00844;
DROP TABLE IF EXISTS t2_00844;

CREATE TABLE IF NOT EXISTS t1_00844 (
f1 UInt32,
f2 String
) ENGINE = MergeTree ORDER BY (f1);

CREATE TABLE IF NOT EXISTS t2_00844 (
f1 String,
f3 String
) ENGINE = MergeTree ORDER BY (f1);

insert into t1_00844 values(1,'1');
insert into t2_00844 values('1','name1');

select t1_00844.f1,t2_00844.f3 from t1_00844 all inner join t2_00844 on t1_00844.f2 = t2_00844.f1
where t2_00844.f1 = '1';

DROP TABLE t1_00844;
DROP TABLE t2_00844;
