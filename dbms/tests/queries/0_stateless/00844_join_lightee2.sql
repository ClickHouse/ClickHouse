DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE IF NOT EXISTS t1 (
f1 UInt32,
f2 String
) ENGINE = MergeTree ORDER BY (f1);

CREATE TABLE IF NOT EXISTS t2 (
f1 String,
f3 String
) ENGINE = MergeTree ORDER BY (f1);

insert into t1 values(1,'1');
insert into t2 values('1','name1');

select t1.f1,t2.f3 from t1 all inner join t2 on t1.f2 = t2.f1
where t2.f1 = '1';

DROP TABLE t1;
DROP TABLE t2;
