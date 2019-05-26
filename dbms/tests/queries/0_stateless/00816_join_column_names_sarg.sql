drop table if exists t1;
drop table if exists t2;
create table t1 (a Int8, val Float32) engine=Memory();
create table t2 (a Int8, val Float32) engine=Memory();

INSERT INTO t1 VALUES (1, 123);
INSERT INTO t2 VALUES (1, 456);


select t1.a, t2.a from t1 all inner join t2 on t1.a=t2.a;
-- Received exception from server (version 18.14.1):
-- Code: 47. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Unknown identifier: t2.a.

-- this query works fine
select t1.a, t2.* from t1 all inner join t2 on t1.a=t2.a;

-- and this
select t1.a, t2.val from t1 all inner join t2 on t1.a=t2.a;

DROP TABLE t1;
DROP TABLE t2;
