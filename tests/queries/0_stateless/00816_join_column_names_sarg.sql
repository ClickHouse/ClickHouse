drop table if exists t1_00816;
drop table if exists t2_00816;
create table t1_00816 (a Int8, val Float32) engine=Memory();
create table t2_00816 (a Int8, val Float32) engine=Memory();

INSERT INTO t1_00816 VALUES (1, 123);
INSERT INTO t2_00816 VALUES (1, 456);


select t1_00816.a, t2_00816.a from t1_00816 all inner join t2_00816 on t1_00816.a=t2_00816.a;
-- Received exception from server (version 18.14.1):
-- Code: 47. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Unknown identifier: t2_00816.a.

-- this query works fine
select t1_00816.a, t2_00816.* from t1_00816 all inner join t2_00816 on t1_00816.a=t2_00816.a;

-- and this
select t1_00816.a, t2_00816.val from t1_00816 all inner join t2_00816 on t1_00816.a=t2_00816.a;

DROP TABLE t1_00816;
DROP TABLE t2_00816;
