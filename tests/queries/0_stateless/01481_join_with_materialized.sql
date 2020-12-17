drop table if exists t1;
drop table if exists t2;

create table t1
(
    col UInt64,
    x UInt64 MATERIALIZED col + 1
) Engine = MergeTree order by tuple();

create table t2
(
    x UInt64
) Engine = MergeTree order by tuple();

insert into t1 values (1),(2),(3),(4),(5);
insert into t2 values (1),(2),(3),(4),(5);

SELECT COUNT() FROM t1 INNER JOIN t2 USING x;

drop table t1;
drop table t2;
