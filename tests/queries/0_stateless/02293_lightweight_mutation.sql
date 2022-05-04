drop table if exists t_light;
create table t_light(a int, b int, c int, index i_c(b) type minmax granularity 4) engine = MergeTree order by a partition by c % 5 settings min_bytes_for_wide_part=0;
INSERT INTO t_light SELECT number, number, number FROM numbers(10);

SELECT '-----lightweight mutation type-----';
SET mutations_sync = 1;
SET enable_lightweight_mutation = 1;

alter table t_light DELETE WHERE a=6;
alter table t_light MATERIALIZE INDEX i_c;
alter table t_light UPDATE b=b+1 where a=9;

alter table t_light delete in partition 2 where a < 10;
set enable_lightweight_mutation = 0;
alter table t_light update b=-2 where a < 5;

SELECT mutation_type, command, is_done FROM system.mutations WHERE table = 't_light';

SELECT '-----Check that select and merge with lightweight mutation.-----';
select count(*) from t_light;
select * from t_light order by a;

select table, partition, name, rows from system.parts where active and table ='t_light' order by name;

optimize table t_light final;
select count(*) from t_light;

select table, partition, name, rows from system.parts where active and table ='t_light' order by name;

drop table t_light;

SELECT '-----Test lightweight mutation for table with MATERIALIZED column, scalar subquery mutations-----';
set enable_lightweight_mutation = 1;

drop table if exists t_mix;
CREATE TABLE t_mix(a UInt32, b int, c MATERIALIZED a+b) engine = MergeTree order by a settings min_bytes_for_wide_part=0;
INSERT INTO t_mix SELECT number + 1, number + 1  FROM numbers(10);

ALTER TABLE t_mix UPDATE b = -1 WHERE a = (SELECT a FROM t_mix WHERE a = 1);
ALTER TABLE t_mix DELETE WHERE a = (SELECT a FROM t_mix WHERE a = 2), UPDATE b = -2 WHERE a = 1;

ALTER TABLE t_mix UPDATE b = -3 WHERE a = 2;

SELECT * FROM t_mix order by a;
SELECT count(*) FROM t_mix;

truncate table t_mix;

SELECT count(*) FROM t_mix;

drop table t_mix;

SELECT '-----Test lightweight mutation in multi blocks-----';
CREATE TABLE t_large(a UInt32, b int) ENGINE=MergeTree order BY a settings min_bytes_for_wide_part=0;
INSERT INTO t_large SELECT number + 1, number + 1  FROM numbers(100000);

ALTER TABLE t_large UPDATE b = -1 WHERE a = 1, DELETE WHERE a = 50000, UPDATE b = -1 WHERE a = 99999;
ALTER TABLE t_large UPDATE b = -2 WHERE a  between 1000 and 1005 , DELETE WHERE a between 1004 and 1010;
SELECT * FROM t_large WHERE a in (1, 1000,1005,1011, 50000, 99999) order by a;
desc t_large;

DROP TABLE  t_large;

SELECT '-----Check that ttl info was updated after mutation.-----';
drop table if exists ttl;
set mutations_sync = 2;

create table ttl (i Int, a Int, s String) engine = MergeTree order by i settings min_bytes_for_wide_part=0;
insert into ttl values (1, 1, 'a') (2, 1, 'b') (3, 1, 'c') (4, 1, 'd');

alter table ttl modify ttl a % 2 = 0 ? today() - 10 : toDate('2100-01-01');
alter table ttl materialize ttl;

select * from ttl order by i;
alter table ttl update a = 0 where i % 2 = 0;
select * from ttl order by i;
alter table ttl materialize ttl;
select * from ttl order by i;

drop table ttl;

