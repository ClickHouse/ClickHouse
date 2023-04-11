DROP TABLE IF EXISTS merge_table_standard_delete;

CREATE TABLE merge_table_standard_delete(id Int32, name String) ENGINE = MergeTree order by id settings min_bytes_for_wide_part=0;

INSERT INTO merge_table_standard_delete select number, toString(number) from numbers(100);

SET mutations_sync = 1;
SET allow_experimental_lightweight_delete = 1;

DELETE FROM merge_table_standard_delete WHERE id = 10;

SELECT COUNT() FROM merge_table_standard_delete;

DETACH TABLE merge_table_standard_delete;
ATTACH TABLE merge_table_standard_delete;
CHECK TABLE merge_table_standard_delete;

DELETE FROM merge_table_standard_delete WHERE name IN ('1','2','3','4');

SELECT COUNT() FROM merge_table_standard_delete;

DETACH TABLE merge_table_standard_delete;
ATTACH TABLE merge_table_standard_delete;
CHECK TABLE merge_table_standard_delete;

DELETE FROM merge_table_standard_delete WHERE 1;

SELECT COUNT() FROM merge_table_standard_delete;

DETACH TABLE merge_table_standard_delete;
ATTACH TABLE merge_table_standard_delete;
CHECK TABLE merge_table_standard_delete;

DROP TABLE merge_table_standard_delete;

drop table if exists t_light;
create table t_light(a int, b int, c int, index i_c(b) type minmax granularity 4) engine = MergeTree order by a partition by c % 5 settings min_bytes_for_wide_part=0;
INSERT INTO t_light SELECT number, number, number FROM numbers(10);

SELECT '-----lightweight mutation type-----';

DELETE FROM t_light WHERE c%5=1;

DETACH TABLE t_light;
ATTACH TABLE t_light;
CHECK TABLE t_light;

DELETE FROM t_light WHERE c=4;

DETACH TABLE t_light;
ATTACH TABLE t_light;
CHECK TABLE t_light;

alter table t_light MATERIALIZE INDEX i_c;
alter table t_light update b=-1 where a<3;
alter table t_light drop index i_c;

DETACH TABLE t_light;
ATTACH TABLE t_light;
CHECK TABLE t_light;

SELECT command, is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_light';

SELECT '-----Check that select and merge with lightweight delete.-----';
select count(*) from t_light;
select * from t_light order by a;

select table, partition, name, rows from system.parts where database = currentDatabase() AND active and table ='t_light' order by name;

optimize table t_light final;
select count(*) from t_light;

select table, partition, name, rows from system.parts where database = currentDatabase() AND active and table ='t_light' and rows > 0 order by name;

drop table t_light;

SELECT '-----Test lightweight delete in multi blocks-----';
CREATE TABLE t_large(a UInt32, b int) ENGINE=MergeTree order BY a settings min_bytes_for_wide_part=0;
INSERT INTO t_large SELECT number + 1, number + 1  FROM numbers(100000);

DELETE FROM t_large WHERE a = 50000;

DETACH TABLE t_large;
ATTACH TABLE t_large;
CHECK TABLE t_large;

ALTER TABLE t_large UPDATE b = -2 WHERE a between 1000 and 1005;
ALTER TABLE t_large DELETE WHERE a=1;

DETACH TABLE t_large;
ATTACH TABLE t_large;
CHECK TABLE t_large;

SELECT * FROM t_large WHERE a in (1,1000,1005,50000) order by a;

DROP TABLE  t_large;

SELECT '----Test lighweight delete is disabled if table has projections-----';

CREATE TABLE t_proj(a UInt32, b int) ENGINE=MergeTree order BY a settings min_bytes_for_wide_part=0;

ALTER TABLE t_proj ADD PROJECTION p_1 (SELECT avg(a), avg(b), count());

INSERT INTO t_proj SELECT number + 1, number + 1  FROM numbers(1000);

DELETE FROM t_proj WHERE a < 100; -- { serverError NOT_IMPLEMENTED }

SELECT avg(a), avg(b), count() FROM t_proj;

DROP TABLE t_proj;
