DROP TABLE IF EXISTS dest_table_mv;
DROP TABLE IF EXISTS left_table;
DROP TABLE IF EXISTS right_table;
DROP TABLE IF EXISTS dest_table;
DROP TABLE IF EXISTS src_table;
DROP VIEW IF EXISTS dst_mv;
DROP VIEW IF EXISTS dst_mv_1;
DROP VIEW IF EXISTS dst_mv_2;


create table src_table Engine=Memory as system.numbers;
CREATE MATERIALIZED VIEW dst_mv_1 Engine=Memory as select *, (SELECT count() FROM src_table) AS cnt FROM src_table;
insert into src_table select 1 from numbers(3);
insert into src_table select 2 from numbers(2);
insert into src_table select 3 from numbers(1);
select * from dst_mv_1 order by number;


CREATE TABLE dest_table (`Date` Date, `Id` UInt64, `Units` Float32) ENGINE = Memory;
create table left_table as dest_table;
create table right_table as dest_table;
insert into right_table select toDate('2020-01-01') + number, number, number / 2 from numbers(10);

CREATE MATERIALIZED VIEW dest_table_mv TO dest_table as select Date, Id, Units FROM (SELECT * FROM left_table) AS t1 INNER JOIN (WITH (SELECT DISTINCT Date FROM left_table LIMIT 1) AS dt SELECT * FROM right_table WHERE Date = dt) AS t2 USING (Date, Id);

insert into left_table select toDate('2020-01-01'), 0, number * 2 from numbers(3);
select 'the rows get inserted';
select * from dest_table order by Date, Id, Units;

insert into left_table select toDate('2020-01-01'), 5, number * 2 from numbers(3);
select 'no new rows';
select * from dest_table order by Date, Id, Units;

truncate table left_table;
insert into left_table select toDate('2020-01-01') + 5, 5, number * 2 from numbers(3);
select 'the rows get inserted';
select * from dest_table order by Date, Id, Units;

drop table dest_table_mv;
drop table left_table;
drop table right_table;
drop table dest_table;
drop table src_table;
drop view if exists dst_mv;
drop view if exists dst_mv_1;
drop view if exists dst_mv_2;
