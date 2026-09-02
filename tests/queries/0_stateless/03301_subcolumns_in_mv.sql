set enable_json_type = 1;
drop table if exists src;
drop table if exists dst;
drop view if exists view;

create table src (a Tuple(b Tuple(c UInt32)), json JSON(x.y UInt32)) engine=Memory;
create table dst (a UInt32, x UInt32, z UInt32) engine=Memory;
create materialized view view to dst as select a.b.c as a, json.x.y as x, json.z::UInt32 as z from src;

insert into src select tuple(tuple(1)), '{"x" : {"y" : 2}, "z" : 3}';
select * from dst;

drop view view;
drop table dst;
drop table src;

