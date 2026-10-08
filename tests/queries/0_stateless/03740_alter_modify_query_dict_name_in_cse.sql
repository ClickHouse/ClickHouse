drop table if exists mv;
drop table if exists dst;
drop table if exists src;
drop dictionary if exists dict;

create table src (key Int) engine=MergeTree order by ();
create table dst (key Int) engine=MergeTree order by ();
create dictionary dict (key Int, value Int) primary key key layout(direct) source(clickhouse(query 'select 0 key, 0 value'));
create materialized view mv to dst as select * from src;
alter table mv modify query with 'dict' as dict_name select dictGetInt32(dict_name, 'value', key) from src;
