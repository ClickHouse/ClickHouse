create table if not exists replacing_mt (x String) engine=ReplacingMergeTree() ORDER BY x SETTINGS force_select_final=1;

insert into replacing_mt values ('abc');
insert into replacing_mt values ('abc');

select count() from replacing_mt