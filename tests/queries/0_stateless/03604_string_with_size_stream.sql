-- { echo ON }

drop table if exists test;

create table test (s String, s2 StringWithSizeStream) engine MergeTree order by () settings serialize_string_with_size_stream = 0;

insert into test values ('hello', 'world');

-- New string type supports .size subcolumn
select s2.size from test;

-- Old string type also supports .size subcolumn
select s.size from test;

-- system.parts_columns table only lists physical subcolumns/substreams
select column, substreams, subcolumns.names, subcolumns.types from system.parts_columns where database = currentDatabase() and table = 'test' and active order by column;

drop table test;

create table test (s String) engine MergeTree order by () settings serialize_string_with_size_stream = 1;

insert into test values ('hello world');

-- Verify that string type is serialized with a physical .size stream
select column, substreams, subcolumns.names, subcolumns.types from system.parts_columns where database = currentDatabase() and table = 'test' and active order by column;

set enable_analyzer = 1;
set optimize_empty_string_comparisons = 1;
set optimize_functions_to_subcolumns = 0;

select replaceRegexpAll(explain, '__table1\.|_UInt8', '') from (explain actions=1 select count() from test where s != '') where explain like '%Prewhere filter%' or explain like '%Filter column%';
select count() from test where s != '';

set optimize_functions_to_subcolumns = 1;

select replaceRegexpAll(explain, '__table1\.|_UInt8', '') from (explain actions=1 select count() from test where s != '') where explain like '%Prewhere filter%' or explain like '%Filter column%';
select count() from test where s != '';

drop table test;
