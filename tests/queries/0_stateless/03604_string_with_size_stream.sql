-- { echo ON }

drop table if exists test;

create table test (s String) engine MergeTree order by () settings serialize_string_with_size_stream = 0;

insert into test values ('hello world');

-- Old string type also supports .size subcolumn
select s.size from test;

-- system.parts_columns table only lists physical subcolumns/substreams
select column, substreams, subcolumns.names, subcolumns.types from system.parts_columns where database = currentDatabase() and table = 'test' and active order by column;

drop table test;

create table test (s String) engine MergeTree order by () settings serialize_string_with_size_stream = 1;

insert into test values ('hello world');

-- Verify that string type is serialized with a physical .size stream
select column, substreams, subcolumns.names, subcolumns.types from system.parts_columns where database = currentDatabase() and table = 'test' and active order by column;

-- Test empty string comparison and .size subcolumn optimization
set enable_analyzer = 1;
set optimize_empty_string_comparisons = 1;
set optimize_functions_to_subcolumns = 0;

drop table if exists t_column_names;

create table t_column_names (s String) engine Memory;

insert into t_column_names values ('foo');

explain query tree dump_tree = 0, dump_ast = 1 select s != '' from t_column_names;

select s != '' from t_column_names;

set optimize_functions_to_subcolumns = 1;

explain query tree dump_tree = 0, dump_ast = 1 select s != '' from t_column_names;

select s != '' from t_column_names;

drop table t_column_names;
