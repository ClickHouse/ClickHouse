-- Tags: no-parallel

DROP DATABASE IF EXISTS some_tests;
set allow_deprecated_database_ordinary=1;
CREATE DATABASE some_tests ENGINE=Ordinary; -- Different inner table name with Atomic

set allow_deprecated_syntax_for_merge_tree=1;
create table some_tests.my_table ENGINE = MergeTree(day, (day), 8192) as select today() as day, 'mystring' as str;
show tables from some_tests;
create materialized view some_tests.my_materialized_view ENGINE = MergeTree(day, (day), 8192) as select * from some_tests.my_table;
show tables from some_tests;
select * from some_tests.my_materialized_view;

DROP DATABASE some_tests;
