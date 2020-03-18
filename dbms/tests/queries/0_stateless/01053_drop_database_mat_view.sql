DROP DATABASE IF EXISTS some_tests;
CREATE DATABASE some_tests ENGINE=Ordinary;

create table some_tests.my_table ENGINE = MergeTree(day, (day), 8192) as select today() as day, 'mystring' as str;
show tables from some_tests;
create materialized view some_tests.my_materialized_view ENGINE = MergeTree(day, (day), 8192) as select * from some_tests.my_table;
show tables from some_tests;
select * from some_tests.my_materialized_view;

DROP DATABASE some_tests;
