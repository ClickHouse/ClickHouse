drop database if exists local_test;

create database if not exists local_test;

create table local_test.auto_index_test (a Int32, b String, c Float64) engine MergeTree order by a
settings add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=1;

create table local_test.auto_index_test_clone as local_test.auto_index_test;

drop database local_test;