-- Tags: no-fasttest

drop table if exists test_table;

create table test_table (col String, col_nullable Nullable(String)) engine MergeTree order by col;
insert into test_table select '{"string_value":null}' as col, '{"string_value":null}' as col_nullable;

select JSONExtract(col, 'string_value', 'Nullable(String)') as res1, JSONExtract(col_nullable, 'string_value', 'Nullable(String)') as res2, JSONExtract(assumeNotNull(col_nullable), 'string_value', 'Nullable(String)') as res3 from test_table;

drop table test_table;
