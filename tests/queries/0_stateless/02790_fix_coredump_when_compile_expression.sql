CREATE TABLE test (col1 Nullable(DOUBLE), col2 Nullable(DOUBLE), col3 DOUBLE) ENGINE=Memory;

insert into test values(1.0 , 2.0, 3.0);
select multiIf(col1 > 2, col2/col3, 4.0) from test SETTINGS min_count_to_compile_expression=0;
