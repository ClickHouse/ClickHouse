set allow_experimental_low_cardinality_type = 1;

SELECT CAST(NULL, 'LowCardinality(Nullable(Int8))');

drop table if exists test.lc_null_int8_defnull;
CREATE TABLE test.lc_null_int8_defnull (val LowCardinality(Nullable(Int8)) DEFAULT NULL) ENGINE = MergeTree order by tuple();
insert into test.lc_null_int8_defnull values (1);
select * from test.lc_null_int8_defnull values;
alter table test.lc_null_int8_defnull add column val2 LowCardinality(Nullable(Int8)) DEFAULT NULL;
insert into test.lc_null_int8_defnull values (2, 3);
select * from test.lc_null_int8_defnull order by val;
drop table if exists test.lc_null_int8_defnull;

