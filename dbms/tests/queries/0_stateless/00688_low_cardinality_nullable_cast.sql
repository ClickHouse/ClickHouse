SELECT CAST(NULL, 'LowCardinality(Nullable(Int8))');

drop table if exists lc_null_int8_defnull;
CREATE TABLE lc_null_int8_defnull (val LowCardinality(Nullable(Int8)) DEFAULT NULL) ENGINE = MergeTree order by tuple();
insert into lc_null_int8_defnull values (1);
select * from lc_null_int8_defnull values;
alter table lc_null_int8_defnull add column val2 LowCardinality(Nullable(Int8)) DEFAULT NULL;
insert into lc_null_int8_defnull values (2, 3);
select * from lc_null_int8_defnull order by val;
drop table if exists lc_null_int8_defnull;

