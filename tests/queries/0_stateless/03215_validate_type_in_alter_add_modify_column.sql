set allow_experimental_variant_type = 0;
set allow_experimental_dynamic_type = 0;
set allow_suspicious_low_cardinality_types = 0;
set allow_suspicious_fixed_string_types = 0;

drop table if exists test;
create table test (id UInt64) engine=MergeTree order by id;
alter table test add column bad Variant(UInt32, String); -- {serverError ILLEGAL_COLUMN}
alter table test add column bad Dynamic; -- {serverError ILLEGAL_COLUMN}
alter table test add column bad LowCardinality(UInt8); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
alter table test add column bad FixedString(10000); -- {serverError ILLEGAL_COLUMN}

alter table test modify column id Variant(UInt32, String); -- {serverError ILLEGAL_COLUMN}
alter table test modify column id Dynamic; -- {serverError ILLEGAL_COLUMN}
alter table test modify column id LowCardinality(UInt8); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
alter table test modify column id FixedString(10000); -- {serverError ILLEGAL_COLUMN}

drop table test;

