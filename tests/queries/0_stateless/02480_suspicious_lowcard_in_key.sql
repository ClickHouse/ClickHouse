set allow_suspicious_low_cardinality_types=1;

drop table if exists test;

create table test (val LowCardinality(Float32)) engine MergeTree order by val;

insert into test values (nan);

select count() from test where toUInt64(val) = -1; -- { serverError 70 }

drop table if exists test;
