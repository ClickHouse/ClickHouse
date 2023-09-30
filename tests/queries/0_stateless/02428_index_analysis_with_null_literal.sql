-- From https://github.com/ClickHouse/ClickHouse/issues/41814
drop table if exists test;

create table test(a UInt64, m UInt64, d DateTime) engine MergeTree partition by toYYYYMM(d) order by (a, m, d) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into test select number, number, '2022-01-01 00:00:00' from numbers(1000000);

select count() from test where a = (select toUInt64(1) where 1 = 2) settings enable_early_constant_folding = 0, force_primary_key = 1;

drop table test;

-- From https://github.com/ClickHouse/ClickHouse/issues/34063
drop table if exists test_null_filter;

create table test_null_filter(key UInt64, value UInt32) engine MergeTree order by key SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into test_null_filter select number, number from numbers(10000000);

select count() from test_null_filter where key = null and value > 0 settings force_primary_key = 1;

drop table test_null_filter;
