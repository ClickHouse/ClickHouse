set allow_suspicious_low_cardinality_types=1;
drop table if exists test;
create table test (x LowCardinality(Int32)) engine=Memory;
insert into test select 1;
insert into test select 2;
select x + 1e10 from test order by 1e10;
select x + (1e10 + 1e20) from test order by (1e10 + 1e20);
select x + (pi() + exp(2)) from test order by (pi() + exp(2));
drop table test;

