select toDate32('2020-01-01') in (toDate('2020-01-01'));
select toDate('2020-01-01') in (toDate32('2020-01-01'));
select toDate('2020-01-01') in 1::Int64;
select toDate32('2020-01-01') in 1::UInt64;
