select CAST(toLowCardinality(val) as UInt64) from (select arrayJoin(['1']) as val);
select toUInt64(toLowCardinality(val)) from (select arrayJoin(['1']) as val);
select 1 % toLowCardinality(val) from (select arrayJoin([1]) as val);
select gcd(1, toLowCardinality(val)) from (select arrayJoin([1]) as val);
