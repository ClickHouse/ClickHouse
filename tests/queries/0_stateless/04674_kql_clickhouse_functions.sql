-- ClickHouse functions are reachable from KQL.
--
-- A name the KQL registry does not know is passed through to ClickHouse under the spelling
-- the user wrote, so a KQL query is not limited to the functions Kusto happens to define.
-- The one thing this must not do is let a *Kusto* name quietly mean something else, which is
-- why the names Kusto defines but this dialect does not implement stay rejected -- see
-- 04673_kql_unsupported_is_rejected.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- plain ClickHouse scalar functions --';
print cityHash64('abc');
print bitCount(255);
print formatReadableSize(1024);
print toStartOfHour(toDateTime('2026-08-01 12:34:56'));
print IPv4NumToString(toUInt32(16909060));

print '-- names are case-sensitive, as they are in SQL --';
print toTypeName(1);
print reinterpretAsUInt8('A');

print '-- they compose with KQL expressions and operators --';
print x = bitCount(255) + strlen('ab');
print p = cityHash64('a') == cityHash64('a');
datatable (S:string) ['abc', 'defg']
| extend Hashed = lengthUTF8(S) * 2
| where Hashed > 6
| project S, Hashed;

print '-- and inside summarize, where ClickHouse aggregates also work --';
datatable (K:string, V:long) ['a', 1, 'a', 5, 'b', 3]
| summarize Middle = medianExact(V), Spread = varPop(V) by K
| sort by K asc;

-- ClickHouse's parametric form `f(p)(x)` has no KQL spelling, so those aggregates are out of
-- reach from KQL; use a named alternative such as `medianExact` above instead of
-- `quantileExact(0.5)`.

print '-- a KQL function still wins over a same-named ClickHouse one --';
-- ClickHouse `substring` counts from 1; Kusto's counts from 0, and Kusto's is what applies.
print substring('abcdefg', 0, 2);
-- ClickHouse `reverse` works on arrays too; here the Kusto string meaning applies.
print reverse('abc');

SET dialect = 'clickhouse';
