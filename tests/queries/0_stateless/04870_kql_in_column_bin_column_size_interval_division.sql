-- Regression tests for three KQL fixes: a lone unbound name inside `in (...)` reads as a
-- column rather than a physical table, `bin` over a datetime takes a per-row bin size, and
-- a timespan divided by a timespan is their real-valued ratio.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- in (column) is a scalar membership test --';
datatable (x:string, y:string) ['a', 'a', 'b', 'c'] | where x in (y) | project x;
datatable (x:string, y:string) ['a', 'a', 'b', 'c'] | where x !in (y) | project x;

print '-- in (column, literal) mixes columns and literals --';
datatable (x:string, y:string) ['a', 'z', 'b', 'c'] | where x in (y, 'b') | project x;

print '-- a let-bound tabular name and a piped expression still read as tables --';
let T = datatable (k:string) ['a', 'b'];
print v = 'a' | where v in (T) | count;
let S = datatable (k:string) ['a', 'b'];
print v = 'c' | where v in (S | project k) | count;

print '-- bin of a datetime by a timespan column --';
datatable (ts:datetime, step:timespan) [
    datetime(2026-08-01 12:34:56), 1h,
    datetime(2026-08-01 12:34:56), 1d,
    datetime(2026-08-01 12:34:56), 30m
] | project r = bin(ts, step);

print '-- bin of a datetime by a negative or null timespan column is null per row --';
datatable (ts:datetime, step:timespan) [
    datetime(2026-08-01 12:34:56), 1h,
    datetime(2026-08-01 12:34:56), -1h
] | project r = bin(ts, step);

print '-- bin of a datetime by a constant keeps its alignment --';
print bin(datetime(2026-08-01 12:34:56), 1h);
print bin(datetime(2026-08-01 12:34:56), 1d);
print bin(datetime(2026-08-01 12:34:56), 7d);
print bin(datetime(1969-12-30 23:59:59), 1d);
print bin(datetime(null), 1h);

print '-- bin_at still agrees with bin when the fixed point is the epoch --';
print bin_at(datetime(2026-08-01 12:34:56), 1h, datetime(1970-01-01 00:00:00));
print bin_at(datetime(1970-01-01 00:00:00), 2d, datetime(1970-01-02 00:00:00));

print '-- timespan / timespan is a real --';
print 15ms / 10ms;
print 1h / 30m;
print 1d / 5h;
print timespan('12.23:12:23') / 1s;

print '-- timespan / timespan over columns --';
datatable (a:timespan, b:timespan) [15ms, 10ms, 2h, 30m] | project r = a / b;

print '-- a null operand gives a null ratio --';
print timespan(null) / 1h;

print '-- integer and real division are unchanged --';
print 7 / 2;
print 7.0 / 2;

SET dialect = 'clickhouse';

SELECT '-- kqlDivide over intervals from SQL --';
SELECT kqlDivide(toIntervalNanosecond(15000000), toIntervalNanosecond(10000000));
SELECT kqlDivide(toIntervalHour(3), toIntervalMinute(90));
SELECT kqlDivide(toIntervalMonth(1), toIntervalHour(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- kqlBin over a datetime with a non-constant interval from SQL --';
SELECT kqlBin(toDateTime64('2026-08-01 12:34:56', 7, 'UTC'), materialize(toIntervalHour(1)));
SELECT kqlBin(toDateTime64('2026-08-01 12:34:56.123', 3, 'UTC'), toIntervalMicrosecond(700));
