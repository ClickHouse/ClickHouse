-- Scaling a timespan by an exact number stays exact, and a tabular parameter's declared
-- schema is the only thing its function body sees.

SELECT '-- an exact factor scales a timespan without going through Float64 --';
SELECT kqlMultiply(toIntervalNanosecond(86400000000000), toDecimal64('0.57', 2));
SELECT kqlMultiply(toDecimal64('0.57', 2), toIntervalNanosecond(86400000000000));
SELECT kqlToTimespan(toDecimal64('0.57', 2));
SELECT kqlMultiply(toIntervalNanosecond(1), toInt128('9007199254740993'));
SELECT kqlMultiply(toIntervalNanosecond(2000), 1.5);
-- A fraction of a nanosecond truncates toward zero, as the Float64 path did.
SELECT kqlToTimespan(toDecimal64('0.00000000001', 11));

SELECT '-- a wide integer range is counted exactly --';
SELECT kqlRangeCount(toUInt128(9007199254740993), toUInt128(9007199254740995), toUInt128(1));
SELECT kqlRangeCount(toInt256(-9007199254740995), toInt256(-9007199254740993), toInt256(1));
SELECT kqlRangeCount(1, 7, 2);
SELECT kqlRangeCount(toDecimal64('0.1', 1), toDecimal64('0.3', 1), toDecimal64('0.1', 1));

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- the same exactness through the KQL surface --';
print 1d * decimal(0.57);
print totimespan(decimal(0.57));

print '-- a tabular parameter shows its body only the columns it declares --';
let Above = (T:(a:long), lim:long) { T | where a > lim };
Above(datatable (a:long, b:long) [1, 10, 5, 50, 9, 90], 4);
let Undeclared = (T:(a:long)) { T | project b }; Undeclared(datatable (a:long, b:long) [1, 2]); -- { serverError UNKNOWN_IDENTIFIER }
let Any = (T:(*)) { T | project b };
Any(datatable (a:long, b:long) [1, 2]);
let Empty = (T:()) { T | count }; -- { clientError SYNTAX_ERROR }
let Twice = (T:(a:long, a:long)) { T | count }; -- { clientError SYNTAX_ERROR }

print '-- a tabular function is not a value --';
let Source = datatable (x:long) [1]; let F = () { Source }; datatable (Source:long) [7] | extend y = F; -- { clientError SYNTAX_ERROR }
let Source = datatable (x:long) [1]; let F = () { Source }; print F; -- { clientError SYNTAX_ERROR }
let Source = datatable (x:long) [1]; let F = () { Source }; F;

SET dialect = 'clickhouse';
