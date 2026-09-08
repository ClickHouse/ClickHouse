-- Regressions for KQL join kinds, join key qualification, `let` rebinding and `bin_at`.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- the default join is innerunique: one left row per key --';
datatable (K:long, L:string) [1, 'a', 1, 'b', 2, 'c']
| join (datatable (K:long, R:string) [1, 'p', 1, 'q', 2, 'r']) on K
| sort by K asc, R asc;

print '-- kind=inner keeps the duplicated left rows --';
datatable (K:long, L:string) [1, 'a', 1, 'b', 2, 'c']
| join kind=inner (datatable (K:long, R:string) [1, 'p', 1, 'q', 2, 'r']) on K
| sort by K asc, L asc, R asc;

print '-- $left/$right keys stay side-qualified when both sides expose both names --';
datatable (a:long, b:long) [1, 10, 2, 20]
| join kind=inner (datatable (a:long, b:long) [10, 1, 20, 2]) on $left.a == $right.b
| summarize matches = count();

print '-- let rebinds a tabular result --';
let T = datatable (N:long) [1, 2, 3];
let U = T;
U | where N > 1 | sort by N asc;

print '-- let binds the result of a tabular function call --';
let Base = datatable (N:long) [1, 2, 3, 4];
let Tail = (cutoff: long) { Base | where N > cutoff };
let Q = Tail(2);
Q | sort by N asc;

print '-- while a scalar function call bound by let stays scalar --';
let Double = (v: long) { v * 2 };
let x = Double(21);
print x;

print '-- an oversized string-form timespan fails cleanly --';
print timespan('99999999999999999.00:00:00');   -- { clientError BAD_ARGUMENTS }

print '-- bin_at --';
print bin_at(6.5, 2.5, -0.5);
print bin_at(17, 5, 1);
print bin_at(datetime(2017-05-17 10:20:00), 1d, datetime(2017-05-14 12:00:00));
print bin_at(long(null), 2, 1);
print bin(4.5, 2) == bin_at(4.5, 2, 0);

print '-- bin propagates null --';
print bin(long(null), 2);

SET dialect = 'clickhouse';

-- The function-property fuzzer found `kqlBin` over `Nullable` big integers answering with a
-- logical error: the delegated `multiply` was built over the `Nullable` types but executed
-- over columns the default null implementation had already stripped.
SELECT kqlBin(CAST(308762132851165525175899756 AS Nullable(UInt128)), CAST(14 AS UInt8));
SELECT kqlBin(materialize(CAST(10.5 AS Nullable(Float64))), 3);
SELECT kqlBin(CAST(NULL AS Nullable(Float64)), 2);
SELECT kqlBinAt(6.5, 2.5, -0.5);
