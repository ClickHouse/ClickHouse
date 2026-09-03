-- Regressions for three review findings: `range` over integers must count exactly (a
-- `Float64` cannot tell integers above 2^53 apart), aggregate functions are rejected
-- outside the aggregation list of `summarize`, and a `let` may bind a bare physical table
-- name (`let T = Events;`), also through a parameterless function body.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- range counts long bounds exactly above 2^53 --';
range x from 9007199254740993 to 9007199254740994 step 1 | count;
range x from 9007199254740993 to 9007199254740994 step 1 | summarize mn = min(x), mx = max(x);
range x from -9223372036854775807 to 9223372036854775807 step 9223372036854775807 | count;
range x from -9223372036854775807 to 9223372036854775807 step 9223372036854775807 | summarize s = sum(x);

print '-- the floor still rounds toward minus infinity --';
range x from -5 to 5 step 3 | count;
range x from 5 to -5 step -3 | count;
range x from 5 to 1 step 1 | count;

print '-- real ranges still work --';
range x from 1.0 to 2.2 step 0.5 | count;

print '-- aggregates only in the aggregation of a summarize --';
print count(); -- { clientError SYNTAX_ERROR }
print s = sum(1); -- { clientError SYNTAX_ERROR }
datatable (x:long) [1] | extend y = sum(x); -- { clientError SYNTAX_ERROR }
datatable (x:long) [1] | where max(x) > 0; -- { clientError SYNTAX_ERROR }
datatable (x:long) [1, 2] | summarize c = count() by k = sum(x); -- { clientError SYNTAX_ERROR }
datatable (x:long) [1] | summarize s = max(iff(x in (datatable (y:long) [1] | where sum(y) > 0 | project y), 1, 0)); -- { clientError SYNTAX_ERROR }

print '-- ... where they still work, also composed and nested --';
datatable (x:long) [1, 2, 3] | summarize t = sum(x) * 2;
print v = 3 | where v in (datatable (y:long) [1, 2, 3] | summarize m = max(y) | project m) | count;

SET dialect = 'clickhouse';

DROP TABLE IF EXISTS events_04836;
CREATE TABLE events_04836 (n Int64) ENGINE = Memory;
INSERT INTO events_04836 VALUES (1), (2), (3);

SET dialect = 'kusto';

print '-- let binds a bare physical table --';
let T = events_04836;
T | count;

let T = events_04836;
T | summarize s = sum(n);

print '-- also through a parameterless function body --';
let F = () { events_04836 };
let T = F;
T | count;

print '-- a bare scalar binding stays scalar --';
let a = 5;
let b = a;
print b;

let t = true;
print t;

print '-- in (Table) reads the first column when the name is bound tabular --';
let E = events_04836;
print v = 2 | where v in (E) | count;

SET dialect = 'clickhouse';

SELECT '-- kqlRangeCount: exact, and a constant nullable argument --';
SELECT kqlRangeCount(9007199254740993, 9007199254740994, 1);
SELECT kqlRangeCount(0::UInt64, 18446744073709551615::UInt64, 18446744073709551615::UInt64);
SELECT kqlRangeCount(materialize(1), toNullable(5), 1);
SELECT kqlRangeCount(materialize(1), CAST(NULL, 'Nullable(Int64)'), 1); -- { serverError BAD_ARGUMENTS }

DROP TABLE events_04836;
