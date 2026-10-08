-- Regressions for the `range` source going over datetimes and timespans, not only numbers,
-- and for negative constants inside a `dynamic([...])` literal (`-1` parses as `negate`
-- over a literal, which used to be rejected as a non-constant).

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- a negative constant is a constant of a dynamic literal --';
print dynamic([-1]);
print dynamic([1, -2.5]);
print dynamic([[1, -2], [-3]]);
print array_length(dynamic([-1, -2, -3]));

print '-- but an expression still is not --';
print dynamic([1 + 2]); -- { clientError SYNTAX_ERROR }

print '-- a numeric range works as before --';
range x from 1 to 5 step 1 | summarize c = count(), s = sum(x);
range x from 0 to 10 step 5;
range x from 5 to 1 step -2;
range x from 5 to 1 step 2 | count;
range x from 1.0 to 2.0 step 0.5 | count;

print '-- a range of datetimes steps by a timespan --';
range d from datetime(2026-01-01) to datetime(2026-01-03) step 1d;
range d from datetime(2026-01-01 00:00:00) to datetime(2026-01-01 02:00:00) step 30m | summarize c = count();
range d from datetime(2026-01-01 00:00:00) to datetime(2026-01-01 00:00:01) step 250ms | count;
range d from datetime(2026-01-02) to datetime(2026-01-01) step 1d | count;

print '-- a range of timespans steps by a timespan --';
range t from 1h to 3h step 30m | summarize c = count(), mn = min(t), mx = max(t);
range t from 3h to 1h step -1h;
range t from timespan(1) to timespan(3) step 1d | count;

print '-- the bounds and the step must agree, and the step must move --';
range x from 1 to 5 step 0; -- { serverError BAD_ARGUMENTS }
range x from 1 to 5 step 1h; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
range t from 1h to 2h step 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
range d from datetime(2026-01-01) to datetime(2026-01-02) step 5; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
range d from datetime(2026-01-01) to datetime(2026-01-02) step 0h; -- { serverError BAD_ARGUMENTS }
range x from long(null) to 5 step 1; -- { serverError BAD_ARGUMENTS }
