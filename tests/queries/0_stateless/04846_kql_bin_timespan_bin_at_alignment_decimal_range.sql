-- Regressions for `bin` / `floor` / `bin_at` over timespans, datetime `bin_at` aligned to a
-- fixed point after the value, exact `Decimal` counting in `range`, and the wildcard
-- rejection in `project-away` / `project-keep`.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- bin and two-argument floor over timespans --';
print bin(16d, 7d);
print floor(16d, 7d);
print bin(2h, 1h);
print bin(-11h, 1d);
print bin(time('0.01:02:03'), 1h);
print isnull(bin(1h, -1h));
print bin(1h, 7); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

print '-- bin_at over timespans --';
print bin_at(1h, 1d, 12h);
print bin_at(time(1h), 1d, 12h);
print bin_at(-16d, 7d, 3d);
print bin_at(90m, 1h, 30m);
print isnull(bin_at(1h, -1h, 30m));
print bin_at(1h, 1d, 5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

print '-- datetime bin_at with the fixed point after the value --';
print bin_at(datetime(2017-05-15 10:20:00), 1d, datetime(2017-05-15 12:00:00));
print bin_at(datetime(2017-05-14 11:59:00), 1d, datetime(2017-05-15 12:00:00));
print bin_at(datetime(2017-05-17 10:20:00), 1d, datetime(2017-05-15 12:00:00));
print bin_at(datetime(2026-08-01 12:34:56), 1h, datetime(2026-08-01 00:30:00));
print isnull(bin_at(datetime(2017-05-15 10:20:00), -1d, datetime(2017-05-15 12:00:00)));

print '-- exact decimal range counting --';
range x from decimal(0.1) to decimal(0.3) step decimal(0.1) | count;
range x from decimal(0.1) to decimal(0.3) step decimal(0.1) | summarize sum(x);
range x from 1 to decimal(2.5) step decimal(0.5) | count;
range x from decimal(0.3) to decimal(0.1) step decimal(-0.1) | count;
range x from decimal(0.1) to decimal(0.3) step decimal(0.0) | count; -- { serverError BAD_ARGUMENTS }

print '-- wildcards in project-away and project-keep are rejected --';
datatable(foo: long, foobar: long, col: long) [1, 2, 3] | project-away foo*; -- { clientError SYNTAX_ERROR }
datatable(foo: long, foobar: long, col: long) [1, 2, 3] | project-keep col*; -- { clientError SYNTAX_ERROR }
datatable(foo: long, foobar: long, col: long) [1, 2, 3] | project-away foo;
datatable(foo: long, foobar: long, col: long) [1, 2, 3] | project-keep col;
