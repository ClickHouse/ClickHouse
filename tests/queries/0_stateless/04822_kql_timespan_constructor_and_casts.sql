-- Regressions for the documented `timespan(...)` constructor forms beyond the glued literal,
-- for `totimespan()` being a cast that takes an arbitrary expression, and for `make_timespan`
-- validating its clock fields instead of rolling an overflow into the next one.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- a bare number in timespan()/time() counts days --';
print timespan(2);
print time(2);
print timespan(0.5);
print timespan(-2);
print '-- the unit may be a separate word --';
print timespan(15 seconds);
print timespan(2.5 hours);
print time(1 tick);
print '-- the printed form may come unquoted --';
print timespan(0.12:34:56.7);
print timespan(12:34:56);
print '-- and the literal forms still work --';
print timespan(1d);
print timespan('0.00:01:00');
print timespan(null);
print '-- totimespan() takes an expression, not only a literal --';
print x = 2 | extend t = totimespan(x * 1h) | project t;
print x = 2 | extend t = totimespan(1h * x) | project t;
print s = '1.02:03:04.5' | extend t = totimespan(s) | project t;
print totimespan(2);
print totimespan(1.5);
print totimespan(strcat('0.00:', '02', ':00'));
print '-- an unreadable string casts to null, not an error --';
print totimespan('abc');
print s = 'garbage' | extend t = totimespan(s) | project t;
print totimespan('');
print totimespan(null);
print '-- timespans scale by a number on either side --';
print 2 * 1h;
print 1h * 2.5;
print x = 3 | project p = x * 4;
print '-- make_timespan validates its clock fields --';
print make_timespan(1, 12);
print make_timespan(23, 59);
print make_timespan(1, 23, 59, 59.5);
print make_timespan(25, 0);
print make_timespan(24, 0);
print make_timespan(0, 61);
print make_timespan(0, 60);
print make_timespan(0, 0, 61);
print make_timespan(0, 0, 60);
print make_timespan(-1, 0, 0, 0);
print make_timespan(0, -1, 0);
print '-- constructor mistakes still fail loudly --';
print timespan(15 parsecs); -- { clientError BAD_ARGUMENTS }
print timespan(2 + 1); -- { clientError BAD_ARGUMENTS }
