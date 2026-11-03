-- Nullable timespans must retain their interval overloads, and typed literals in a dynamic
-- array must fold to values before the array literal is constructed.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- nullable timespan multiplication --';
print timespan(null) * 2, 1h * long(null);

print '-- nullable totimespan --';
print totimespan(timespan(null)), totimespan(string(null)), totimespan(long(null));

print '-- typed dynamic literals --';
print dynamic([timespan(1d)]);
print totimespan(dynamic([timespan(1d)])[0]);
print dynamic([datetime(2026-08-15 12:34:56)]);
print dynamic([guid(74be27de-1e4e-49d9-b579-fe0b331d3642)]);
print dynamic([long(1)]);
