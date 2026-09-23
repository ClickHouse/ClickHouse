SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

-- `extract` returns null when its pattern does not match.
print extract('x', 0, 'y');
-- Timespan conversions and scaling reject values outside the `Int64` nanosecond carrier.
print 2 * 60000d; -- { serverError BAD_ARGUMENTS }
print totimespan(200000); -- { serverError BAD_ARGUMENTS }
-- This union mode is not implemented, so do not accept it as `UNION ALL`.
union kind=outer (print a = 1), (print a = 2); -- { clientError SYNTAX_ERROR }

SET dialect = 'clickhouse';
