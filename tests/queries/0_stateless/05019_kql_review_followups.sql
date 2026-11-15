SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

-- ClickHouse aggregate names must not bypass KQL's `summarize` requirement.
print uniqExact(1); -- { clientError SYNTAX_ERROR }

SET dialect = 'clickhouse';
SET interval_output_format = 'kusto';

-- Calendar intervals and sub-tick nanosecond intervals are not KQL timespans.
-- Test formatting in the query pipeline so the expected-error annotation can observe it.
SELECT formatRowNoNewline('TSV', toIntervalMonth(1)); -- { serverError BAD_ARGUMENTS }
SELECT formatRowNoNewline('TSV', toIntervalNanosecond(1)); -- { serverError BAD_ARGUMENTS }
SELECT toIntervalNanosecond(100);
