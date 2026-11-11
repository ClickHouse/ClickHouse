-- Tags: use-xray

-- https://github.com/ClickHouse/ClickHouse/pull/103854#discussion_r3235932168

SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1 2; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[0,1,2]';

SELECT 'min_greater_than_max';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 2 1; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[2,1]';

SELECT 'negative_duration';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY -1; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[-1]';

SELECT 'negative_min';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY -1 1; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[-1,1]';

SELECT 'negative_max';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 -1; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[0,-1]';

SELECT 'nan_duration';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY nan; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[nan]';

SELECT 'inf_duration';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY inf; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[inf]';

SELECT 'oversized_duration';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 1e20; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[100000000000000000000]';

SELECT 'rounded_boundary_duration';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 9223372036854776; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[9223372036854776]';

SELECT 'nan_min';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY nan 1; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[nan,1]';

SELECT 'inf_max';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 inf; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[0,inf]';

SELECT 'oversized_max';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1e20; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[0,100000000000000000000]';

SELECT 'rounded_boundary_range';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 9223372036854776 9223372036854776; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry' AND toString(arguments) = '[9223372036854776,9223372036854776]';
