-- Tags: use-xray

SYSTEM INSTRUMENT REMOVE ALL;

SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 9.223372036854776e15; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry';

SELECT 'oversized_max_after_rounding';
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 9.223372036854776e15; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.instrumentation WHERE handler = 'sleep' AND entry_type = 'Entry';

SYSTEM INSTRUMENT REMOVE ALL;
