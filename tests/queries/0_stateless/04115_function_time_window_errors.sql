-- Error paths and rarely-tested code paths for tumble/tumbleStart/tumbleEnd/hop/hopStart/hopEnd/windowID.

SELECT '--- tumble: error paths ---';
SELECT tumble(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tumble(toDateTime('2020-01-01 00:00:00', 'UTC')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tumble(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 0 SECOND); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT tumble(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL -1 SECOND); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT tumble('not a datetime', INTERVAL 1 HOUR); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tumble(toDateTime('2020-01-01 00:00:00', 'UTC'), 42); -- { serverError ILLEGAL_COLUMN,ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tumble(materialize(toDateTime('2020-01-01 00:00:00', 'UTC')), materialize(INTERVAL 1 HOUR)); -- { serverError ILLEGAL_COLUMN }
SELECT tumble(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 HOUR, 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tumble(toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), INTERVAL 1 HOUR); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- hop: error paths ---';
SELECT hop(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hop(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 SECOND); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hop(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 DAY, INTERVAL 0 DAY); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT hop(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 0 DAY, INTERVAL 1 DAY); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- Mixing date-based intervals with time-based intervals (kinds must match)
SELECT hop(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 SECOND, INTERVAL 1 DAY); -- { serverError ILLEGAL_COLUMN }
SELECT hop('not a datetime', INTERVAL 1 SECOND, INTERVAL 1 HOUR); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- tumbleStart: error paths ---';
SELECT tumbleStart(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- single-arg form requires UInt32 or Tuple(Date/DateTime, Date/DateTime)
SELECT tumbleStart('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tumbleStart(toDateTime('2020-01-01 00:00:00', 'UTC')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tumbleStart((toDateTime('2020-01-01 00:00:00', 'UTC'), 'wrong')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- two-arg form: must validate like tumble
SELECT tumbleStart(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 0 SECOND); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '--- tumbleEnd/hopStart/hopEnd: similar validation ---';
SELECT tumbleEnd('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hopStart('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hopEnd('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hopStart(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 0 DAY, INTERVAL 1 DAY); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT hopEnd(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 0 DAY, INTERVAL 1 DAY); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '--- windowID ---';
-- 2 args (tumble-like)
SELECT windowID(toDateTime('2020-01-02 00:00:01', 'UTC'), INTERVAL 1 HOUR);
-- 4 args (hop-like with timezone)
SELECT windowID(toDateTime('2020-01-02 00:00:01', 'UTC'), INTERVAL 1 HOUR, INTERVAL 3 HOUR, 'UTC');
-- Interval kind mismatch between window and hop
SELECT windowID(toDateTime('2020-01-02 00:00:01', 'UTC'), INTERVAL 1 DAY, INTERVAL 1 HOUR); -- { serverError ILLEGAL_COLUMN }
-- Wrong number of arguments
SELECT windowID(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT windowID(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 SECOND, INTERVAL 2 SECOND, 'UTC', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '--- Tuple extraction via tumbleStart/tumbleEnd pass-through ---';
-- Verify single-arg Tuple form: extracts start/end element
SELECT tumbleStart((toDateTime('2020-01-02 10:00:00', 'UTC'), toDateTime('2020-01-02 11:00:00', 'UTC')));
SELECT tumbleEnd((toDateTime('2020-01-02 10:00:00', 'UTC'), toDateTime('2020-01-02 11:00:00', 'UTC')));
-- Allow Tuple with Date
SELECT tumbleStart((toDate('2020-01-02'), toDate('2020-01-03')));
SELECT tumbleEnd((toDate('2020-01-02'), toDate('2020-01-03')));

SELECT '--- Tuple extraction via hopStart/hopEnd pass-through ---';
SELECT hopStart((toDateTime('2020-01-02 10:00:00', 'UTC'), toDateTime('2020-01-02 11:00:00', 'UTC')));
SELECT hopEnd((toDateTime('2020-01-02 10:00:00', 'UTC'), toDateTime('2020-01-02 11:00:00', 'UTC')));

SELECT '--- tumble result types for all interval kinds ---';
SELECT toTypeName(tumble(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 SECOND, 'UTC'));
SELECT toTypeName(tumble(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 WEEK, 'UTC'));
SELECT toTypeName(tumble(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 MONTH, 'UTC'));
SELECT toTypeName(tumble(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 QUARTER, 'UTC'));
SELECT toTypeName(tumble(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 YEAR, 'UTC'));
