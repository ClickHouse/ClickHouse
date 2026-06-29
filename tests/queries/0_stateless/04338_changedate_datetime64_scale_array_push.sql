-- Regression test for issue #108517. changeYear/changeMonth/changeDay/changeHour/
-- changeMinute/changeSecond over DateTime64(N) used to build the result column with the
-- hardcoded default scale 3 instead of N, so a result declared DateTime64(N != 3) carried a
-- physically scale-3 column. Feeding it into a structure-sensitive consumer (arrayPushBack /
-- arrayPushFront / arrayConcat over Array(DateTime64(N))) skips the cast on declared-type
-- equality and reaches the generic writeSlice, whose ColumnDecimal::structureEquals throws a
-- LOGICAL_ERROR on the scale mismatch. materialize() prevents constant folding so the divergent
-- column actually reaches the array function.

SELECT 'changeSecond scale 7 into arrayPushBack';
SELECT arrayPushBack([toDateTime64('2020-01-01 00:00:00', 7)], changeSecond(materialize(toDateTime64('2020-01-01 12:00:00', 7)), 30));

SELECT 'changeMinute scale 7 into arrayPushFront';
SELECT arrayPushFront([toDateTime64('2020-01-01 00:00:00', 7)], changeMinute(materialize(toDateTime64('2020-01-01 12:00:00', 7)), 45));

SELECT 'changeHour scale 1 into arrayConcat';
SELECT arrayConcat([toDateTime64('2020-01-01 00:00:00', 1)], [changeHour(materialize(toDateTime64('2020-01-01 12:00:00', 1)), 5)]);

SELECT 'changeDay scale 6 into arrayPushBack';
SELECT arrayPushBack([toDateTime64('2020-01-01 00:00:00', 6)], changeDay(materialize(toDateTime64('2020-01-15 12:00:00', 6)), 20));

SELECT 'changeMonth scale 5 into arrayPushFront';
SELECT arrayPushFront([toDateTime64('2020-01-01 00:00:00', 5)], changeMonth(materialize(toDateTime64('2020-01-01 12:00:00', 5)), 6));

SELECT 'result column scale matches declared type';
SELECT toTypeName(changeSecond(materialize(toDateTime64('2020-01-01 12:00:00', 7)), 30));

-- Same producer bug class in timeSlots: getReturnTypeImpl declares
-- Array(DateTime64(max(start_scale, duration_scale))) but executeImpl built the values column
-- at start_scale only, so a duration with a larger scale produced a result column whose physical
-- scale was below its declared type. The same structure-sensitive consumers reach writeSlice and
-- throw. Read the scale from result_type so the column always matches its declared type.

SELECT 'timeSlots start scale 1, duration scale 5 into arrayConcat';
SELECT arrayConcat(timeSlots(materialize(toDateTime64('2012-01-01 12:20:00', 1)), toDecimal64(600, 5)), [toDateTime64('2000-01-01 00:00:00', 5)]);

SELECT 'timeSlots start scale 2, duration scale 6 into arrayPushBack';
SELECT arrayPushBack([toDateTime64('2000-01-01 00:00:00', 6)], (timeSlots(materialize(toDateTime64('2012-01-01 12:20:00', 2)), toDecimal64(600, 6)))[1]);

SELECT 'timeSlots constant start, duration scale 4 column into arrayConcat';
SELECT arrayConcat(timeSlots(toDateTime64('2012-01-01 12:20:00', 1), materialize(toDecimal64(600, 4))), [toDateTime64('2000-01-01 00:00:00', 4)]);

SELECT 'timeSlots result column scale matches declared type';
SELECT toTypeName(timeSlots(materialize(toDateTime64('2012-01-01 12:20:00', 1)), toDecimal64(600, 5)));
