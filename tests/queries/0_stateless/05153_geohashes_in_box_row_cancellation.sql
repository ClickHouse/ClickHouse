-- geohashesInBox expands one input row into up to 10,000,000 geohashes inside a single call, and that
-- call used to hold no cancellation checkpoint: the only one was in the caller, before the row it
-- charged for, so a single row ignored max_execution_time for the whole of its expansion. The check
-- now runs inside the expansion, at least once per 65,536 geohashes.
--
-- The box below expands to more than one such interval, and to less than the 100,000 items the
-- caller's own accumulator needed before it looked at the clock, which is exactly the window in which
-- one row was uninterruptible. Every argument is a constant, so the function runs while the query is
-- being analysed rather than inside a pipeline; there the timeout reaches the client, while a
-- pipeline turns it into a cancelled query that reports nothing.

SELECT length(geohashesInBox(toFloat64(0), toFloat64(0),
    toFloat64(300 * 360 / pow(2, 30)), toFloat64(300 * 180 / pow(2, 30)), toUInt8(12)));

SELECT length(geohashesInBox(toFloat64(0), toFloat64(0),
    toFloat64(300 * 360 / pow(2, 30)), toFloat64(300 * 180 / pow(2, 30)), toUInt8(12)))
SETTINGS max_execution_time = 0.00001, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }

-- Writing the offsets of one row's geohashes is a second unbounded loop, charged as well. The box below
-- is narrower than one interval, so its 40,000 encodes reach no checkpoint on their own and it can only
-- raise once writing the offsets is charged too.

SELECT length(geohashesInBox(toFloat64(0), toFloat64(0),
    toFloat64(200 * 360 / pow(2, 30)), toFloat64(200 * 180 / pow(2, 30)), toUInt8(12)));

SELECT length(geohashesInBox(toFloat64(0), toFloat64(0),
    toFloat64(200 * 360 / pow(2, 30)), toFloat64(200 * 180 / pow(2, 30)), toUInt8(12)))
SETTINGS max_execution_time = 0.00001, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
