-- Out-of-range DateTime64 arguments used to trigger a signed integer overflow (undefined behavior)
-- in DateLUTImpl::toStableRelativeHourNum / toRelativeHourNum / toRelativeMinuteNum.
-- The wrapped-around result is meaningless but must not be reported as undefined behavior.

-- toStableRelativeHourNum (used by dateDiff and age): the reported case.
SELECT dateDiff('hour', toDateTime64(0, 0, 'UTC'), reinterpret(9223372036854775807, 'DateTime64(0)'), 'UTC');
SELECT age('hour', toDateTime64(0, 0, 'UTC'), reinterpret(9223372036854775807, 'DateTime64(0)'), 'UTC');

-- toRelativeMinuteNum (used by dateDiff('minute', ...)).
SELECT dateDiff('minute', toDateTime64(0, 0, 'UTC'), reinterpret(9223372036854775807, 'DateTime64(0)'), 'UTC');

-- toRelativeHourNum / toRelativeMinuteNum standard precision. A fractional-offset time zone
-- forces the slow code path even for a positive argument.
SELECT toRelativeHourNum(reinterpret(9223372036854775807, 'DateTime64(0)'), 'Asia/Kolkata');
SELECT toRelativeMinuteNum(reinterpret(9223372036854775807, 'DateTime64(0)'), 'Asia/Kolkata');
