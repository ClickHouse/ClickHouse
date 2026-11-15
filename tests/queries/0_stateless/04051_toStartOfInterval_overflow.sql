-- Regression test: toStartOfInterval with large interval values should throw instead of UB (signed integer overflow).
-- Use a pre-epoch (negative t) DateTime64 to exercise the else branch where master lacked the mulOverflow check.
SELECT toStartOfInterval(toDateTime64('1900-01-01 00:00:00', 9), toIntervalMillisecond(9223372036854775806)); -- { serverError DECIMAL_OVERFLOW }
SELECT toStartOfInterval(toDateTime64('1900-01-01 00:00:00', 9), toIntervalMicrosecond(9223372036854775806)); -- { serverError DECIMAL_OVERFLOW }
