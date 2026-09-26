-- Regression test: parsing datetime strings with many fractional digits must not cause
-- signed integer overflow (UB) in readDecimalNumber. Fractional digits are capped at
-- digits10 of the result type (Int64::digits10 = 18).

-- 18 fractional digits (at the cap) - parses without truncation
SELECT parseDateTime64BestEffort('1596752940.123456789012345678', 6, 'UTC');
SELECT parseDateTime64BestEffort('100000000.123456789012345678', 6, 'UTC');
SELECT parseDateTime64BestEffort('2020-08-07 01:29:00.123456789012345678', 6, 'UTC');

-- 19 fractional digits with a value that overflows Int64 without the cap:
-- readDecimalNumber processes chunks 4+4+4+4+3; at the last step
-- 9999999999999999 * 1000 overflows Int64, previously causing UB.
-- With the fix the digit count is capped to 18 (chunk 4+4+4+4+2) and the
-- 19th digit is silently dropped.
SELECT parseDateTime64BestEffort('1596752940.9999999999999999999', 6, 'UTC');
SELECT parseDateTime64BestEffort('100000000.9999999999999999999', 6, 'UTC');
SELECT parseDateTime64BestEffort('2020-08-07 01:29:00.9999999999999999999', 6, 'UTC');

-- 20+ fractional digits: the 20th digit is left in the stream after readDigits
-- exhausts its buffer (UInt64::digits10 = 19), causing a parse error
SELECT parseDateTime64BestEffortOrNull('1596752940.12345678901234567890', 6, 'UTC');
SELECT parseDateTime64BestEffortOrNull('100000000.12345678901234567890', 6, 'UTC');
SELECT parseDateTime64BestEffortOrNull('2020-08-07 01:29:00.12345678901234567890', 6, 'UTC');
