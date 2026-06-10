-- Regression test for signed integer overflow in std::condition_variable::wait_for.
-- A huge interactive_delay made the lazy-output queue wait with a millisecond timeout that overflowed
-- when libc++ converts the duration to nanoseconds (multiplying by 1'000'000) inside wait_for. UBSan
-- aborts on that overflow (CI STID 2881-3dbd); without sanitizers it silently wraps to a garbage
-- timeout. The timeout is now clamped, so the query must just return its result.
SELECT 1 SETTINGS interactive_delay = 100000000000000000;
