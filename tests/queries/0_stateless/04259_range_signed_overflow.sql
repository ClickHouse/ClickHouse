-- range(start, end, step) used to do an unconditional `value += step` after
-- writing the last element, which signed-overflows for valid runs at the
-- high/low end of the type's range (UB, miscompiles allowed). The fix peels
-- the last iteration so the trailing increment is skipped.
--
-- Each call below is constructed so that the LAST emitted element + step
-- would overflow the target type. Without the fix, UBSan flags this as
-- "signed integer overflow". The output is still observable on non-UBSan
-- builds because no actual user-visible miscompile happens on x86, so this
-- test doubles as a value-correctness regression test.

-- Forward direction at Int64::max.
SELECT range(toInt64(9223372036854775806), toInt64(9223372036854775807), toInt64(2));
SELECT range(toInt64(9223372036854775804), toInt64(9223372036854775807), toInt64(2));

-- Forward direction at Int32::max.
SELECT range(toInt32(2147483646), toInt32(2147483647), toInt32(2));

-- Reverse direction at Int64::min.
SELECT range(toInt64(-9223372036854775807), toInt64(-9223372036854775808), toInt64(-2));
SELECT range(toInt64(-9223372036854775805), toInt64(-9223372036854775808), toInt64(-2));

-- Ordinary forward and reverse to verify the inner loop still works.
SELECT range(toInt64(1), toInt64(10), toInt64(3));
SELECT range(toInt64(10), toInt64(1), toInt64(-3));

-- Empty range.
SELECT range(toInt64(5), toInt64(5), toInt64(1));
