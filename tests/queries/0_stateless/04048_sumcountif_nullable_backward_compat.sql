-- Regression test for sumCountIf backward compat with Nullable args.
-- Originally this test documented the broken behavior after Nullable(Tuple) was introduced.
-- Now it verifies the fix: the If and Distinct combinators preserve the old state layout.

-- sumCountIf with non-nullable args: state has no flag byte (9 bytes)
SELECT 'non_nullable_hex', hex(sumCountIfState(x, x > 0))
FROM (SELECT CAST(number, 'UInt8') AS x FROM numbers(5));

-- sumCountIf with Nullable args: previously gained a leading flag byte (10 bytes),
-- now fixed to produce the same 9-byte format as non-nullable.
SELECT 'nullable_hex', hex(sumCountIfState(x, x > 0))
FROM (SELECT CAST(number, 'Nullable(UInt8)') AS x FROM numbers(5));

-- Old 9-byte state (written before Nullable(Tuple) was introduced): previously failed
-- with ATTEMPT_TO_READ_AFTER_EOF, now deserializes correctly.
SELECT 'old_format_works', finalizeAggregation(CAST(unhex('0A0000000000000004'), 'AggregateFunction(sumCountIf, Nullable(UInt8), UInt8)'));

-- 10-byte state with flag byte (produced by the broken intermediate version): now rejected
-- because the fixed serialization no longer expects the flag byte.
SELECT finalizeAggregation(CAST(unhex('010A0000000000000004'), 'AggregateFunction(sumCountIf, Nullable(UInt8), UInt8)')); -- { serverError BAD_ARGUMENTS }

-- Return type: previously Nullable(Tuple(UInt64, UInt64)), now Tuple(UInt64, UInt64)
-- because the legacy no-flag adapter does not wrap the result in Nullable.
SELECT 'return_type', toTypeName(sumCountIf(x, x > 0))
FROM (SELECT CAST(number, 'Nullable(UInt8)') AS x FROM numbers(5));

-- Contrast: sumCount itself is also fixed (old format still deserializes).
SELECT 'sumcount_fixed', finalizeAggregation(CAST(unhex('0A0000000000000004'), 'AggregateFunction(sumCount, Nullable(UInt8))'));
