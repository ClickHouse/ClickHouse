-- Bug test for PR #97502 (Fix sumCount aggregate backward compat with Nullable args).
-- PR #97502 adds getOwnNullAdapter to sumCount fixing its backward compat, but
-- sumCountIf (via AggregateFunctionIf) still wraps in NullAdapter producing a
-- leading flag byte, so old serialized states (no flag byte) fail to deserialize.
-- sumCountDistinct has the same issue.

-- sumCountIf with non-nullable args: state has no flag byte (9 bytes)
SELECT 'non_nullable_hex', hex(sumCountIfState(x, x > 0))
FROM (SELECT CAST(number, 'UInt8') AS x FROM numbers(5));

-- sumCountIf with Nullable args: state gains a leading flag byte (10 bytes)
SELECT 'nullable_hex', hex(sumCountIfState(x, x > 0))
FROM (SELECT CAST(number, 'Nullable(UInt8)') AS x FROM numbers(5));

-- Old 9-byte state (written before the flag byte was added) fails to deserialize
-- as AggregateFunction(sumCountIf, Nullable(UInt8), UInt8) — backward compat break
SELECT finalizeAggregation(CAST(unhex('0A0000000000000004'), 'AggregateFunction(sumCountIf, Nullable(UInt8), UInt8)')); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- New 10-byte state (with flag byte) deserializes correctly
SELECT 'new_format_works', finalizeAggregation(CAST(unhex('010A0000000000000004'), 'AggregateFunction(sumCountIf, Nullable(UInt8), UInt8)'));

-- Return type for sumCountIf with Nullable args is Nullable(Tuple(UInt64, UInt64))
SELECT 'return_type', toTypeName(sumCountIf(x, x > 0))
FROM (SELECT CAST(number, 'Nullable(UInt8)') AS x FROM numbers(5));

-- Contrast: sumCount itself is fixed by PR #97502 (old format still deserializes)
SELECT 'sumcount_fixed', finalizeAggregation(CAST(unhex('0A0000000000000004'), 'AggregateFunction(sumCount, Nullable(UInt8))'));
