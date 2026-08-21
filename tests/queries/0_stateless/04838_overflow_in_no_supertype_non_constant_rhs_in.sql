-- Out-of-range numeric elements in a non-constant RHS of IN.
-- When a least supertype of the LHS and all RHS elements exists, the row-wise rewrite compares in
-- the supertype, so out-of-range elements never match - same as the constant Set path, which skips
-- values that do not fit the LHS type.
-- When no supertype exists (mixed numeric/string RHS), the row-wise rewrite falls back to casting
-- each element to the LHS type with plain CAST, mirroring the same pre-existing fallback of the
-- analyzer, so an out-of-range numeric element wraps (1000 -> 232 as UInt8). The constant Set path
-- has no answer for this class at all - it throws TYPE_MISMATCH.

-- { echoOn }

SELECT toUInt8(232) IN (1000, 'x'); -- { serverError TYPE_MISMATCH }
SELECT toUInt8(232) IN (1000, 'x') SETTINGS transform_null_in = 1; -- { serverError TYPE_MISMATCH }
SELECT toUInt8(232) IN (1000, 500);
SELECT toUInt8(232) NOT IN (1000, 500);

SET enable_analyzer = 1;

-- a supertype exists: compared as UInt16, no wrap-around
SELECT toUInt8(232) IN (materialize(1000), 500);
SELECT toUInt8(232) NOT IN (materialize(1000), 500);
SELECT toUInt8(232) IN (materialize(1000), 500) SETTINGS transform_null_in = 1;
SELECT toUInt8(232) IN (materialize(232), 500);
-- no supertype: cast-to-LHS-type fallback, identical to the analyzer on master
SELECT toUInt8(232) IN (materialize(1000), 'x');
SELECT toUInt8(232) NOT IN (materialize(1000), 'x');
SELECT toUInt8(232) IN (materialize(1000), 'x') SETTINGS transform_null_in = 1; -- { serverError CANNOT_PARSE_TEXT }

SET enable_analyzer = 0;

SELECT toUInt8(232) IN (materialize(1000), 500);
SELECT toUInt8(232) NOT IN (materialize(1000), 500);
SELECT toUInt8(232) IN (materialize(1000), 500) SETTINGS transform_null_in = 1;
SELECT toUInt8(232) IN (materialize(232), 500);
SELECT toUInt8(232) IN (materialize(1000), 'x');
SELECT toUInt8(232) NOT IN (materialize(1000), 'x');
SELECT toUInt8(232) IN (materialize(1000), 'x') SETTINGS transform_null_in = 1; -- { serverError CANNOT_PARSE_TEXT }
