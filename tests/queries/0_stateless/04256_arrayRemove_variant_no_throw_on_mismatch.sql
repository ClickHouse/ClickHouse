-- Regression test for arrayRemove crashing with a logical error
-- (`assertTypeEquality` chassert in `IColumn::insertFrom`) when the first
-- argument is an array of `Variant` whose alternatives are all incompatible
-- with the type of the second argument AND `variant_throw_on_type_mismatch`
-- is disabled. In that case `equals` returns `Nullable(Nothing)` and the
-- previous code propagated the resulting `ColumnNothing` into `FunctionIf`
-- under a declared `UInt8` type, triggering the chassert.
--
-- Trigger pattern: `[NULL, X]` produces `Array(Variant(typeof(X)))`. Pairing
-- it with a literal whose nested type does not match any Variant alternative
-- (e.g. `[['hello']]` vs `Variant(Array(<integers>))`) exercises the path
-- where the comparison cannot succeed.
--
-- Expected behaviour: no element is considered equal to the second argument,
-- so the array is returned unchanged.
SET variant_throw_on_type_mismatch = 0;

-- Variant(Array(UInt32)) — single alternative, no inner truncation.
SELECT arrayRemove([NULL, [257, 65537]], [['hello']]);

-- Variant(Array(Int64)) — Int64 to fit the negative value and the very large one.
SELECT arrayRemove([NULL, [9223372036854775807, -2147483648, 1048576]], [['hello']]);

-- The default `variant_throw_on_type_mismatch = 1` continues to throw a clean
-- exception rather than silently no-op'ing.
SELECT arrayRemove([NULL, [257, 65537]], [['hello']]) SETTINGS variant_throw_on_type_mismatch = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
