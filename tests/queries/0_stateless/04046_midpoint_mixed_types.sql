-- Regression test: midpoint with mixed Int64/UInt64 arguments must not cause
-- "Unexpected return type" LOGICAL_ERROR during constant folding.
-- The bug was that the binary arithmetic fast-path used NumberTraits::ResultOfIf
-- (which widens Int64+UInt64 to Int128) while getReturnTypeImpl used
-- getLeastSupertype (which folds UInt64 literals fitting Int64 back to Int64).

SELECT midpoint(toInt64(0), 9223372036854775806);
SELECT midpoint(intDiv(0.5, 1048576), 9223372036854775806);
SELECT toTypeName(midpoint(toInt64(0), 9223372036854775806));
SELECT midpoint(toInt32(0), toUInt32(0));
SELECT toTypeName(midpoint(toInt32(0), toUInt32(0)));
