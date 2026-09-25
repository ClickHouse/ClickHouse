-- The analyzer must respect short-circuit semantics of `coalesce` and `ifNull` as it does for `if` and `multiIf`:
-- the arguments after the one statically known to be not NULL are unreachable and must not fail constant folding.

SET enable_analyzer = 1;

SELECT coalesce(toNullable(1), intDiv(1, 0));
SELECT ifNull(toNullable(1), intDiv(1, 0));
SELECT coalesce(1, intDiv(1, 0));
SELECT coalesce(NULL, CAST(NULL, 'Nullable(UInt8)'), 2, intDiv(1, 0), intDiv(2, 0));
SELECT COALESCE(toNullable(3), intDiv(1, 0)), IFNULL(toNullable(4), intDiv(1, 0));

-- A dead argument that cannot be resolved at all.
SELECT coalesce(5, not_existing_column);

-- An argument of a type that cannot contain NULL is never NULL.
SELECT coalesce(number, intDiv(1, 0)) FROM numbers(2);
SELECT ifNull(number, intDiv(1, 0)) FROM numbers(2);

-- Inside an expression.
SELECT coalesce(toNullable(6), intDiv(1, 0)) + 1;

-- The argument after a constant NULL or after an argument that can be NULL is reachable.
SELECT coalesce(NULL, intDiv(1, 0)); -- { serverError ILLEGAL_DIVISION }
SELECT ifNull(NULL, intDiv(1, 0)); -- { serverError ILLEGAL_DIVISION }
SELECT coalesce(toNullable(number), intDiv(1, 0)) FROM numbers(1); -- { serverError ILLEGAL_DIVISION }

-- When all the dead arguments are valid, the common supertype of all the arguments is still used.
SELECT toTypeName(coalesce(toNullable(toUInt8(1)), toUInt16(2)));
SELECT toTypeName(ifNull(toNullable(toUInt8(1)), toUInt16(2)));
SELECT toTypeName(coalesce(toUInt8(1), toUInt16(2)));
