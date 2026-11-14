-- A materialized, typed all-NULL condition such as `materialize(CAST(NULL AS Nullable(UInt8)))` is NOT a
-- constant condition: `ColumnNullable::onlyNull` is `nested_column->isDummy()`, which only holds for
-- `Nullable(Nothing)`, so `executeForConstAndNullableCondition` does not short-circuit to the else branch
-- and the result is a full (non-constant) column. `LowCardinality` is therefore the right result type here,
-- and this is not the "whole result is constant" case that `getReturnTypeImpl` deliberately keeps as plain
-- `String` (see 04628 / 04650 / 04655).

SET optimize_if_transform_strings_to_enum = 0;

SET optimize_if_transform_const_strings_to_lowcardinality = 1;

-- Typed NULL condition: non-constant result, so the optimisation applies.
SELECT toTypeName(if(materialize(CAST(NULL AS Nullable(UInt8))), 'sum', 'max'));
SELECT isConstant(if(materialize(CAST(NULL AS Nullable(UInt8))), 'sum', 'max'));
SELECT if(materialize(CAST(NULL AS Nullable(UInt8))), 'sum', 'max') FROM numbers(2);

-- A `Nullable(Nothing)` condition is always NULL and the result is constant, so it stays plain `String`.
SELECT toTypeName(if(materialize(NULL), 'sum', 'max'));
SELECT isConstant(if(materialize(NULL), 'sum', 'max'));

-- The same for `multiIf`.
SELECT toTypeName(multiIf(materialize(CAST(NULL AS Nullable(UInt8))), 'sum', 'max'));
SELECT toTypeName(multiIf(materialize(NULL), 'sum', 'max'));

SET optimize_if_transform_const_strings_to_lowcardinality = 0;

-- With the optimisation off the result type is plain `String`, but the constness is unchanged: the typed
-- NULL condition still produces a non-constant column, and the values are identical.
SELECT toTypeName(if(materialize(CAST(NULL AS Nullable(UInt8))), 'sum', 'max'));
SELECT isConstant(if(materialize(CAST(NULL AS Nullable(UInt8))), 'sum', 'max'));
SELECT if(materialize(CAST(NULL AS Nullable(UInt8))), 'sum', 'max') FROM numbers(2);
SELECT toTypeName(multiIf(materialize(CAST(NULL AS Nullable(UInt8))), 'sum', 'max'));

-- Consumers that require a constant string argument (`arrayReduce`, `joinGet`, `tupleElement`) reject a
-- typed NULL condition with and without the optimisation alike - this is pre-existing behaviour, not a
-- regression introduced by returning `LowCardinality`.
SELECT arrayReduce(if(materialize(CAST(NULL AS Nullable(UInt8))), 'sum', 'max'), [1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SELECT arrayReduce(if(materialize(CAST(NULL AS Nullable(UInt8))), 'sum', 'max'), [1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
