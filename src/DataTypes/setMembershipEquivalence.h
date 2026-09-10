#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/** `IN` matches by set membership while `equals` compares by value, and the two relations disagree on
  * floating-point NaN and signed zero: `nan = nan` is 0 while `nan IN (nan)` is 1, and `-0.0 = 0.0` is 1
  * while `-0.0 IN (0.0)` is 0. Folding such a comparison into `IN`/`NOT IN` would silently change the
  * result, so it has to stay a comparison.
  *
  * Returns whether comparing an expression of `expression_type` with `constant_value` is the same relation
  * as probing a set that holds this constant. Floating-point values are also reachable through compound
  * carriers: `equals` on a `Tuple` or an `Array` is evaluated element-wise, while `IN` hashes the raw bits
  * of every element, so the check recurses into the constant. A compound carrier of floats whose structure
  * does not match the constant is not analyzed and is reported as diverging.
  */
bool comparisonWithConstantMatchesSetMembership(const DataTypePtr & expression_type, const Field & constant_value);

/** Whether a floating-point value anywhere in `constant_value` is NaN or zero, which is where `equals` and
  * set membership disagree. This is the value half of `comparisonWithConstantMatchesSetMembership`, for
  * callers that do not know the type of the compared expression (the legacy AST rewrite).
  *
  * A constant is compared in the domain of the expression, so an integer zero reaches a floating-point
  * comparison as `+0.0` as well; only a value that is provably a non-zero number is reported as safe.
  */
bool constantMayHoldFloatNaNOrZero(const Field & constant_value);

/// Whether a floating-point type appears anywhere in the type, including nested inside Array/Tuple/Map.
bool hasFloat(const DataTypePtr & type);

}
