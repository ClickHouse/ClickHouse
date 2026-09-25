#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/** Whether hoisting `constant` out of a binary arithmetic operation with an argument of `argument_type`
  * would change the value the operation is computed with.
  *
  * A `Decimal` operand makes the arithmetic compute in the decimal's own native signed width
  * (`Int32` for every `Decimal32`, `Int64` for every `Decimal64`, and so on), into which the other
  * operand is materialised by a `static_cast`. A value outside that width participates as a
  * different value: `Decimal32 * 9223372036854775807` multiplies by `-1`, `Decimal32 + 4294967296`
  * adds `0`, and an `Int64` column combined with a `Decimal32` constant wraps around for every row
  * above `2^31 - 1`.
  *
  * An optimization that moves the operation out of an aggregate function must not fire then: it
  * would compute the operation in the wider result type of the aggregate, where the truncation
  * does not happen, and the result differs. Both operand orders are screened: a `Decimal` argument
  * with a constant its native width cannot represent, and an integer argument wider than the native
  * width of a `Decimal` constant.
  */
bool operandTruncatesIntoDecimalWidth(const DataTypePtr & argument_type, const DataTypePtr & constant_type, const Field & constant);

}
