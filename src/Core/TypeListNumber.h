#pragma once

#include <Core/Types.h>
#include <Common/UInt128.h>
#include <Common/TypeList.h>

namespace DB
{

using TypeListNativeNumbers = TypeList<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>;
using TypeListDecimalNumbers = TypeList<Decimal32, Decimal64, Decimal128>;
using TypeListNumbers = typename TypeListConcat<TypeListNativeNumbers, TypeListDecimalNumbers>::Type;

/// Currently separate because UInt128 cannot be used in every context where other numbers can be used.
using TypeListNumbersAndUInt128 = typename AppendToTypeList<UInt128, TypeListNumbers>::Type;

}
