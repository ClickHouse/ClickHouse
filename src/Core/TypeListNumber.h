#pragma once

#include <Core/Types.h>
#include <Common/TypeList.h>

namespace DB
{

using TypeListNativeNumbers = TypeList<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>;
using TypeListExtendedNumbers = TypeList<UInt128, Int128, UInt256, Int256>;
using TypeListDecimalNumbers = TypeList<Decimal32, Decimal64, Decimal128, Decimal256>;

using TypeListGeneralNumbers = typename TypeListConcat<TypeListNativeNumbers, TypeListExtendedNumbers>::Type;
using TypeListNumbers = typename TypeListConcat<TypeListGeneralNumbers, TypeListDecimalNumbers>::Type;

using TypeListNumbersAndUUID = typename TypeListConcat<TypeListNumbers, TypeList<UUID>>::Type;

}
