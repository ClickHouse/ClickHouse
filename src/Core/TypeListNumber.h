#pragma once
#include <Core/Types.h>
#include <Common/TypeList.h>

namespace DB
{

using TypeListNativeNumbers = TypeList<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>;
using TypeListDecimalNumbers = TypeList<Decimal32, Decimal64, Decimal128>;
using TypeListNumbers = TypeList<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64,
    Decimal32, Decimal64, Decimal128>;

}
