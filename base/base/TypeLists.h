#pragma once

#include "TypeList.h"
#include "extended_types.h"
#include "Decimal.h"
#include "UUID.h"

namespace DB
{

using TypeListNativeInt = TypeList<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64>;
using TypeListFloat = TypeList<Float32, Float64>;
using TypeListNativeNumber = TypeListConcat<TypeListNativeInt, TypeListFloat>;
using TypeListWideInt = TypeList<UInt128, Int128, UInt256, Int256>;
using TypeListInt = TypeListConcat<TypeListNativeInt, TypeListWideInt>;
using TypeListIntAndFloat = TypeListConcat<TypeListInt, TypeListFloat>;
using TypeListDecimal = TypeList<Decimal32, Decimal64, Decimal128, Decimal256>;
using TypeListNumber = TypeListConcat<TypeListIntAndFloat, TypeListDecimal>;
using TypeListNumberWithUUID = TypeListAppend<UUID, TypeListNumber>;

}
