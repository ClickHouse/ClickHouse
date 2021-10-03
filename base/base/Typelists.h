#pragma once

#include "TL.h"
#include "extended_types.h"
#include "Decimal.h"
#include "UUID.h"

namespace DB
{
using TLIntegral = TL<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>;
using TLExtendedIntegral = TL<UInt128, Int128, UInt256, Int256>;
using TLDecimals = TL<Decimal32, Decimal64, Decimal128, Decimal256>;

using TLIntegralWithExtended = TLConcat<TLIntegral, TLExtendedIntegral>;

using TLNumbers = TLConcat<TLIntegralWithExtended, TLDecimals>;
using TLNumbersWithUUID = TLAppend<UUID, TLNumbers>;
}
