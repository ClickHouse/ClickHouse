#pragma once

#include <cstdint>
#include <string>
#include "wide_integer.h"

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

/// This is needed for more strict aliasing. https://godbolt.org/z/xpJBSb https://stackoverflow.com/a/57453713
#if !defined(PVS_STUDIO) // But PVS-Studio does not treat it correctly. FIXME Recheck that incorrect behaviour continues
using UInt8 = char8_t;
#else
using UInt8 = uint8_t;
#endif

using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using Int128 = wide::integer<128, signed>;
using UInt128 = wide::integer<128, unsigned>;
using Int256 = wide::integer<256, signed>;
using UInt256 = wide::integer<256, unsigned>;

static_assert(sizeof(Int256) == 32);
static_assert(sizeof(UInt256) == 32);

using String = std::string;

namespace DB
{
using UInt8 = ::UInt8;
using UInt16 = ::UInt16;
using UInt32 = ::UInt32;
using UInt64 = ::UInt64;
using UInt128 = ::UInt128;
using UInt256 = ::UInt256;

using Int8 = ::Int8;
using Int16 = ::Int16;
using Int32 = ::Int32;
using Int64 = ::Int64;
using Int128 = ::Int128;
using Int256 = ::Int256;

using Float32 = float;
using Float64 = double;

using String = ::String;
}
