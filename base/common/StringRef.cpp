#include "StringRef.h"

#include <city.h>

#if !defined(ARCADIA_BUILD)
using namespace CityHash_v1_0_2;
#endif

size_t StringRefHash64::operator()(StringRef x) const
{
    return CityHash64(x.data, x.size);
}

#if defined(__SSE4_2__)

inline UInt64 hashLen16(UInt64 u, UInt64 v)
{
    return Hash128to64(uint128(u, v));
}

#endif
