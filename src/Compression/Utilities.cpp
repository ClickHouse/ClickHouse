#include "Utilities.h"

namespace DB
{
CityHash_v1_0_2::uint128 CalculateCityHash128InLittleEndian(const std::span<char> buffer)
{
    const auto hash = CityHash_v1_0_2::CityHash128(buffer.data(), buffer.size());

#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    return {__builtin_bswap64(hash.first), __builtin_bswap64(hash.second)};
#else
    return hash;
#endif
}
}
