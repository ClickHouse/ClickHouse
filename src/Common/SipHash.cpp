#include <Common/Exception.h>
#include <Common/SipHash.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

UInt128 SipHash::get128Reference()
{
    if (!is_reference_128)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Can't call get128Reference when is_reference_128 is not set");
    finalize();
    const auto lo = v0 ^ v1 ^ v2 ^ v3;
    v1 ^= 0xdd;
    SIPROUND;
    SIPROUND;
    SIPROUND;
    SIPROUND;
    const auto hi = v0 ^ v1 ^ v2 ^ v3;

    UInt128 res = hi;
    res <<= 64;
    res |= lo;
    return res;
}
