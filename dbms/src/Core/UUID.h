#pragma once

#include <Common/UInt128.h>
#include <common/strong_typedef.h>
#include <Common/thread_local_rng.h>

namespace DB
{

STRONG_TYPEDEF(UInt128, UUID)

namespace UUIDHelpers
{
    inline UUID generateV4()
    {
        UInt128 res{thread_local_rng(), thread_local_rng()};
        res.low = (res.low & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        res.high = (res.high & 0x3fffffffffffffffull) | 0x8000000000000000ull;
        return UUID{res};
    }

    const UUID Nil = UUID(UInt128(0, 0));
}

}
