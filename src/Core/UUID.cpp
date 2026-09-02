#include <Core/UUID.h>
#include <Common/thread_local_rng.h>
#include <Common/SipHash.h>


namespace DB
{

namespace UUIDHelpers
{
    UUID generateV4()
    {
        UUID uuid;
        getHighBytes(uuid) = (thread_local_rng() & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        getLowBytes(uuid) = (thread_local_rng() & 0x3fffffffffffffffull) | 0x8000000000000000ull;

        return uuid;
    }

    UUID makeUUIDv4FromHash(const String & string)
    {
        UInt128 hash = sipHash128(string.data(), string.size());

        UUID uuid;
        getHighBytes(uuid) = (hash.items[HighBytes] & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        getLowBytes(uuid) = (hash.items[LowBytes] & 0x3fffffffffffffffull) | 0x8000000000000000ull;

        return uuid;
    }
}

}
