#include <Core/UUID.h>
#include <Common/thread_local_rng.h>


namespace DB
{

namespace UUIDHelpers
{
    UUID generateV4()
    {
        UUID uuid;
        uuid.toUnderType() = {thread_local_rng(), thread_local_rng()};

        getUUIDHigh(uuid) = (getUUIDHigh(uuid) & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        getUUIDLow(uuid) = (getUUIDLow(uuid) & 0x3fffffffffffffffull) | 0x8000000000000000ull;

        return uuid;
    }
}

}
