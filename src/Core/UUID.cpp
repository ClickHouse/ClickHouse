#include <Core/UUID.h>
#include <Common/thread_local_rng.h>


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
}

}
