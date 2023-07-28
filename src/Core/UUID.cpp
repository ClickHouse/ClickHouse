#include <Core/UUID.h>
#include <Common/thread_local_rng.h>


namespace DB
{

namespace UUIDHelpers
{
    UUID generateV4()
    {
        UUID uuid;
        UInt128 & res = uuid.toUnderType();

        res = {thread_local_rng(), thread_local_rng()};
        res.items[0] = (res.items[0] & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        res.items[1] = (res.items[1] & 0x3fffffffffffffffull) | 0x8000000000000000ull;

        toLegacyFormat(uuid);
        return uuid;
    }
}

}
