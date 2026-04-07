#pragma once
#include <cstdint>

namespace DB
{
enum class SyncReplicaMode : uint8_t
{
    DEFAULT,
    STRICT,
    LIGHTWEIGHT,
    PULL,
};
}
