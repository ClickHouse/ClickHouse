#pragma once

namespace DB
{
enum class SyncReplicaMode
{
    DEFAULT,
    STRICT,
    LIGHTWEIGHT,
    PULL,
};
}
