#pragma once
#include <base/defines.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
struct VFSSettings
{
    VFSSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
        : gc_sleep_ms(config.getUInt64(config_prefix + ".vfs_gc_sleep_ms", 10'000))
        , batch_min_size(config.getUInt64(config_prefix + ".vfs_batch_min_size", 1))
        , batch_max_size(config.getUInt64(config_prefix + ".vfs_batch_max_size", 100'000))
        , batch_can_wait_ms(config.getUInt64(config_prefix + ".vfs_batch_can_wait_ms", 0))
        , snapshot_lz4_compression_level(config.getInt(config_prefix + ".vfs_snapshot_lz4_compression_level", 8))
    {
    }
    UInt64 gc_sleep_ms;
    UInt64 batch_min_size;
    UInt64 batch_max_size;
    UInt64 batch_can_wait_ms;
    Int16 snapshot_lz4_compression_level;
};
}
