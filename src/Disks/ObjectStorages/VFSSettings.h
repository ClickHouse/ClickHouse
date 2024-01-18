#pragma once
#include <Poco/Util/AbstractConfiguration.h>
#include <base/defines.h>

namespace DB
{
struct VFSSettings
{
    explicit VFSSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    : gc_sleep_ms(config.getUInt64(config_prefix + ".vfs_gc_sleep_ms", 1'000))
    , batch_min_size(config.getUInt64(config_prefix + ".vfs_batch_min_size", 1))
    , batch_max_size(config.getUInt64(config_prefix + ".vfs_batch_max_size", 100'000))
    , batch_can_wait_milliseconds(config.getUInt64(config_prefix + ".vfs_batch_can_wait_milliseconds", 0))
    , snapshot_lz4_compression_level(config.getInt(config_prefix + ".vfs_snapshot_lz4_compression_level", 8))
    {
    }
    const UInt64 gc_sleep_ms;
    const UInt64 batch_min_size;
    const UInt64 batch_max_size;
    const UInt64 batch_can_wait_milliseconds;
    const Int16 snapshot_lz4_compression_level;
};
} // namespace DB
