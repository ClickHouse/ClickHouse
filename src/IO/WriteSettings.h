#pragma once

#include <Common/Throttler_fwd.h>
#include <Common/Scheduler/ResourceLink.h>
#include <IO/DistributedCacheSettings.h>

namespace DB
{

/// Settings to be passed to IDisk::writeFile()
struct WriteSettings
{
    /// Bandwidth throttler to use during writing
    ThrottlerPtr remote_throttler;
    ThrottlerPtr local_throttler;

    IOSchedulingSettings io_scheduling;

    /// Filesystem cache settings
    bool enable_filesystem_cache_on_write_operations = false;
    bool enable_filesystem_cache_log = false;
    bool throw_on_error_from_cache = false;
    size_t filesystem_cache_reserve_space_wait_lock_timeout_milliseconds = 1000;

    bool s3_allow_parallel_part_upload = true;
    bool azure_allow_parallel_part_upload = true;

    bool use_adaptive_write_buffer = false;
    size_t adaptive_write_buffer_initial_size = 16 * 1024;

    bool write_through_distributed_cache = false;
    DistributedCacheSettings distributed_cache_settings;

    bool operator==(const WriteSettings & other) const = default;
};

}
