#pragma once

#include <Common/IThrottler.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Core/Types.h>
#if ENABLE_DISTRIBUTED_CACHE
#include <IO/DistributedCacheSettings.h>
#endif

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
#if ENABLE_DISTRIBUTED_CACHE
    DistributedCacheSettings distributed_cache_settings;
#endif

    bool is_initial_access_check = false;

    /// A requested compare-and-swap. Honoured only by S3-like object storages; a storage that cannot
    /// express the condition must refuse the write, never perform it unconditionally.
    std::string object_storage_write_if_none_match;
    std::string object_storage_write_if_match;

    bool operator==(const WriteSettings & other) const = default;
};

WriteSettings getWriteSettings();

WriteSettings getWriteSettingsForMetadata();
}
