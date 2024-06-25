#pragma once

#include <Common/Throttler_fwd.h>
#include <Common/Scheduler/ResourceLink.h>

namespace DB
{

/// Settings to be passed to IDisk::writeFile()
struct WriteSettings
{
    /// Bandwidth throttler to use during writing
    ThrottlerPtr remote_throttler;
    ThrottlerPtr local_throttler;

    // Resource to be used during reading
    ResourceLink resource_link;

    /// Filesystem cache settings
    bool enable_filesystem_cache_on_write_operations = false;
    bool enable_filesystem_cache_log = false;
    bool throw_on_error_from_cache = false;
    size_t filesystem_cache_reserve_space_wait_lock_timeout_milliseconds = 1000;

    bool s3_allow_parallel_part_upload = true;
    bool azure_allow_parallel_part_upload = true;

    bool operator==(const WriteSettings & other) const = default;
};

}
