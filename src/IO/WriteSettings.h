#pragma once

#include <Common/Throttler_fwd.h>

namespace DB
{

/// Settings to be passed to IDisk::writeFile()
struct WriteSettings
{
    /// Bandwidth throttler to use during writing
    ThrottlerPtr remote_throttler;

    /// Filesystem cache settings
    bool enable_filesystem_cache_on_write_operations = false;
    bool enable_filesystem_cache_log = false;
    bool is_file_cache_persistent = false;
    bool s3_allow_parallel_part_upload = true;

    /// Monitoring
    bool for_object_storage = false; // to choose which profile events should be incremented
};

}
