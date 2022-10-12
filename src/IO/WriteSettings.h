#pragma once

#include <Common/Throttler_fwd.h>

namespace DB
{

/// Settings to be passed to IDisk::writeFile()
struct WriteSettings
{
    bool enable_filesystem_cache_on_write_operations = false;

    /// Bandwidth throttler to use during writing
    ThrottlerPtr remote_throttler;
};

}
