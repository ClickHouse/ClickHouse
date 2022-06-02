#pragma once

namespace DB
{

/// Settings to be passed to IDisk::writeFile()
struct WriteSettings
{
    /// User's settings
    bool enable_filesystem_cache_on_write_operations = false;
    bool enable_filesystem_cache_log = false;
    /// Internal settings
    bool is_file_persistent = false;
};

}
