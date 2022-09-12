#pragma once

namespace DB
{

/// Settings to be passed to IDisk::writeFile()
struct WriteSettings
{
    bool enable_filesystem_cache_on_write_operations = false;
};

}
