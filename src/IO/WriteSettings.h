#pragma once

namespace DB
{

struct WriteSettings
{
    bool enable_filesystem_cache_on_write_operations = false;
};

}
