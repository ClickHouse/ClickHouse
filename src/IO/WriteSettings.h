#pragma once

namespace DB
{

struct WriteSettings
{
    bool remote_fs_cache_on_write_operations = false;
};

}
