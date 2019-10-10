#pragma once
#include <cstddef>

namespace DB
{
    struct ReaderSettings
    {
        size_t min_bytes_to_use_direct_io = 0;
        size_t max_read_buffer_size = 0;
        bool save_marks_in_cache = false;
    };
}

