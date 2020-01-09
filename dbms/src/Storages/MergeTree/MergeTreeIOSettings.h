#pragma once
#include <cstddef>
#include <Core/Settings.h>

namespace DB
{

struct MergeTreeReaderSettings
{
    size_t min_bytes_to_use_direct_io = 0;
    size_t min_bytes_to_use_mmap_io = 0;
    size_t max_read_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    bool save_marks_in_cache = false;
};

struct MergeTreeWriterSettings
{
    MergeTreeWriterSettings(const Settings & global_settings, bool can_use_adaptive_granularity_, bool blocks_are_granules_size_ = false)
        : min_compress_block_size(global_settings.min_compress_block_size)
        , max_compress_block_size(global_settings.min_compress_block_size)
        , aio_threshold(global_settings.min_bytes_to_use_direct_io)
        , can_use_adaptive_granularity(can_use_adaptive_granularity_)
        , blocks_are_granules_size(blocks_are_granules_size_) {}

    size_t min_compress_block_size;
    size_t max_compress_block_size;
    size_t aio_threshold;
    bool can_use_adaptive_granularity;
    bool blocks_are_granules_size;
    String filename_suffix = "";
    size_t estimated_size = 0;
    bool skip_offsets = false;
};
}

