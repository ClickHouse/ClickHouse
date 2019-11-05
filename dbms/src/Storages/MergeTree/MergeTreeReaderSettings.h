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

struct WriterSettings
{
    WriterSettings(const Settings & settings, bool can_use_adaptive_granularity_, bool blocks_are_granules_size_ = false)
        : min_compress_block_size(settings.min_compress_block_size)
        , max_compress_block_size(settings.max_compress_block_size)
        , aio_threshold(settings.min_bytes_to_use_direct_io)
        , can_use_adaptive_granularity(can_use_adaptive_granularity_)
        , blocks_are_granules_size(blocks_are_granules_size_) {}

    WriterSettings & setAdaptive(bool value)
    {
        can_use_adaptive_granularity = value;
    }

    WriterSettings & setAioThreshHold(size_t value)
    {
        
    }

    size_t min_compress_block_size;
    size_t max_compress_block_size;
    size_t aio_threshold;
    bool can_use_adaptive_granularity;
    bool blocks_are_granules_size;
    size_t estimated_size = 0;
};
}

