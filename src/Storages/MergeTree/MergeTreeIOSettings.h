#pragma once
#include <cstddef>
#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>


namespace DB
{

class MMappedFileCache;
using MMappedFileCachePtr = std::shared_ptr<MMappedFileCache>;


struct MergeTreeReaderSettings
{
    /// Common read settings.
    ReadSettings read_settings;
    /// If save_marks_in_cache is false, then, if marks are not in cache,
    ///  we will load them but won't save in the cache, to avoid evicting other data.
    bool save_marks_in_cache = false;
    /// Validate checksums on reading (should be always enabled in production).
    bool checksum_on_read = true;
};

struct MergeTreeWriterSettings
{
    MergeTreeWriterSettings() = default;

    MergeTreeWriterSettings(
        const Settings & global_settings,
        const MergeTreeSettingsPtr & storage_settings,
        bool can_use_adaptive_granularity_,
        bool rewrite_primary_key_,
        bool blocks_are_granules_size_ = false)
        : min_compress_block_size(
            storage_settings->min_compress_block_size ? storage_settings->min_compress_block_size : global_settings.min_compress_block_size)
        , max_compress_block_size(
              storage_settings->max_compress_block_size ? storage_settings->max_compress_block_size
                                                        : global_settings.max_compress_block_size)
        , can_use_adaptive_granularity(can_use_adaptive_granularity_)
        , rewrite_primary_key(rewrite_primary_key_)
        , blocks_are_granules_size(blocks_are_granules_size_)
    {
    }

    size_t min_compress_block_size;
    size_t max_compress_block_size;
    bool can_use_adaptive_granularity;
    bool rewrite_primary_key;
    bool blocks_are_granules_size;
};

}
