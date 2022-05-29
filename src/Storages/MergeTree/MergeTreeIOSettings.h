#pragma once
#include <cstddef>
#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <IO/WriteSettings.h>


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
    /// True if we read in order of sorting key.
    bool read_in_order = false;
    /// Deleted mask is applied to all reads except internal select from mutate some part columns.
    bool apply_deleted_mask = true;
};

struct MergeTreeWriterSettings
{
    MergeTreeWriterSettings() = default;

    MergeTreeWriterSettings(
        const Settings & global_settings,
        const WriteSettings & query_write_settings_,
        const MergeTreeSettingsPtr & storage_settings,
        bool can_use_adaptive_granularity_,
        bool rewrite_primary_key_,
        bool blocks_are_granules_size_ = false)
        : min_compress_block_size(
            storage_settings->min_compress_block_size ? storage_settings->min_compress_block_size : global_settings.min_compress_block_size)
        , max_compress_block_size(
              storage_settings->max_compress_block_size ? storage_settings->max_compress_block_size
                                                        : global_settings.max_compress_block_size)
        , marks_compression_codec(storage_settings->marks_compression_codec)
        , marks_compress_block_size(storage_settings->marks_compress_block_size)
        , is_compress_primary_key(storage_settings->compress_primary_key)
        , primary_key_compression_codec(storage_settings->primary_key_compression_codec)
        , primary_key_compress_block_size(storage_settings->primary_key_compress_block_size)
        , can_use_adaptive_granularity(can_use_adaptive_granularity_)
        , rewrite_primary_key(rewrite_primary_key_)
        , blocks_are_granules_size(blocks_are_granules_size_)
        , query_write_settings(query_write_settings_)
    {
    }

    size_t min_compress_block_size;
    size_t max_compress_block_size;

    String marks_compression_codec;
    size_t marks_compress_block_size;

    bool is_compress_primary_key;
    String primary_key_compression_codec;
    size_t primary_key_compress_block_size;

    bool can_use_adaptive_granularity;
    bool rewrite_primary_key;
    bool blocks_are_granules_size;
    WriteSettings query_write_settings;
};

}
