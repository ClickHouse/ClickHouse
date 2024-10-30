#pragma once
#include <cstddef>
#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <IO/WriteSettings.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>


namespace DB
{

class MMappedFileCache;
using MMappedFileCachePtr = std::shared_ptr<MMappedFileCache>;

enum class CompactPartsReadMethod
{
    SingleBuffer,
    MultiBuffer,
};

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
    /// Use one buffer for each column or for all columns while reading from compact.
    CompactPartsReadMethod compact_parts_read_method = CompactPartsReadMethod::SingleBuffer;
    /// True if we read stream for dictionary of LowCardinality type.
    bool is_low_cardinality_dictionary = false;
    /// True if data may be compressed by different codecs in one stream.
    bool allow_different_codecs = false;
    /// Deleted mask is applied to all reads except internal select from mutate some part columns.
    bool apply_deleted_mask = true;
    /// Put reading task in a common I/O pool, return Async state on prepare()
    bool use_asynchronous_read_from_pool = false;
    /// If PREWHERE has multiple conditions combined with AND, execute them in separate read/filtering steps.
    bool enable_multiple_prewhere_read_steps = false;
    /// If true, try to lower size of read buffer according to granule size and compressed block size.
    bool adjust_read_buffer_size = true;
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
        , compress_primary_key(storage_settings->compress_primary_key)
        , primary_key_compression_codec(storage_settings->primary_key_compression_codec)
        , primary_key_compress_block_size(storage_settings->primary_key_compress_block_size)
        , can_use_adaptive_granularity(can_use_adaptive_granularity_)
        , rewrite_primary_key(rewrite_primary_key_)
        , blocks_are_granules_size(blocks_are_granules_size_)
        , query_write_settings(query_write_settings_)
        , max_threads_for_annoy_index_creation(global_settings.max_threads_for_annoy_index_creation)
    {
    }

    size_t min_compress_block_size;
    size_t max_compress_block_size;

    String marks_compression_codec;
    size_t marks_compress_block_size;

    bool compress_primary_key;
    String primary_key_compression_codec;
    size_t primary_key_compress_block_size;

    bool can_use_adaptive_granularity;
    bool rewrite_primary_key;
    bool blocks_are_granules_size;
    WriteSettings query_write_settings;

    size_t max_threads_for_annoy_index_creation;
};

}
