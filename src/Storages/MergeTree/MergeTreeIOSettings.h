#pragma once
#include <cstddef>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>


namespace DB
{

struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;
struct Settings;

class MMappedFileCache;
using MMappedFileCachePtr = std::shared_ptr<MMappedFileCache>;

enum class CompactPartsReadMethod : uint8_t
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
    /// If true, it's allowed to read the whole part without reading marks.
    bool can_read_part_without_marks = false;
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
        bool save_marks_in_cache_,
        bool blocks_are_granules_size_);

    size_t min_compress_block_size;
    size_t max_compress_block_size;

    String marks_compression_codec;
    size_t marks_compress_block_size;

    bool compress_primary_key;
    String primary_key_compression_codec;
    size_t primary_key_compress_block_size;

    bool can_use_adaptive_granularity;
    bool rewrite_primary_key;
    bool save_marks_in_cache;
    bool blocks_are_granules_size;
    WriteSettings query_write_settings;

    size_t low_cardinality_max_dictionary_size;
    bool low_cardinality_use_single_dictionary_for_part;
    bool use_compact_variant_discriminators_serialization;
    bool use_adaptive_write_buffer_for_dynamic_subcolumns;
    size_t adaptive_write_buffer_initial_size;
};

}
