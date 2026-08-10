#pragma once
#include "config.h"
#if USE_PARQUET

#include <Formats/FormatSettings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/Parquet/ReadManager.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/ParquetMetadataCache.h>
#include <unordered_set>

namespace DB
{

struct ParquetFileBucketInfo : public FileBucketInfo
{
    std::vector<size_t> row_group_ids;

    /// Total number of row groups the file had when the bucket assignment was computed (from the
    /// footer read at planning time). Carried so the read path can verify the file still has the
    /// same number of row groups and fail close if it diverged - e.g. an object overwritten
    /// between the split decision and the per-bucket read. The exact generation is checked by
    /// `footer_digest` below; this count is the coarser of the two guards. A value of 0 means
    /// "unknown" (e.g. a bucket deserialized from a node that predates this field) and disables
    /// the check.
    size_t file_num_row_groups = 0;

    /// Digest of the footer (`FileMetaData`) the bucket assignment was computed from
    /// (see `computeParquetFooterDigest`). 0 means "unknown" and disables the check.
    ///
    /// This is the exact generation token of the file the assignment describes, and it is the only
    /// one available on every backend: neither the local `{mtime, inode, size}` version token nor a
    /// storage etag can be relied on. Locally, an in-place rewrite that keeps the inode and the byte
    /// size and lands in the same filesystem timestamp tick produces an identical token, so a
    /// `ParquetMetadataCache` entry keyed on that token may describe a previous generation of the
    /// file. On object storage, only S3 pins the read to the listed etag
    /// (`s3_validate_etag_on_read`), so with that check off - or on a backend whose etag is not a
    /// strong content identifier - two bucket readers of the same object can otherwise open two
    /// different generations and return a mixed-generation result.
    ///
    /// Every per-bucket source therefore parses the footer of the bytes it actually opened (the
    /// format metadata cache is bypassed unless the read is pinned to the generation the cache key
    /// names) and compares this digest, so an assignment computed from a stale cached footer - or
    /// from a different generation of the same path - fails close with `FILE_CHANGED_WHILE_READING`
    /// instead of silently applying another generation's row-group layout. The field travels over
    /// the cluster protocol from
    /// `DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_PARQUET_FILE_ROW_GROUP_COUNT` on, so a
    /// distributed bucketed read is guarded the same way; an older worker cannot carry it, which
    /// `getMinProtocolVersion` turns into a fail-closed task instead of a silently unguarded read.
    UInt64 footer_digest = 0;

    ParquetFileBucketInfo() = default;
    explicit ParquetFileBucketInfo(const std::vector<size_t> & row_group_ids_, size_t file_num_row_groups_ = 0);
    void serialize(WriteBuffer & buffer, size_t protocol_version) override;
    void deserialize(ReadBuffer & buffer, size_t protocol_version) override;
    String getIdentifier() const override;
    String getFormatName() const override
    {
        return "Parquet";
    }
    std::shared_ptr<FileBucketInfo> filterByMatchingRowGroups(
        const std::vector<size_t> & matching_row_groups, size_t file_num_row_groups) const override;
    UInt64 getMinProtocolVersion() const override;
    bool coversWholeFile() const override;
};
using ParquetFileBucketInfoPtr = std::shared_ptr<ParquetFileBucketInfo>;

struct ParquetBucketSplitter : public IBucketSplitter
{
    ParquetBucketSplitter() = default;
    std::vector<FileBucketInfoPtr> splitToBuckets(size_t bucket_size, ReadBuffer & buf, const FormatSettings & format_settings_) override;
    std::vector<FileBucketInfoPtr> splitToBucketsByCount(size_t target_count, ReadBuffer & buf, const FormatSettings & format_settings_) override;
};

/// Digest of a parsed Parquet footer, used to tie a single-file bucket assignment to the file
/// generation it was computed from (see `ParquetFileBucketInfo::footer_digest`). Computed over the
/// footer's layout - schema shape, row-group and column-chunk row counts, sizes and offsets - so a
/// footer parsed from the file and the same footer returned by `ParquetMetadataCache` produce the
/// same value. Thrift enum fields are deliberately not read (a malformed file can hold an
/// out-of-range enumerator, whose load is undefined behavior). Never returns 0 (the "unknown"
/// marker).
UInt64 computeParquetFooterDigest(const parquet::format::FileMetaData & file_metadata);

/// Cache-aware single-file split. Parses the Parquet footer via `Parquet::Reader::readFileMetaData`
/// (the same path the input format uses) and stores the result in the `ParquetMetadataCache` under
/// the `(file_path, cache_etag)` key, so the per-bucket sources created by the caller hit the cache
/// instead of re-parsing the footer. If `metadata_cache` is null or the key components are empty,
/// metadata is parsed without caching.
std::vector<FileBucketInfoPtr> splitParquetFileWithCache(
    size_t target_count,
    const String & file_path,
    const String & cache_etag,
    ReadBuffer & buf,
    const FormatSettings & format_settings,
    ParquetMetadataCachePtr metadata_cache,
    const std::unordered_set<String> & requested_columns,
    size_t min_bytes_to_split,
    size_t min_bytes_per_bucket);

/// Warm-cache fast path for the single-file split decision. Returns the bucket layout without any
/// I/O when `(file_path, cache_etag)` is already present in `metadata_cache`, and an empty vector
/// otherwise (so the caller can fall through to the full `splitParquetFileWithCache` path that
/// opens the file). The point is to avoid `createReadBuffer` + `Prefetcher::init` overhead on
/// repeated queries against the same file — those are ~0.3 ms of fixed cost that visibly slows
/// "short" queries (e.g. `clickbench_parquet_short`).
std::vector<FileBucketInfoPtr> trySplitParquetFileFromCacheOnly(
    size_t target_count,
    const String & file_path,
    const String & cache_etag,
    const ParquetMetadataCachePtr & metadata_cache,
    const std::unordered_set<String> & requested_columns,
    size_t min_bytes_to_split,
    size_t min_bytes_per_bucket);

class ParquetV3BlockInputFormat final : public IInputFormat
{
public:
    ParquetV3BlockInputFormat(
        ReadBuffer & buf,
        SharedHeader header_,
        const FormatSettings & format_settings,
        FormatParserSharedResourcesPtr parser_shared_resources_,
        FormatFilterInfoPtr format_filter_info_,
        size_t min_bytes_for_seek,
        ParquetMetadataCachePtr metadata_cache_ = nullptr,
        const std::optional<RelativePathWithMetadata> & object_with_metadata_ = std::nullopt);

    void resetParser() override;

    String getName() const override { return "ParquetV3BlockInputFormat"; }

    const BlockMissingValues * getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override
    {
        return previous_approx_bytes_read_for_chunk;
    }

    void setBucketsToRead(const FileBucketInfoPtr & buckets_to_read_) override;

    std::optional<std::pair<std::vector<size_t>, size_t>> getMatchedBuckets() const override;

private:
    Chunk read() override;

    void onCancel() noexcept override;

    const FormatSettings format_settings;
    Parquet::ReadOptions read_options;
    FormatParserSharedResourcesPtr parser_shared_resources;
    FormatFilterInfoPtr format_filter_info;
    ParquetMetadataCachePtr metadata_cache;
    const std::optional<RelativePathWithMetadata> object_with_metadata;

    /// (This mutex is not important. It protects `reader.emplace` in a weird case where onCancel()
    ///  may be called in parallel with first read(). ReadManager itself is thread safe for that,
    ///  but initializing vs checking the std::optional would race without this mutex.)
    std::mutex reader_mutex;

    std::optional<Parquet::ReadManager> reader;
    bool reported_count = false; // if need_only_count

    BlockMissingValues previous_block_missing_values;
    size_t previous_approx_bytes_read_for_chunk = 0;

    void initializeIfNeeded();
    std::shared_ptr<ParquetFileBucketInfo> buckets_to_read;

    parquet::format::FileMetaData getFileMetadata(Parquet::Prefetcher & prefetcher) const;
};

class NativeParquetSchemaReader final : public ISchemaReader
{
public:
    NativeParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings);

    NamesAndTypesList readSchema() override;
    std::optional<size_t> readNumberOrRows() override;

private:
    void initializeIfNeeded();

    Parquet::ReadOptions read_options;
    parquet::format::FileMetaData file_metadata;
    bool initialized = false;
};

}

#endif
