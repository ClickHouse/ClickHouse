#pragma once
#include "config.h"
#if USE_PARQUET

#include <Formats/FormatSettings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/Parquet/ReadManager.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/ParquetMetadataCache.h>

#include <condition_variable>

namespace DB
{

/// The footer of one object, read in the background ahead of the reader that opens the object.
class ParquetPrefetchedFooter : private boost::noncopyable
{
public:
    /// The background read calls `tryStart` and then `finish` or `fail`. It must not start if `tryStart` returns false.
    bool tryStart();
    void finish(parquet::format::FileMetaData metadata_);
    void fail(std::exception_ptr exception_) noexcept;

    /// For the reader: waits for a read in progress and returns its footer or rethrows its error.
    /// Returns nullopt if the read has not started, and then it never starts.
    std::optional<parquet::format::FileMetaData> take();

private:
    enum class State : uint8_t
    {
        Scheduled,
        Running,
        Done,
        Taken,
    };

    std::mutex mutex;
    std::condition_variable done;
    State state = State::Scheduled;
    std::optional<parquet::format::FileMetaData> metadata;
    std::exception_ptr exception;
};

parquet::format::FileMetaData readParquetFileMetaData(ReadBuffer & buf, const FormatSettings & format_settings);

/// Reads the footer of `object` as `ParquetV3BlockInputFormat` does, through `metadata_cache` if it is not null.
parquet::format::FileMetaData readParquetFileMetaDataAhead(
    ReadBuffer & buf,
    const FormatSettings & format_settings,
    const ParquetMetadataCachePtr & metadata_cache,
    const RelativePathWithMetadata & object);

struct ParquetFileBucketInfo : public FileBucketInfo
{
    std::vector<size_t> row_group_ids;

    ParquetFileBucketInfo() = default;
    explicit ParquetFileBucketInfo(const std::vector<size_t> & row_group_ids_);
    void serialize(WriteBuffer & buffer) override;
    void deserialize(ReadBuffer & buffer) override;
    String getIdentifier() const override;
    String getFormatName() const override
    {
        return "Parquet";
    }
    std::shared_ptr<FileBucketInfo> filterByMatchingRowGroups(const std::vector<size_t> & matching_row_groups) const override;
};
using ParquetFileBucketInfoPtr = std::shared_ptr<ParquetFileBucketInfo>;

struct ParquetBucketSplitter : public IBucketSplitter
{
    ParquetBucketSplitter() = default;
    std::vector<FileBucketInfoPtr> splitToBuckets(size_t bucket_size, ReadBuffer & buf, const FormatSettings & format_settings_) override;
};

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

    void resetReadBuffer() override;

    String getName() const override { return "ParquetV3BlockInputFormat"; }

    const BlockMissingValues * getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override
    {
        return previous_approx_bytes_read_for_chunk;
    }

    void setBucketsToRead(const FileBucketInfoPtr & buckets_to_read_) override;

    std::optional<std::pair<std::vector<size_t>, size_t>> getMatchedBuckets() const override;
    std::vector<std::pair<size_t, Field>> getTopKBestValuesOfBuckets() const override;
    bool isTopKFilterApplied() const override;
    bool isMinMaxFromStatisticsApplied() const override { return min_max_from_statistics; }

    void setPrefetchedFooter(std::shared_ptr<ParquetPrefetchedFooter> footer) override;

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

    /// The rows were replaced by min / max from column chunk statistics; no row groups were read.
    bool min_max_from_statistics = false;

    BlockMissingValues previous_block_missing_values;
    size_t previous_approx_bytes_read_for_chunk = 0;

    void initializeIfNeeded();
    std::shared_ptr<ParquetFileBucketInfo> buckets_to_read;
    std::shared_ptr<ParquetPrefetchedFooter> prefetched_footer;

    parquet::format::FileMetaData getFileMetadata(Parquet::Prefetcher & prefetcher);
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
