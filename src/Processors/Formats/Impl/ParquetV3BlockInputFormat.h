#pragma once
#include "config.h"
#if USE_PARQUET

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/Parquet/ReadManager.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/ParquetMetadataCache.h>

namespace DB
{

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

class ParquetV3BlockInputFormat : public IInputFormat
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

class NativeParquetSchemaReader : public ISchemaReader
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
