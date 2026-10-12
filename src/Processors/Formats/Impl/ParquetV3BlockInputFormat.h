#pragma once
#include "config.h"
#if USE_PARQUET

#include <Formats/FormatSettings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
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

class KeyCondition;

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

    /// Keeps the statistics ranges of the filter columns, as the reader checked them, with the cached metadata.
    void cacheFilterColumnStatistics() override;

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

    parquet::format::FileMetaData getFileMetadata(Parquet::Prefetcher & prefetcher);

    /// The cache cell of this file's metadata and its key, if it came from the metadata cache.
    ParquetMetadataCache::MappedPtr metadata_cache_cell;
    std::optional<ParquetMetadataCacheKey> metadata_cache_key;
};

/// Whether the statistics that earlier reads kept with the cached metadata of a file version (see
/// `ParquetV3BlockInputFormat::cacheFilterColumnStatistics`) show that no row group of it can satisfy the filter,
/// so that the file need not be opened: the reader would skip every row group by the same statistics.
struct ParquetCachedStatisticsFilter
{
    /// nullptr if no file can be skipped: filter push-down off, no key condition or one the statistics cannot
    /// decide, a column mapper, or a header that `FormatFactory::getInputImpl` would change for the format.
    static std::shared_ptr<const ParquetCachedStatisticsFilter> create(
        const FormatFilterInfoPtr & format_filter_info, const Block & header, const FormatSettings & format_settings);

    /// If the kept ranges exclude every row group of the file version, the number of row groups of the file.
    std::optional<size_t> canSkipFile(ParquetMetadataCache & metadata_cache, const String & path, const String & version) const;

    /// Counts a skipped file as the reader would have counted it.
    static void countSkippedFile(size_t num_row_groups);

    /// Identifies the statistics ranges of a column in a cache cell: the column and the settings that change how
    /// the reader finds it in the file and converts its statistics.
    static String makeColumnKey(const String & column_name, const IDataType & type, const FormatSettings & format_settings);

private:
    std::shared_ptr<const KeyCondition> key_condition;
    /// The positions of the columns that `key_condition` uses and their keys.
    std::vector<std::pair<size_t, String>> column_keys;
    /// The types of the header followed by `FormatFilterInfo::additional_columns`, as `key_condition` was built for.
    DataTypes key_types;
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
