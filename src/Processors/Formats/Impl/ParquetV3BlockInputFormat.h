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
        const std::optional<RelativePathWithMetadata> & object_with_metadata_ = std::nullopt,
        bool is_remote_fs_ = false);

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

    void cacheFilterColumnStatistics() override;

    /// Use the columns cache, see `Parquet::ColumnsCacheOptions`. Must be called before the first read.
    void setColumnsCacheOptions(std::shared_ptr<const Parquet::ColumnsCacheOptions> options) { read_options.columns_cache = std::move(options); }

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

    /// nullopt if the metadata cache is not used.
    std::optional<ParquetMetadataCacheKey> getMetadataCacheKey() const;
    std::shared_ptr<const Parquet::FileFooter> readFileFooter(Parquet::Prefetcher & prefetcher) const;
    /// For count(), which needs no column chunks.
    std::shared_ptr<const Parquet::FileFooter> getFileFooter(Parquet::Prefetcher & prefetcher) const;

    /// Only a cell found in the cache keeps statistics: counting them moves the cell, which only a hit may do.
    ParquetMetadataCache::MappedPtr metadata_cache_cell;
    std::optional<ParquetMetadataCacheKey> metadata_cache_key;
};

/// Skips a file version, without opening it, if the kept statistics show that the reader would prune every row group.
struct ParquetCachedStatisticsFilter
{
    /// nullptr if no file can be skipped with this filter and header.
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
    /// Positions are in the header followed by `FormatFilterInfo::additional_columns`, as `key_condition` was built for.
    std::vector<std::pair<size_t, String>> column_keys;
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
    std::shared_ptr<const Parquet::FileFooter> file_footer;
    bool initialized = false;
};

}

#endif
