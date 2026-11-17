#pragma once
#include "config.h"
#if USE_PARQUET

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/Parquet/ReadManager.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/ParquetMetadataCache.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>

namespace DB
{

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

    void resetReadBuffer() override;

    String getName() const override { return "ParquetV3BlockInputFormat"; }

    const BlockMissingValues * getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override
    {
        return previous_approx_bytes_read_for_chunk;
    }

    void setBucketsToRead(const FileBucketInfoPtr & buckets_to_read_) override;

private:
    Chunk read() override;

    void prefetch() override;

    void onCancel() noexcept override;

    const FormatSettings format_settings;
    Parquet::ReadOptions read_options;
    FormatParserSharedResourcesPtr parser_shared_resources;
    FormatFilterInfoPtr format_filter_info;
    ParquetMetadataCachePtr metadata_cache;
    const std::optional<RelativePathWithMetadata> object_with_metadata;

    /// (This mutex is not important. It protects setting `reader` in a weird case where onCancel()
    ///  may be called in parallel with first read(). ReadManager itself is thread safe for that,
    ///  but initializing vs checking the pointer would race without this mutex.)
    std::mutex reader_mutex;

    std::unique_ptr<Parquet::ReadManager> reader;
    /// Has the file footer, but is not initialized: initializing it starts reading and decoding the column chunks.
    std::unique_ptr<Parquet::ReadManager> reader_with_footer;
    bool reported_count = false; // if need_only_count

    BlockMissingValues previous_block_missing_values;
    size_t previous_approx_bytes_read_for_chunk = 0;

    void readFileFooterIfNeeded();
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
