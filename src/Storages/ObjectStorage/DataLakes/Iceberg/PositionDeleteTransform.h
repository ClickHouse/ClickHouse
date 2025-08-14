#pragma once
#include "config.h"

#if USE_AVRO

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>

namespace DB
{

class IcebergPositionDeleteTransform : public ISimpleTransform
{
public:
    static constexpr const char * positions_column_name = "pos";
    static constexpr const char * data_file_path_column_name = "file_path";

    IcebergPositionDeleteTransform(
        const SharedHeader & header_,
        IcebergDataObjectInfoPtr iceberg_object_info_,
        const ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_,
        String delete_object_format_,
        String delete_object_compression_method_)
        : ISimpleTransform(header_, header_, false)
        , header(header_)
        , iceberg_object_info(iceberg_object_info_)
        , object_storage(object_storage_)
        , format_settings(format_settings_)
        , context(context_)
        , delete_object_format(delete_object_format_)
        , delete_object_compression_method(delete_object_compression_method_)
    {
        initializeDeleteSources();
    }

    String getName() const override { return "IcebergPositionDeleteTransform"; }

private:
    void initializeDeleteSources();

protected:
    LoggerPtr log = getLogger("IcebergPositionDeleteTransform");
    static size_t getColumnIndex(const std::shared_ptr<IInputFormat> & delete_source, const String & column_name);

    SharedHeader header;
    IcebergDataObjectInfoPtr iceberg_object_info;
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    ContextPtr context;
    const String delete_object_format;
    const String delete_object_compression_method;

    /// We need to keep the read buffers alive since the delete_sources depends on them.
    std::vector<std::unique_ptr<ReadBuffer>> delete_read_buffers;
    std::vector<std::shared_ptr<IInputFormat>> delete_sources;
};

class IcebergBitmapPositionDeleteTransform : public IcebergPositionDeleteTransform
{
public:
    IcebergBitmapPositionDeleteTransform(
        const SharedHeader & header_,
        IcebergDataObjectInfoPtr iceberg_object_info_,
        const ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_,
        String delete_object_format_,
        String delete_object_compression_method_ = "auto")
        : IcebergPositionDeleteTransform(
              header_,
              iceberg_object_info_,
              object_storage_,
              format_settings_,
              context_,
              delete_object_format_,
              delete_object_compression_method_)
    {
        initialize();
    }

    String getName() const override { return "IcebergBitmapPositionDeleteTransform"; }

    void transform(Chunk & chunk) override;

private:
    void initialize();
    RoaringBitmapWithSmallSet<size_t, 32> bitmap;
};


class IcebergStreamingPositionDeleteTransform : public IcebergPositionDeleteTransform
{
public:
    IcebergStreamingPositionDeleteTransform(
        const SharedHeader & header_,
        IcebergDataObjectInfoPtr iceberg_object_info_,
        const ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_,
        String delete_object_format_,
        String delete_object_compression_method_ = "auto")
        : IcebergPositionDeleteTransform(
              header_,
              iceberg_object_info_,
              object_storage_,
              format_settings_,
              context_,
              delete_object_format_,
              delete_object_compression_method_)
    {
        initialize();
    }

    String getName() const override { return "IcebergStreamingPositionDeleteTransform"; }

    void transform(Chunk & chunk) override;

private:
    void initialize();

    struct PositionDeleteFileIndexes
    {
        size_t filename_index;
        size_t position_index;
    };

    void fetchNewChunkFromSource(size_t delete_source_index);

    std::vector<PositionDeleteFileIndexes> delete_source_column_indices;
    std::vector<Chunk> latest_chunks;
    std::vector<size_t> iterator_at_latest_chunks;
    std::set<std::pair<size_t, size_t>> latest_positions;

    std::optional<size_t> previous_chunk_offset;
};

}

#endif
