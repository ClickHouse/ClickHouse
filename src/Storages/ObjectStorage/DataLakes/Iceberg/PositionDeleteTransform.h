#pragma once
#include <Processors/Formats/IInputFormat.h>
#include <Poco/JSON/Array.h>
#include "config.h"

#if USE_AVRO

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>

namespace DB::Iceberg
{
class IcebergPositionDeleteTransform : public ISimpleTransform
{
public:
    static constexpr const char * positions_column_name = "pos";
    static constexpr const char * data_file_path_column_name = "file_path";

    static constexpr Int64 positions_column_field_id = 2147483545;
    static constexpr Int64 data_file_path_column_field_id = 2147483546;

    static Poco::JSON::Array::Ptr getSchemaFields();

    IcebergPositionDeleteTransform(
        const SharedHeader & header_,
        IcebergDataObjectInfoPtr iceberg_object_info_,
        ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_)
        : ISimpleTransform(header_, header_, false)
        , header(header_)
        , iceberg_object_info(iceberg_object_info_)
        , object_storage(object_storage_)
        , format_settings(format_settings_)
        , context(context_)
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
        ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_)
        : IcebergPositionDeleteTransform(header_, iceberg_object_info_, object_storage_, format_settings_, context_)
    {
        initialize();
    }

    String getName() const override { return "IcebergBitmapPositionDeleteTransform"; }

    void transform(Chunk & chunk) override;

private:
    void initialize();
    RoaringBitmapWithSmallSet<size_t, 32> bitmap;
};


/// Requires both the deletes and the input Chunk-s to arrive in order of increasing row number.
class IcebergStreamingPositionDeleteTransform : public IcebergPositionDeleteTransform
{
public:
    IcebergStreamingPositionDeleteTransform(
        const SharedHeader & header_,
        IcebergDataObjectInfoPtr iceberg_object_info_,
        ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_)
        : IcebergPositionDeleteTransform(header_, iceberg_object_info_, object_storage_, format_settings_, context_)
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

    std::optional<size_t> previous_chunk_end_offset;
};

}

#endif
