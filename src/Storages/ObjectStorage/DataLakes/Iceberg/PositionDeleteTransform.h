#pragma once
#include <Processors/Formats/IInputFormat.h>
#include <Poco/JSON/Array.h>
#include "config.h"

#if USE_AVRO

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Processors/ISimpleTransform.h>
#include <roaring/roaring64map.hh>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>

namespace DB::Iceberg
{

class IcebergPositionDeletesReader
{
public:
    IcebergPositionDeletesReader(
        IcebergDataObjectInfoPtr iceberg_object_info_,
        ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings,
        FormatParserSharedResourcesPtr parser_shared_resources,
        ContextPtr context_);

    size_t numPositionDeleteFiles() const { return position_delete_files.size(); }

    /// The next chunk of the position delete file `file_index` with at least one row of the current data file and no rows of
    /// other data files, or a chunk without rows at the end of the file.
    Chunk readPositionDeleteChunk(size_t file_index);

    size_t positionColumnIndex(size_t file_index) const { return position_delete_files[file_index].position_index; }

    std::unique_ptr<roaring::Roaring64Map> readDeletionVector() const;

    template <typename Callback>
    void forEachDeletedPosition(Callback && callback)
    {
        if (auto deletion_vector = readDeletionVector())
            for (const auto position : *deletion_vector)
                callback(position);

        for (size_t file_index = 0; file_index < position_delete_files.size(); ++file_index)
        {
            const size_t position_index = positionColumnIndex(file_index);
            while (auto chunk = readPositionDeleteChunk(file_index))
            {
                const auto & positions = *chunk.getColumns()[position_index];
                for (size_t row = 0; row < chunk.getNumRows(); ++row)
                    callback(positions.get64(row));
            }
        }
    }

private:
    struct PositionDeleteFile
    {
        std::shared_ptr<IInputFormat> format;
        size_t filename_index;
        size_t position_index;
    };

    /// Drops rows whose `file_path` column does not match the current data file path.
    /// The WHERE filter on `position_delete_files` only drives row-group/page pruning at the
    /// Parquet reader; rows inside surviving row groups still need to be filtered explicitly.
    /// Returns the number of rows kept (0 if the chunk has no matching rows).
    size_t filterChunkToCurrentDataFile(Chunk & chunk, size_t filename_column_index) const;

    IcebergDataObjectInfoPtr iceberg_object_info;
    const ObjectStoragePtr object_storage;
    ContextPtr context;
    LoggerPtr log = getLogger("IcebergPositionDeletesReader");

    /// We need to keep the read buffers alive since the position_delete_files depend on them.
    std::vector<std::unique_ptr<ReadBuffer>> delete_read_buffers;
    std::vector<PositionDeleteFile> position_delete_files;
};

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
        FormatParserSharedResourcesPtr parser_shared_resources_,
        ContextPtr context_)
        : ISimpleTransform(header_, header_, false)
        , iceberg_object_info(iceberg_object_info_)
        , reader(iceberg_object_info_, object_storage_, format_settings_, parser_shared_resources_, context_)
    {
    }

    String getName() const override { return "IcebergPositionDeleteTransform"; }

protected:
    IcebergDataObjectInfoPtr iceberg_object_info;
    IcebergPositionDeletesReader reader;
};

class IcebergBitmapPositionDeleteTransform final : public IcebergPositionDeleteTransform
{
public:
    using ExcludedRows = DB::DataLakeObjectMetadata::ExcludedRows;

    IcebergBitmapPositionDeleteTransform(
        const SharedHeader & header_,
        IcebergDataObjectInfoPtr iceberg_object_info_,
        ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings_,
        FormatParserSharedResourcesPtr parser_shared_resources_,
        ContextPtr context_)
        : IcebergPositionDeleteTransform(header_, iceberg_object_info_, object_storage_, format_settings_, parser_shared_resources_, context_)
    {
        initialize();
    }

    String getName() const override { return "IcebergBitmapPositionDeleteTransform"; }

    void transform(Chunk & chunk) override;

private:
    void initialize();
    ExcludedRows bitmap;
};


roaring::Roaring64Map readDeletedPositions(
    IcebergDataObjectInfoPtr iceberg_object_info,
    ObjectStoragePtr object_storage,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context);

/// Requires both the deletes and the input Chunk-s to arrive in order of increasing row number.
class IcebergStreamingPositionDeleteTransform final : public IcebergPositionDeleteTransform
{
public:
    IcebergStreamingPositionDeleteTransform(
        const SharedHeader & header_,
        IcebergDataObjectInfoPtr iceberg_object_info_,
        ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings_,
        FormatParserSharedResourcesPtr parser_shared_resources_,
        ContextPtr context_)
        : IcebergPositionDeleteTransform(header_, iceberg_object_info_, object_storage_, format_settings_, parser_shared_resources_, context_)
    {
        initialize();
    }

    String getName() const override { return "IcebergStreamingPositionDeleteTransform"; }

    void transform(Chunk & chunk) override;

private:
    void initialize();

    void fetchNewChunkFromSource(size_t position_delete_file_index);

    std::vector<Chunk> latest_chunks;
    std::vector<size_t> iterator_at_latest_chunks;
    std::set<std::pair<size_t, size_t>> latest_positions;

    std::optional<size_t> previous_chunk_end_offset;
};

}

#endif
