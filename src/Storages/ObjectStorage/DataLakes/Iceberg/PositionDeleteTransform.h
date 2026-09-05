#pragma once
#include <Processors/Formats/IInputFormat.h>
#include <Poco/JSON/Array.h>
#include "config.h"

#if USE_AVRO

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>

#include <roaring/roaring64map.hh>

#include <tuple>

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
        FormatParserSharedResourcesPtr parser_shared_resources_,
        ContextPtr context_)
        : ISimpleTransform(header_, header_, false)
        , header(header_)
        , iceberg_object_info(iceberg_object_info_)
        , object_storage(object_storage_)
        , format_settings(format_settings_)
        , context(context_)
        , parser_shared_resources(parser_shared_resources_)
    {
        initializeDeleteSources();
    }

    String getName() const override { return "IcebergPositionDeleteTransform"; }

private:
    void initializeDeleteSources();

protected:
    LoggerPtr log = getLogger("IcebergPositionDeleteTransform");
    static size_t getColumnIndex(const std::shared_ptr<IInputFormat> & delete_source, const String & column_name);

    /// Drops rows whose `file_path` column does not match the current data file path.
    /// The WHERE filter on `position_delete_files` only drives row-group/page pruning at the
    /// Parquet reader; rows inside surviving row groups still need to be filtered explicitly.
    /// Returns the number of rows kept (0 if the chunk has no matching rows).
    size_t filterChunkToCurrentDataFile(Chunk & chunk, size_t filename_column_index) const;

    SharedHeader header;
    IcebergDataObjectInfoPtr iceberg_object_info;
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    ContextPtr context;
    FormatParserSharedResourcesPtr parser_shared_resources;

    /// We need to keep the read buffers alive since the position_delete_files depend on them.
    std::vector<std::unique_ptr<ReadBuffer>> delete_read_buffers;
    std::vector<std::shared_ptr<IInputFormat>> position_delete_files;
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

    struct PositionDeleteFileIndexes
    {
        size_t filename_index;
        size_t position_index;
    };

    struct DeletionVectorState
    {
        explicit DeletionVectorState(std::unique_ptr<roaring::Roaring64Map> bitmap_);

        std::unique_ptr<roaring::Roaring64Map> bitmap;
        roaring::Roaring64Map::const_iterator iterator;
        roaring::Roaring64Map::const_iterator end;
    };

    enum class DeleteSourceKind
    {
        File,
        Vector,
    };

    struct DeleteSourceRef
    {
        DeleteSourceKind kind;
        size_t index;

        bool operator<(const DeleteSourceRef & rhs) const
        {
            return std::tie(kind, index) < std::tie(rhs.kind, rhs.index);
        }
    };

    void fetchNewChunkFromSource(size_t position_delete_file_index);

    std::vector<PositionDeleteFileIndexes> position_delete_file_column_indices;
    std::vector<Chunk> latest_chunks;
    std::vector<size_t> iterator_at_latest_chunks;
    std::vector<DeletionVectorState> deletion_vectors;
    std::set<std::pair<size_t, DeleteSourceRef>> latest_positions;

    std::optional<size_t> previous_chunk_end_offset;
};

}

#endif
