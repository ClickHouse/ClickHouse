#include <Storages/ObjectStorage/Utils.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/logger_useful.h>
#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDeletionVectorReader.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteObject.h>
#include <Storages/ObjectStorage/DataLakes/DeletionVectorTransform.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

namespace DB::Setting
{
extern const SettingsBool allow_experimental_iceberg_deletion_vectors;
extern const SettingsNonZeroUInt64 max_block_size;
}
namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int SUPPORT_IS_DISABLED;
}

namespace DB::Iceberg
{

Poco::JSON::Array::Ptr IcebergPositionDeleteTransform::getSchemaFields()
{
    Poco::JSON::Array::Ptr pos_delete_schema = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr field_pos = new Poco::JSON::Object;
    field_pos->set(Iceberg::f_id, IcebergPositionDeleteTransform::positions_column_field_id);
    field_pos->set(Iceberg::f_name, IcebergPositionDeleteTransform::positions_column_name);
    field_pos->set(Iceberg::f_required, true);
    field_pos->set(Iceberg::f_type, "long");

    Poco::JSON::Object::Ptr field_filename = new Poco::JSON::Object;
    field_filename->set(Iceberg::f_id, IcebergPositionDeleteTransform::data_file_path_column_field_id);
    field_pos->set(Iceberg::f_name, IcebergPositionDeleteTransform::data_file_path_column_name);
    field_pos->set(Iceberg::f_required, true);
    field_pos->set(Iceberg::f_type, "string");

    pos_delete_schema->add(field_filename);
    pos_delete_schema->add(field_pos);
    return pos_delete_schema;
}

void IcebergPositionDeleteTransform::initializeDeleteSources()
{
    const bool can_read_deletion_vectors = context->getSettingsRef()[Setting::allow_experimental_iceberg_deletion_vectors].value;

    /// Create filter on the data object to get interested rows
    auto iceberg_data_path = iceberg_object_info->info.data_object_file_path_key.serialize();
    ASTPtr where_ast = makeASTFunction(
        "equals",
        make_intrusive<ASTIdentifier>(IcebergPositionDeleteTransform::data_file_path_column_name),
        make_intrusive<ASTLiteral>(Field(iceberg_data_path)));

    for (const auto & position_deletes_object : iceberg_object_info->info.position_deletes_objects)
    {
        if (position_deletes_object.isDeletionVector())
        {
            if (!can_read_deletion_vectors)
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "Iceberg v3 deletion vectors are not enabled. Set allow_experimental_iceberg_deletion_vectors = 1.");
            continue;
        }

        if (position_deletes_object.reference_data_file_path.has_value()
            && position_deletes_object.reference_data_file_path != iceberg_data_path)
        {
            continue;
        }


        auto object_path = position_deletes_object.file_path;
        auto object_metadata = object_storage->getObjectMetadata(object_path, /*with_tags=*/ false);
        auto object_info = RelativePathWithMetadata{object_path, object_metadata};


        String format = position_deletes_object.file_format;
        if (boost::to_lower_copy(format) != "parquet")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Position deletes are supported only for parquet format");

        Block initial_header;
        {
            std::unique_ptr<ReadBuffer> read_buf_schema = createReadBuffer(object_info, object_storage, context, log);
            auto schema_reader = FormatFactory::instance().getSchemaReader(format, *read_buf_schema, context);
            auto columns_with_names = schema_reader->readSchema();
            ColumnsWithTypeAndName initial_header_data;
            for (const auto & elem : columns_with_names)
            {
                initial_header_data.push_back(ColumnWithTypeAndName(elem.type, elem.name));
            }
            initial_header = Block(initial_header_data);
        }

        CompressionMethod compression_method = chooseCompressionMethod(object_path, "auto");

        delete_read_buffers.push_back(createReadBuffer(object_info, object_storage, context, log));

        auto syntax_result = TreeRewriter(context).analyze(where_ast, initial_header.getNamesAndTypesList());
        ExpressionAnalyzer analyzer(where_ast, syntax_result, context);
        std::optional<ActionsDAG> actions = analyzer.getActionsDAG(true);
        std::shared_ptr<const ActionsDAG> actions_dag_ptr = [&actions]()
        {
            if (actions.has_value())
                return std::make_shared<const ActionsDAG>(std::move(actions.value()));
            return std::shared_ptr<const ActionsDAG>();
        }();

        auto delete_format = FormatFactory::instance().getInput(
            format,
            *delete_read_buffers.back(),
            initial_header,
            context,
            context->getSettingsRef()[DB::Setting::max_block_size],
            format_settings,
            parser_shared_resources,
            std::make_shared<FormatFilterInfo>(actions_dag_ptr, context, nullptr, nullptr, nullptr),
            true /* is_remote_fs */,
            compression_method);

        position_delete_files.push_back(std::move(delete_format));
    }
}

size_t IcebergPositionDeleteTransform::getColumnIndex(const std::shared_ptr<IInputFormat> & delete_source, const String & column_name)
{
    const auto & delete_header = delete_source->getOutputs().back().getHeader();
    for (size_t i = 0; i < delete_header.getNames().size(); ++i)
    {
        if (delete_header.getNames()[i] == column_name)
        {
            return i;
        }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not find column {} in chunk", column_name);
}

size_t IcebergPositionDeleteTransform::filterChunkToCurrentDataFile(Chunk & chunk, size_t filename_column_index) const
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return 0;

    const auto & filename_column = *chunk.getColumns()[filename_column_index];
    const auto iceberg_uri = iceberg_object_info->info.data_object_file_path_key.serialize();
    const auto storage_path = iceberg_object_info->getPath();

    IColumn::Filter filter(num_rows, 0);
    size_t num_matched = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        const auto file_to_delete = filename_column.getDataAt(i);
        if (file_to_delete == iceberg_uri || file_to_delete == "/" + iceberg_uri
            || file_to_delete == storage_path || file_to_delete == "/" + storage_path)
        {
            filter[i] = 1;
            ++num_matched;
        }
    }

    if (num_matched == num_rows)
        return num_rows;

    if (num_matched == 0)
    {
        chunk.clear();
        return 0;
    }

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->filter(filter, static_cast<ssize_t>(num_matched));
    chunk.setColumns(std::move(columns), num_matched);
    return num_matched;
}

void IcebergBitmapPositionDeleteTransform::transform(Chunk & chunk)
{
    DeletionVectorTransform::transform(chunk, bitmap);
}

void IcebergBitmapPositionDeleteTransform::initialize()
{
    for (const auto & position_deletes_object : iceberg_object_info->info.position_deletes_objects)
    {
        if (!position_deletes_object.isDeletionVector())
            continue;

        auto deletion_vector = readIcebergDeletionVector(
            position_deletes_object.file_path,
            position_deletes_object.content_offset.value(),
            position_deletes_object.content_size_in_bytes.value(),
            object_storage,
            context,
            log);
        for (const auto position : *deletion_vector)
            bitmap.add(position);
    }

    for (auto & position_delete_file : position_delete_files)
    {
        const auto position_index = getColumnIndex(position_delete_file, IcebergPositionDeleteTransform::positions_column_name);
        const auto filename_index = getColumnIndex(position_delete_file, IcebergPositionDeleteTransform::data_file_path_column_name);

        while (auto delete_chunk = position_delete_file->read())
        {
            if (filterChunkToCurrentDataFile(delete_chunk, filename_index) == 0)
                continue;

            const auto & position_column = *delete_chunk.getColumns()[position_index];
            for (size_t i = 0; i < delete_chunk.getNumRows(); ++i)
                bitmap.add(position_column.get64(i));
        }
    }
}


void IcebergStreamingPositionDeleteTransform::initialize()
{
    for (const auto & position_deletes_object : iceberg_object_info->info.position_deletes_objects)
    {
        if (!position_deletes_object.isDeletionVector())
            continue;

        auto deletion_vector = readIcebergDeletionVector(
            position_deletes_object.file_path,
            position_deletes_object.content_offset.value(),
            position_deletes_object.content_size_in_bytes.value(),
            object_storage,
            context,
            log);
        deletion_vectors.emplace_back(std::move(deletion_vector));
        const auto & deletion_vector_state = deletion_vectors.back();
        if (deletion_vector_state.iterator != deletion_vector_state.end)
            latest_positions.emplace(
                static_cast<size_t>(*deletion_vector_state.iterator),
                DeleteSourceRef{.kind = DeleteSourceKind::Vector, .index = deletion_vectors.size() - 1});
    }

    for (size_t i = 0; i < position_delete_files.size(); ++i)
    {
        auto & position_delete_file = position_delete_files[i];
        size_t position_index = getColumnIndex(position_delete_file, IcebergPositionDeleteTransform::positions_column_name);
        size_t filename_index = getColumnIndex(position_delete_file, IcebergPositionDeleteTransform::data_file_path_column_name);

        position_delete_file_column_indices.push_back(PositionDeleteFileIndexes{
            .filename_index = filename_index,
            .position_index = position_index
        });
        iterator_at_latest_chunks.push_back(0);
        latest_chunks.emplace_back();
        fetchNewChunkFromSource(i);
    }
}

IcebergStreamingPositionDeleteTransform::DeletionVectorState::DeletionVectorState(std::unique_ptr<roaring::Roaring64Map> bitmap_)
    : bitmap(std::move(bitmap_))
    , iterator(bitmap->begin())
    , end(bitmap->end())
{
}

void IcebergStreamingPositionDeleteTransform::fetchNewChunkFromSource(size_t position_delete_file_index)
{
    iterator_at_latest_chunks[position_delete_file_index] = 0;

    /// The position delete file is sorted by (file_path, pos), so positions for one data file
    /// arrive in ascending order. But a chunk read from the Parquet reader may still
    /// contain rows for other data files because filter_actions_dag is only used for
    /// row-group/page pruning, not row-level filtering. Drop those rows here so the
    /// streaming merge invariant (positions ascending per source) is preserved.
    /// Keep reading until we find a chunk with at least one matching row, or end of source.
    while (true)
    {
        auto chunk = position_delete_files[position_delete_file_index]->read();
        if (!chunk.hasRows())
        {
            latest_chunks[position_delete_file_index] = std::move(chunk);
            return;
        }

        const auto filename_index = position_delete_file_column_indices[position_delete_file_index].filename_index;
        if (filterChunkToCurrentDataFile(chunk, filename_index) == 0)
            continue;

        const auto position_index = position_delete_file_column_indices[position_delete_file_index].position_index;
        size_t first_position_value_in_delete_file = chunk.getColumns()[position_index]->get64(0);
        latest_positions.emplace(
            first_position_value_in_delete_file,
            DeleteSourceRef{.kind = DeleteSourceKind::File, .index = position_delete_file_index});
        latest_chunks[position_delete_file_index] = std::move(chunk);
        return;
    }
}

void IcebergStreamingPositionDeleteTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    IColumn::Filter filter(num_rows, true);
    size_t num_rows_after_filtration = chunk.getNumRows();
    auto chunk_info = chunk.getChunkInfos().get<ChunkInfoRowNumbers>();
    if (!chunk_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ChunkInfoRowNumbers does not exist");

    size_t num_indices = chunk_info->applied_filter.has_value() ? chunk_info->applied_filter->size() : chunk.getNumRows();

    /// We get chunks in order of increasing row number because:
    ///  * this transform should be immediately after the IInputFormat
    ///    (typically ParquetV3BlockInputFormat) in the pipeline,
    ///  * IInputFormat outputs chunks in order of row number even if it uses multiple threads
    ///    internally; for parquet IcebergMetadata::modifyFormatSettings sets
    ///    `format_settings.parquet.preserve_order = true` to ensure this, other formats return
    ///    chunks in order by default.
    if (previous_chunk_end_offset && previous_chunk_end_offset.value() > chunk_info->row_num_offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunks offsets should increase.");
    previous_chunk_end_offset = chunk_info->row_num_offset + num_indices;

    size_t idx_in_chunk = 0;
    for (size_t i = 0; i < num_indices; i++)
    {
        if (!chunk_info->applied_filter.has_value() || chunk_info->applied_filter.value()[i])
        {
            size_t row_idx = chunk_info->row_num_offset + i;

            while (!latest_positions.empty())
            {
                auto it = latest_positions.begin();
                if (it->first < row_idx)
                {
                    const auto source = it->second;
                    latest_positions.erase(it);
                    if (source.kind == DeleteSourceKind::File)
                    {
                        if (iterator_at_latest_chunks[source.index] + 1 >= latest_chunks[source.index].getNumRows()
                            && latest_chunks[source.index].getNumRows() > 0)
                        {
                            fetchNewChunkFromSource(source.index);
                        }
                        else
                        {
                            ++iterator_at_latest_chunks[source.index];
                            const auto position_index = position_delete_file_column_indices[source.index].position_index;
                            const size_t next_position = latest_chunks[source.index].getColumns()[position_index]->get64(iterator_at_latest_chunks[source.index]);
                            latest_positions.emplace(next_position, source);
                        }
                    }
                    else
                    {
                        auto & deletion_vector = deletion_vectors[source.index];
                        ++deletion_vector.iterator;
                        if (deletion_vector.iterator != deletion_vector.end)
                            latest_positions.emplace(static_cast<size_t>(*deletion_vector.iterator), source);
                    }
                }
                else if (it->first == row_idx)
                {
                    filter[idx_in_chunk] = false;

                    if (chunk_info->applied_filter.has_value())
                        chunk_info->applied_filter.value()[i] = false;

                    --num_rows_after_filtration;
                    break;
                }
                else
                    break;
            }

            idx_in_chunk += 1;
        }
    }
    chassert(idx_in_chunk == chunk.getNumRows());

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->filter(filter, -1);

    if (!chunk_info->applied_filter.has_value())
        chunk_info->applied_filter.emplace(std::move(filter));

    chunk.setColumns(std::move(columns), num_rows_after_filtration);
}

}

#endif
