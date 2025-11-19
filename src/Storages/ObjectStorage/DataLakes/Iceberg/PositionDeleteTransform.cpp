#include <Storages/ObjectStorage/Utils.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/logger_useful.h>
#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>

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
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteObject.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

namespace DB::Setting
{
extern const SettingsNonZeroUInt64 max_block_size;
}
namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
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
    /// Create filter on the data object to get interested rows
    auto iceberg_data_path = iceberg_object_info->info.data_object_file_path_key;
    ASTPtr where_ast = makeASTFunction(
        "equals",
        std::make_shared<ASTIdentifier>(IcebergPositionDeleteTransform::data_file_path_column_name),
        std::make_shared<ASTLiteral>(Field(iceberg_data_path)));

    for (const auto & position_deletes_object : iceberg_object_info->info.position_deletes_objects)
    {
        /// Skip position deletes that do not match the data file path.
        if (position_deletes_object.reference_data_file_path.has_value()
            && position_deletes_object.reference_data_file_path != iceberg_data_path)
            continue;

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
            std::make_shared<FormatParserSharedResources>(context->getSettingsRef(), 1),
            std::make_shared<FormatFilterInfo>(actions_dag_ptr, context, nullptr, nullptr, nullptr),
            true /* is_remote_fs */,
            compression_method);

        delete_sources.push_back(std::move(delete_format));
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

void IcebergBitmapPositionDeleteTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    IColumn::Filter delete_vector(num_rows, true);
    size_t num_rows_after_filtration = num_rows;

    auto chunk_info = chunk.getChunkInfos().get<ChunkInfoRowNumbers>();
    if (!chunk_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ChunkInfoRowNumbers does not exist");

    size_t row_num_offset = chunk_info->row_num_offset;
    auto & applied_filter = chunk_info->applied_filter;
    size_t num_indices = applied_filter.has_value() ? applied_filter->size() : num_rows;
    size_t idx_in_chunk = 0;
    for (size_t i = 0; i < num_indices; i++)
    {
        if (!applied_filter.has_value() || applied_filter.value()[i])
        {
            size_t row_idx = row_num_offset + i;
            if (bitmap.rb_contains(row_idx))
            {
                delete_vector[idx_in_chunk] = false;

                /// If we already have a _row_number-indexed filter vector, update it in place.
                if (applied_filter.has_value())
                    applied_filter.value()[i] = false;

                num_rows_after_filtration--;
            }
            idx_in_chunk += 1;
        }
    }
    chassert(idx_in_chunk == num_rows);

    if (num_rows_after_filtration == num_rows)
        return;

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->filter(delete_vector, -1);

    /// If it's the first filtering we do on this Chunk (i.e. its _row_number-s were consecutive),
    /// assign its applied_filter.
    if (!applied_filter.has_value())
        applied_filter.emplace(std::move(delete_vector));

    chunk.setColumns(std::move(columns), num_rows_after_filtration);
}

void IcebergBitmapPositionDeleteTransform::initialize()
{
    for (auto & delete_source : delete_sources)
    {
        while (auto delete_chunk = delete_source->read())
        {
            int position_index = getColumnIndex(delete_source, IcebergPositionDeleteTransform::positions_column_name);
            int filename_index = getColumnIndex(delete_source, IcebergPositionDeleteTransform::data_file_path_column_name);

            auto position_column = delete_chunk.getColumns()[position_index];
            auto filename_column = delete_chunk.getColumns()[filename_index];

            for (size_t i = 0; i < delete_chunk.getNumRows(); ++i)
            {
                auto position_to_delete = position_column->get64(i);
                bitmap.add(position_to_delete);
            }
        }
    }
}


void IcebergStreamingPositionDeleteTransform::initialize()
{
    for (size_t i = 0; i < delete_sources.size(); ++i)
    {
        auto & delete_source = delete_sources[i];
        size_t position_index = getColumnIndex(delete_source, IcebergPositionDeleteTransform::positions_column_name);
        size_t filename_index = getColumnIndex(delete_source, IcebergPositionDeleteTransform::data_file_path_column_name);

        delete_source_column_indices.push_back(PositionDeleteFileIndexes{
            .filename_index = filename_index,
            .position_index = position_index
        });
        auto latest_chunk = delete_source->read();
        iterator_at_latest_chunks.push_back(0);
        if (latest_chunk.hasRows())
        {
            size_t first_position_value_in_delete_file = latest_chunk.getColumns()[delete_source_column_indices.back().position_index]->get64(0);
            latest_positions.insert(std::pair<size_t, size_t>{first_position_value_in_delete_file, i});
        }
        latest_chunks.push_back(std::move(latest_chunk));
    }
}

void IcebergStreamingPositionDeleteTransform::fetchNewChunkFromSource(size_t delete_source_index)
{
    auto latest_chunk = delete_sources[delete_source_index]->read();
    if (latest_chunk.hasRows())
    {
        size_t first_position_value_in_delete_file = latest_chunk.getColumns()[delete_source_column_indices[delete_source_index].position_index]->get64(0);
        latest_positions.insert(std::pair<size_t, size_t>{first_position_value_in_delete_file, delete_source_index});
    }

    iterator_at_latest_chunks[delete_source_index] = 0;
    latest_chunks[delete_source_index] = std::move(latest_chunk);
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
                    size_t delete_source_index = it->second;
                    latest_positions.erase(it);
                    if (iterator_at_latest_chunks[delete_source_index] + 1 >= latest_chunks[delete_source_index].getNumRows() && latest_chunks[delete_source_index].getNumRows() > 0)
                    {
                        fetchNewChunkFromSource(delete_source_index);
                    }
                    else
                    {
                        ++iterator_at_latest_chunks[delete_source_index];
                        auto position_index = delete_source_column_indices[delete_source_index].position_index;
                        size_t next_index_value_in_positional_delete_file = latest_chunks[delete_source_index].getColumns()[position_index]->get64(iterator_at_latest_chunks[delete_source_index]);
                        latest_positions.insert(std::pair<size_t, size_t>{next_index_value_in_positional_delete_file, delete_source_index});
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
