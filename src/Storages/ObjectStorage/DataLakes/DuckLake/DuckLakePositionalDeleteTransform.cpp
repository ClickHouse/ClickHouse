#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakePositionalDeleteTransform.h>

#if USE_PARQUET

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Storages/ObjectStorage/DataLakes/DeletionVectorTransform.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace Setting
{
extern const SettingsNonZeroUInt64 max_block_size;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

DuckLakePositionalDeleteTransform::DuckLakePositionalDeleteTransform(
    const SharedHeader & header_,
    ObjectStoragePtr object_storage_,
    DuckLakeDataObjectInfoPtr object_info_,
    const std::optional<FormatSettings> & format_settings_,
    FormatParserSharedResourcesPtr parser_shared_resources_,
    ContextPtr context_)
    : ISimpleTransform(header_, header_, /* skip_empty_chunks */ false)
    , object_storage(std::move(object_storage_))
    , object_info(std::move(object_info_))
    , format_settings(format_settings_)
    , parser_shared_resources(std::move(parser_shared_resources_))
    , context(std::move(context_))
{
    initialize();
}

void DuckLakePositionalDeleteTransform::initialize()
{
    for (const auto & delete_file : object_info->positional_delete_files)
    {
        const String format = "Parquet";

        auto object_metadata = object_storage->getObjectMetadata(delete_file.path, /* with_tags= */ false);
        auto delete_object_info = RelativePathWithMetadata{delete_file.path, object_metadata};

        Block initial_header;
        {
            std::unique_ptr<ReadBuffer> read_buf_schema = createReadBuffer(delete_object_info, object_storage, context, log);
            auto schema_reader = FormatFactory::instance().getSchemaReader(format, *read_buf_schema, context);
            auto columns_with_names = schema_reader->readSchema();
            ColumnsWithTypeAndName initial_header_data;
            for (const auto & elem : columns_with_names)
                initial_header_data.push_back(ColumnWithTypeAndName(elem.type, elem.name));
            initial_header = Block(initial_header_data);
        }

        CompressionMethod compression_method = chooseCompressionMethod(delete_file.path, "auto");
        auto read_buffer = createReadBuffer(delete_object_info, object_storage, context, log);

        auto delete_source = FormatFactory::instance().getInput(
            format,
            *read_buffer,
            initial_header,
            context,
            context->getSettingsRef()[Setting::max_block_size],
            format_settings,
            parser_shared_resources,
            /* format_filter_info= */ nullptr,
            true /* is_remote_fs */,
            compression_method);

        /// DuckLake positional delete files have schema (file_path VARCHAR, pos BIGINT).
        const auto & delete_header = delete_source->getOutputs().back().getHeader();
        std::optional<size_t> pos_index;
        for (size_t i = 0; i < delete_header.getNames().size(); ++i)
        {
            if (delete_header.getNames()[i] == "pos")
            {
                pos_index = i;
                break;
            }
        }
        if (!pos_index.has_value())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake delete file '{}' has no 'pos' column", delete_file.path);

        while (auto delete_chunk = delete_source->read())
        {
            const auto & pos_column = *delete_chunk.getColumns()[*pos_index];
            for (size_t i = 0; i < delete_chunk.getNumRows(); ++i)
                excluded_rows.add(static_cast<UInt64>(pos_column.get64(i)));
        }
    }

    LOG_TRACE(log, "Loaded {} deleted positions for data file {}", excluded_rows.size(), object_info->getPath());
}

void DuckLakePositionalDeleteTransform::transform(Chunk & chunk)
{
    DeletionVectorTransform::transform(chunk, excluded_rows);
}

}

#endif
