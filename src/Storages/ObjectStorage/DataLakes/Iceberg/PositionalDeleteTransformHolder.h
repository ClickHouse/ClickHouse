

#pragma once

#include <Processors/Formats/IInputFormat.h>
#include <Processors/IInputFormat.h>


std::shared_ptr<IInputFormat> StorageObjectStorageSource::getInputFormat(
    std::unique_ptr<ReadBuffer> & read_buf,
    std::unique_ptr<ReadBuffer> & read_buf_schema,
    Block & initial_header,
    ObjectStoragePtr object_storage,
    ConfigurationPtr configuration,
    ReadFromFormatInfo & read_from_format_info,
    const std::optional<FormatSettings> & format_settings,
    ObjectInfoPtr object_info,
    ContextPtr context_,
    const LoggerPtr & log,
    size_t max_block_size,
    size_t max_parsing_threads,
    bool need_only_count,
    bool read_all_columns)
{
    CompressionMethod compression_method;

    if (const auto * object_info_in_archive = dynamic_cast<const ArchiveIterator::ObjectInfoInArchive *>(object_info.get()))
    {
        compression_method = chooseCompressionMethod(configuration->getPathInArchive(), configuration->compression_method);
        const auto & archive_reader = object_info_in_archive->archive_reader;
        read_buf = archive_reader->readFile(object_info_in_archive->path_in_archive, /*throw_on_not_found=*/true);
        if (read_all_columns)
            read_buf_schema = archive_reader->readFile(object_info_in_archive->path_in_archive, /*throw_on_not_found=*/true);
    }
    else
    {
        compression_method = chooseCompressionMethod(object_info->getFileName(), configuration->compression_method);
        read_buf = createReadBuffer(*object_info, object_storage, context_, log);
        if (read_all_columns)
            read_buf_schema = createReadBuffer(*object_info, object_storage, context_, log);
    }

    if (auto initial_schema = configuration->getInitialSchemaByPath(object_info->getPath()))
    {
        Block sample_header;
        for (const auto & [name, type] : *initial_schema)
        {
            sample_header.insert({type->createColumn(), type, name});
        }
        initial_header = sample_header;
    }

    if (read_all_columns)
    {
        auto schema_reader = FormatFactory::instance().getSchemaReader(configuration->format, *read_buf_schema, context_);
        auto columns_with_names = schema_reader->readSchema();
        ColumnsWithTypeAndName initial_header_data;
        for (const auto & elem : columns_with_names)
        {
            initial_header_data.push_back(ColumnWithTypeAndName(elem.type, elem.name));
        }
        initial_header = Block(initial_header_data);
    }

    auto input_format = FormatFactory::instance().getInput(
        configuration->format,
        *read_buf,
        initial_header,
        context_,
        max_block_size,
        format_settings,
        need_only_count ? 1 : max_parsing_threads,
        std::nullopt,
        true /* is_remote_fs */,
        compression_method,
        need_only_count);

    input_format->setSerializationHints(read_from_format_info.serialization_hints);

    return input_format;
}


{
    const auto & positional_delete_objects = file_iterator->getPositionalDeleteObjects();

    std::vector<std::shared_ptr<IInputFormat>> delete_sources;

    std::vector<std::unique_ptr<ReadBuffer>> delete_read_buf;
    std::vector<std::unique_ptr<ReadBuffer>> delete_read_buf_schema;
    std::vector<Block> delete_block;

    for (const auto & positional_delete_object_info : positional_delete_objects)
    {
        int delete_sequence_id
            = positional_delete_object_info->metadata ? std::stoi(positional_delete_object_info->metadata->attributes["sequence_id"]) : -1;
        if (source_sequence_id > delete_sequence_id)
            continue;

        delete_read_buf.push_back(nullptr);
        delete_read_buf_schema.push_back(nullptr);
        delete_block.push_back({});

        auto delete_format = getInputFormat(
            delete_read_buf.back(),
            delete_read_buf_schema.back(),
            delete_block.back(),
            object_storage,
            configuration,
            read_from_format_info,
            format_settings,
            positional_delete_object_info,
            context_,
            log,
            max_block_size,
            max_parsing_threads,
            need_only_count,
            true);

        auto target_path = object_info->getPath();
        if (!target_path.empty() && target_path.front() != '/')
            target_path = "/" + target_path;
        ASTPtr where_ast = makeASTFunction(
            "equals",
            std::make_shared<ASTIdentifier>(PositionalDeleteTransform::filename_column_name),
            std::make_shared<ASTLiteral>(Field(target_path)));

        auto syntax_result = TreeRewriter(context_).analyze(where_ast, delete_block.back().getNamesAndTypesList());

        ExpressionAnalyzer analyzer(where_ast, syntax_result, context_);
        const std::optional<ActionsDAG> actions = analyzer.getActionsDAG(true);
        delete_format->setKeyCondition(actions, context_);

        delete_sources.push_back(std::move(delete_format));
    }

    builder.addSimpleTransform([&, delete_sources](const Block & header)
                               { return std::make_shared<PositionalDeleteTransform>(header, delete_sources, object_info->getPath()); });
}