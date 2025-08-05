#include <Processors/Sources/ConstChunkGenerator.h>
#include <Storages/ObjectStorage/Source/ReaderHolder.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB {

ReaderHolder createReader(
    size_t processor,
    ObjectInfoPtr object_info,
    // const std::shared_ptr<IObjectIterator> & file_iterator,
    const StorageObjectStorageConfigurationPtr & configuration,
    StorageObjectStorageQuerySettings query_settings,
    const ObjectStoragePtr & object_storage,
    ReadFromFormatInfo & read_from_format_info,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context_,
    SchemaCache * schema_cache,
    const LoggerPtr & log,
    size_t max_block_size,
    FormatParserSharedResourcesPtr parser_shared_resources,
    FormatFilterInfoPtr format_filter_info,
    bool need_only_count,
    bool use_cache_for_count)
{
    ObjectInfoPtr object_info;
    // auto query_settings = configuration->getQuerySettings(context_);

    // do
    // {
    //     object_info = file_iterator->next(processor);

    //     if (!object_info || object_info->getPath().empty())
    //         return {};

    //     if (!object_info->metadata)
    //     {
    //         const auto & path = object_info->isArchive() ? object_info->getPathToArchive() : object_info->getPath();

    //         if (query_settings.ignore_non_existent_file)
    //         {
    //             auto metadata = object_storage->tryGetObjectMetadata(path);
    //             if (!metadata)
    //                 return {};

    //             object_info->metadata = metadata;
    //         }
    //         else
    //             object_info->metadata = object_storage->getObjectMetadata(path);
    //     }
    // }
    // while (query_settings.skip_empty_files && object_info->metadata->size_bytes == 0);

    QueryPipelineBuilder builder;
    std::shared_ptr<ISource> source;
    std::unique_ptr<ReadBuffer> read_buf;

    auto try_get_num_rows_from_cache = [&]() -> std::optional<size_t>
    {
        if (!schema_cache)
            return std::nullopt;

        const auto cache_key = getKeyForSchemaCache(
            configuration->getUniqueStoragePathIdentifier(*object_info),
            configuration->format,
            format_settings,
            context_);

        auto get_last_mod_time = [&]() -> std::optional<time_t>
        {
            return object_info->metadata
                ? std::optional<size_t>(object_info->metadata->last_modified.epochTime())
                : std::nullopt;
        };
        return schema_cache->tryGetNumRows(cache_key, get_last_mod_time);
    };

    std::optional<size_t> num_rows_from_cache
        = need_only_count && use_cache_for_count ? try_get_num_rows_from_cache() : std::nullopt;

    if (num_rows_from_cache)
    {
        /// We should not return single chunk with all number of rows,
        /// because there is a chance that this chunk will be materialized later
        /// (it can cause memory problems even with default values in columns or when virtual columns are requested).
        /// Instead, we use special ConstChunkGenerator that will generate chunks
        /// with max_block_size rows until total number of rows is reached.
        builder.init(Pipe(std::make_shared<ConstChunkGenerator>(
                              std::make_shared<const Block>(read_from_format_info.format_header), *num_rows_from_cache, max_block_size)));
    }
    else
    {
        CompressionMethod compression_method;
        if (const auto * object_info_in_archive = dynamic_cast<const ArchiveIterator::ObjectInfoInArchive *>(object_info.get()))
        {
            compression_method = chooseCompressionMethod(configuration->getPathInArchive(), configuration->compression_method);
            const auto & archive_reader = object_info_in_archive->archive_reader;
            read_buf = archive_reader->readFile(object_info_in_archive->path_in_archive, /*throw_on_not_found=*/true);
        }
        else
        {
            compression_method = chooseCompressionMethod(object_info->getFileName(), configuration->compression_method);
            read_buf = createReadBuffer(*object_info, object_storage, context_, log);
        }

        Block initial_header = read_from_format_info.format_header;

        if (auto initial_schema = configuration->getInitialSchemaByPath(context_, object_info->getPath()))
        {
            Block sample_header;
            for (const auto & [name, type] : *initial_schema)
            {
                sample_header.insert({type->createColumn(), type, name});
            }
            initial_header = sample_header;
        }

        auto input_format = FormatFactory::instance().getInput(
            configuration->format,
            *read_buf,
            initial_header,
            context_,
            max_block_size,
            format_settings,
            parser_shared_resources,
            format_filter_info,
            true /* is_remote_fs */,
            compression_method,
            need_only_count);

        if (need_only_count)
            input_format->needOnlyCount(need_only_count);

        input_format->setSerializationHints(read_from_format_info.serialization_hints);

        builder.init(Pipe(input_format));

        std::optional<ActionsDAG> transformer;
        if (object_info->data_lake_metadata && object_info->data_lake_metadata->transform)
        {
            transformer = object_info->data_lake_metadata->transform->clone();
            /// FIXME: This is currently not done for the below case (configuration->getSchemaTransformer())
            /// because it is an iceberg case where transformer contains columns ids (just increasing numbers)
            /// which do not match requested_columns (while here requested_columns were adjusted to match physical columns).
            transformer->removeUnusedActions(read_from_format_info.requested_columns.getNames());
        }
        if (!transformer)
        {
            if (auto schema_transformer = configuration->getSchemaTransformer(context_, object_info->getPath()))
                transformer = schema_transformer->clone();
        }

        if (transformer.has_value())
        {
            auto schema_modifying_actions = std::make_shared<ExpressionActions>(std::move(transformer.value()));
            builder.addSimpleTransform([&](const SharedHeader & header)
            {
                return std::make_shared<ExpressionTransform>(header, schema_modifying_actions);
            });
        }

        if (read_from_format_info.columns_description.hasDefaults())
        {
            builder.addSimpleTransform(
                [&](const SharedHeader & header)
                {
                    return std::make_shared<AddingDefaultsTransform>(header, read_from_format_info.columns_description, *input_format, context_);
                });
        }

        source = input_format;
    }

    /// Add ExtractColumnsTransform to extract requested columns/subcolumns
    /// from chunk read by IInputFormat.
    builder.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<ExtractColumnsTransform>(header, read_from_format_info.requested_columns);
    });

    auto pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    auto current_reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    ProfileEvents::increment(ProfileEvents::EngineFileLikeReadFiles);

    return ReaderHolder(
        object_info, std::move(read_buf), std::move(source), std::move(pipeline), std::move(current_reader));
}

};