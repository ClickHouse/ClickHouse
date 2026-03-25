#include <memory>
#include <optional>
#include <Common/CurrentThread.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeSource.h>
#include <Storages/ObjectStorage/DataLakes/DeletionVectorTransform.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/ProfileEvents.h>
#include <Core/SettingsEnums.h>

#include "config.h"

#if USE_PARQUET
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#endif

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event EngineFileLikeReadFiles;
    extern const Event ObjectStorageReadObjects;
}

namespace CurrentMetrics
{
    extern const Metric StorageObjectStorageThreads;
    extern const Metric StorageObjectStorageThreadsActive;
    extern const Metric StorageObjectStorageThreadsScheduled;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_download_buffer_size;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsBool use_cache_for_count_from_files;
    extern const SettingsBool use_parquet_metadata_cache;
    extern const SettingsBool input_format_parquet_use_native_reader_v3;
}

DataLakeSource::DataLakeSource(
    String name_,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const StorageObjectStorageTableOptions & table_options_,
    StorageSnapshotPtr storage_snapshot_,
    const ReadFromFormatInfo & info,
    const std::optional<FormatSettings> & format_settings_,
    ContextPtr context_,
    UInt64 max_block_size_,
    std::shared_ptr<IObjectIterator> file_iterator_,
    FormatParserSharedResourcesPtr parser_shared_resources_,
    FormatFilterInfoPtr format_filter_info_,
    bool need_only_count_,
    IDataLakeMetadata * metadata_)
    : ISource(std::make_shared<const Block>(info.source_header), false)
    , name(std::move(name_))
    , object_storage(object_storage_)
    , configuration(configuration_)
    , table_options(table_options_)
    , storage_snapshot(std::move(storage_snapshot_))
    , read_context(context_)
    , format_settings(format_settings_)
    , max_block_size(max_block_size_)
    , need_only_count(need_only_count_)
    , parser_shared_resources(std::move(parser_shared_resources_))
    , format_filter_info(std::move(format_filter_info_))
    , read_from_format_info(info)
    , create_reader_pool(
          std::make_shared<ThreadPool>(
              CurrentMetrics::StorageObjectStorageThreads,
              CurrentMetrics::StorageObjectStorageThreadsActive,
              CurrentMetrics::StorageObjectStorageThreadsScheduled,
              1 /* max_threads */))
    , file_iterator(file_iterator_)
    , schema_cache(getSchemaCache(context_, configuration->getTypeName()))
    , metadata(metadata_)
    , create_reader_scheduler(threadPoolCallbackRunnerUnsafe<ReaderHolder>(*create_reader_pool, ThreadName::READER_POOL))
{
}

DataLakeSource::~DataLakeSource()
{
    LOG_DEBUG(log, "Source finished: files_read={}", total_files_read);
    create_reader_pool->wait();
}

std::string DataLakeSource::getUniqueStoragePathIdentifier(
    const StorageObjectStorageConfiguration & configuration, const ObjectInfo & object_info, bool include_connection_info)
{
    return StorageObjectStorageSource::getUniqueStoragePathIdentifier(configuration, object_info, include_connection_info);
}

void DataLakeSource::lazyInitialize()
{
    if (initialized)
        return;

    reader = createReader();
    if (reader)
    {
        ++total_files_read;
        reader_future = createReaderAsync();
    }
    initialized = true;
}

Chunk DataLakeSource::generate()
{
    lazyInitialize();

    while (true)
    {
        if (isCancelled() || !reader)
        {
            if (reader)
                reader->cancel();
            break;
        }

        Chunk chunk;
        if (reader->pull(chunk))
        {
            UInt64 num_rows = chunk.getNumRows();
            total_rows_in_file += num_rows;

            size_t chunk_size = 0;
            if (const auto * input_format = reader.getInputFormat())
                chunk_size = input_format->getApproxBytesReadForChunk();

            progress(num_rows, chunk_size ? chunk_size : chunk.bytes());

            const auto & object_info = reader.getObjectInfo();
            const auto & filename = object_info->getFileName();
            std::string full_path = object_info->getPath();

            const auto reading_path = table_options.getPathForRead(configuration->getRawPath()).path;

            if (!full_path.starts_with(reading_path))
                full_path = fs::path(reading_path) / object_info->getPath();

            auto object_metadata = object_info->getObjectMetadata();

            chassert(object_metadata);

            const auto path = getUniqueStoragePathIdentifier(*configuration, *object_info, false);

            VirtualColumnUtils::addRequestedFileLikeStorageVirtualsToChunk(
                chunk,
                read_from_format_info.requested_virtual_columns,
                {
                    .path = path,
                    .size = object_metadata->size_bytes,
                    .filename = &filename,
                    .last_modified = object_metadata->last_modified,
                    .etag = &(object_metadata->etag),
                    .tags = &(object_metadata->tags),
                    .data_lake_snapshot_version = file_iterator->getSnapshotVersion(),
                },
                read_context);

#if USE_PARQUET
            if (chunk_size && chunk.hasColumns())
            {
                if (auto * delta_lake_metadata = dynamic_cast<DeltaLakeMetadata *>(metadata))
                {
                    auto partition_columns = delta_lake_metadata->getPartitionColumns();
                    if (!partition_columns.empty())
                    {
                        auto partition_values = partition_columns.find(full_path);
                        if (partition_values != partition_columns.end())
                        {
                            for (const auto & [name_and_type, value] : partition_values->second)
                            {
                                if (!read_from_format_info.source_header.has(name_and_type.name))
                                    continue;

                                const auto column_pos = read_from_format_info.source_header.getPositionByName(name_and_type.name);
                                auto partition_column = name_and_type.type->createColumnConst(chunk.getNumRows(), value)->convertToFullColumnIfConst();
                                /// This column is filled with default value now, remove it.
                                chunk.erase(column_pos);
                                /// Add correct values.
                                if (column_pos < chunk.getNumColumns())
                                    chunk.addColumn(column_pos, std::move(partition_column));
                                else
                                    chunk.addColumn(std::move(partition_column));
                            }
                        }
                    }
                }
            }
#endif

            /// Convert any Const columns to full columns before returning.
            if (chunk.hasColumns())
            {
                size_t chunk_num_rows = chunk.getNumRows();
                auto columns = chunk.detachColumns();
                for (auto & column : columns)
                {
                    if (column->isConst())
                        column = column->cloneResized(chunk_num_rows)->convertToFullColumnIfConst();
                }
                chunk.setColumns(std::move(columns), chunk_num_rows);
            }

            return chunk;
        }

        if (reader.getInputFormat() && read_context->getSettingsRef()[Setting::use_cache_for_count_from_files]
            && !format_filter_info->filter_actions_dag)
            addNumRowsToCache(*reader.getObjectInfo(), total_rows_in_file);

        total_rows_in_file = 0;

        assert(reader_future.valid());
        reader = reader_future.get();

        if (!reader)
            break;

        ++total_files_read;

        /// Even if task is finished the thread may be not freed in pool.
        /// So wait until it will be freed before scheduling a new task.
        create_reader_pool->wait();
        reader_future = createReaderAsync();
    }

    return {};
}

void DataLakeSource::addNumRowsToCache(const ObjectInfo & object_info, size_t num_rows)
{
    const auto cache_key = getKeyForSchemaCache(
        getUniqueStoragePathIdentifier(*configuration, object_info),
        table_options.format,
        format_settings,
        read_context);
    schema_cache.addNumRows(cache_key, num_rows);
}

DataLakeSource::ReaderHolder DataLakeSource::createReader()
{
    return createReader(
        0,
        file_iterator,
        configuration,
        table_options,
        object_storage,
        read_from_format_info,
        format_settings,
        read_context,
        &schema_cache,
        log,
        max_block_size,
        parser_shared_resources,
        format_filter_info,
        need_only_count,
        metadata);
}

DataLakeSource::ReaderHolder DataLakeSource::createReader(
    size_t processor,
    const std::shared_ptr<IObjectIterator> & file_iterator,
    const StorageObjectStorageConfigurationPtr & configuration,
    const StorageObjectStorageTableOptions & table_options,
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
    IDataLakeMetadata * /* metadata */)
{
    ObjectInfoPtr object_info;
    auto query_settings = configuration->getQuerySettings(context_);

    do
    {
        object_info = file_iterator->next(processor);

        if (!object_info || object_info->getPath().empty())
            return {};

        if (!object_info->getObjectMetadata())
        {
            bool with_tags = read_from_format_info.requested_virtual_columns.contains("_tags");
            const auto & path = object_info->getPath();

            if (query_settings.ignore_non_existent_file)
            {
                auto obj_metadata = object_storage->tryGetObjectMetadata(path, with_tags);
                if (!obj_metadata)
                    return {};

                object_info->setObjectMetadata(obj_metadata.value());
            }
            else
                object_info->setObjectMetadata(object_storage->getObjectMetadata(path, with_tags));
        }
    } while (query_settings.skip_empty_files && object_info->getObjectMetadata()->size_bytes == 0);

    QueryPipelineBuilder builder;
    std::shared_ptr<ISource> source;
    std::unique_ptr<ReadBuffer> read_buf;

    auto try_get_num_rows_from_cache = [&]() -> std::optional<size_t>
    {
        if (!schema_cache)
            return std::nullopt;

        const auto cache_key = getKeyForSchemaCache(
            getUniqueStoragePathIdentifier(*configuration, *object_info),
            table_options.format,
            format_settings,
            context_);

        auto get_last_mod_time = [&]() -> std::optional<time_t>
        {
            return object_info->getObjectMetadata() ? std::optional<size_t>(object_info->getObjectMetadata()->last_modified.epochTime())
                                                    : std::nullopt;
        };
        return schema_cache->tryGetNumRows(cache_key, get_last_mod_time);
    };

    std::optional<size_t> num_rows_from_cache
        = need_only_count && context_->getSettingsRef()[Setting::use_cache_for_count_from_files] ? try_get_num_rows_from_cache() : std::nullopt;

    if (num_rows_from_cache)
    {
        auto names_and_types = read_from_format_info.columns_description.getAllPhysical();
        ColumnsWithTypeAndName columns;
        for (const auto & [col_name, type] : names_and_types)
            columns.emplace_back(type->createColumn(), type, col_name);
        builder.init(Pipe(std::make_shared<ConstChunkGenerator>(
                              std::make_shared<const Block>(columns), *num_rows_from_cache, max_block_size)));
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::ObjectStorageReadObjects);

        CompressionMethod compression_method = chooseCompressionMethod(object_info->getFileName(), table_options.compression_method);
        read_buf = createReadBuffer(object_info->relative_path_with_metadata, object_storage, context_, log);

        Block initial_header = read_from_format_info.format_header;

        auto filter_info = format_filter_info;

        chassert(object_info->getObjectMetadata().has_value());

        LOG_DEBUG(
            log,
            "Reading object '{}', size: {} bytes, with format: {}",
            object_info->getPath(),
            object_info->getObjectMetadata()->size_bytes,
            table_options.format);

        bool use_native_reader_v3 = format_settings.has_value()
            ? format_settings->parquet.use_native_reader_v3
            : context_->getSettingsRef()[Setting::input_format_parquet_use_native_reader_v3];

        InputFormatPtr input_format;
        if (context_->getSettingsRef()[Setting::use_parquet_metadata_cache] && use_native_reader_v3
            && (table_options.format == "Parquet")
            && !object_info->getObjectMetadata()->etag.empty())
        {
            const std::optional<RelativePathWithMetadata> object_with_metadata = object_info->relative_path_with_metadata;
            input_format = FormatFactory::instance().getInputWithMetadata(
                table_options.format,
                *read_buf,
                initial_header,
                context_,
                max_block_size,
                object_with_metadata,
                format_settings,
                parser_shared_resources,
                filter_info,
                true /* is_remote_fs */,
                compression_method,
                need_only_count,
                std::nullopt /*min_block_size_bytes*/,
                std::nullopt /*min_block_size_rows*/,
                std::nullopt /*max_block_size_bytes*/);
        }
        else
        {
            input_format = FormatFactory::instance().getInput(
                table_options.format,
                *read_buf,
                initial_header,
                context_,
                max_block_size,
                format_settings,
                parser_shared_resources,
                filter_info,
                true /* is_remote_fs */,
                compression_method,
                need_only_count);
        }

        input_format->setBucketsToRead(object_info->file_bucket_info);
        input_format->setSerializationHints(read_from_format_info.serialization_hints);

        if (need_only_count)
            input_format->needOnlyCount();

        builder.init(Pipe(input_format));

        if (object_info->data_lake_metadata
            && object_info->data_lake_metadata->excluded_rows
            && object_info->data_lake_metadata->excluded_rows->size() > 0)
        {
            builder.addSimpleTransform([&](const SharedHeader & header)
            {
                return std::make_shared<DeletionVectorTransform>(header, object_info->data_lake_metadata->excluded_rows);
            });
        }

        std::optional<ActionsDAG> schema_transform;
        if (object_info->data_lake_metadata && object_info->data_lake_metadata->schema_transform)
        {
            schema_transform = object_info->data_lake_metadata->schema_transform->clone();
            schema_transform->removeUnusedActions(read_from_format_info.requested_columns.getNames());
        }

        if (schema_transform.has_value())
        {
            auto schema_modifying_actions = std::make_shared<ExpressionActions>(std::move(schema_transform.value()));
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

std::future<DataLakeSource::ReaderHolder> DataLakeSource::createReaderAsync()
{
    return create_reader_scheduler([=, this] { return createReader(); }, Priority{});
}

DataLakeSource::ReaderHolder::ReaderHolder(
    ObjectInfoPtr object_info_,
    std::unique_ptr<ReadBuffer> read_buf_,
    std::shared_ptr<ISource> source_,
    std::unique_ptr<QueryPipeline> pipeline_,
    std::unique_ptr<PullingPipelineExecutor> reader_)
    : object_info(std::move(object_info_))
    , read_buf(std::move(read_buf_))
    , source(std::move(source_))
    , pipeline(std::move(pipeline_))
    , reader(std::move(reader_))
{
}

DataLakeSource::ReaderHolder &
DataLakeSource::ReaderHolder::operator=(ReaderHolder && other) noexcept
{
    /// The order of destruction is important.
    /// reader uses pipeline, pipeline uses read_buf.
    reader = std::move(other.reader);
    pipeline = std::move(other.pipeline);
    source = std::move(other.source);
    read_buf = std::move(other.read_buf);
    object_info = std::move(other.object_info);
    return *this;
}

}
