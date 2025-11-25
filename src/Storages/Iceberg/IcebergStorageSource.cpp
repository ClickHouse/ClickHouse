#include <memory>
#include <optional>
#include <Core/Settings.h>
#include <Common/setThreadName.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <IO/Archives/ArchiveUtils.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/HivePartitioningUtils.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/VirtualColumnUtils.h>
#include <boost/operators.hpp>
#include <Common/SipHash.h>
#include <Common/parseGlobs.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#if ENABLE_DISTRIBUTED_CACHE
#include <DistributedCache/DistributedCacheRegistry.h>
#include <Disks/IO/ReadBufferFromDistributedCache.h>
#include <IO/DistributedCacheSettings.h>
#endif

#include <fmt/ranges.h>
#include <Common/ProfileEvents.h>
#include <Core/SettingsEnums.h>

namespace fs = std::filesystem;
namespace ProfileEvents
{
    extern const Event EngineFileLikeReadFiles;
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
    extern const SettingsString filesystem_cache_name;
    extern const SettingsUInt64 filesystem_cache_boundary_alignment;
    extern const SettingsBool use_iceberg_partition_pruning;
    extern const SettingsBool cluster_function_process_archive_on_multiple_nodes;
    extern const SettingsBool table_engine_read_through_distributed_cache;
    extern const SettingsObjectStorageGranularityLevel cluster_table_function_split_granularity;
}

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
}

StorageObjectStorageSource::StorageObjectStorageSource(
    String name_,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    StorageSnapshotPtr storage_snapshot_,
    const ReadFromFormatInfo & info,
    const std::optional<FormatSettings> & format_settings_,
    ContextPtr context_,
    UInt64 max_block_size_,
    std::shared_ptr<IObjectIterator> file_iterator_,
    FormatParserSharedResourcesPtr parser_shared_resources_,
    FormatFilterInfoPtr format_filter_info_,
    bool need_only_count_)
    : ISource(std::make_shared<const Block>(info.source_header), false)
    , name(std::move(name_))
    , object_storage(object_storage_)
    , configuration(configuration_)
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
    , schema_cache(StorageObjectStorage::getSchemaCache(context_, configuration->getTypeName()))
    , create_reader_scheduler(threadPoolCallbackRunnerUnsafe<ReaderHolder>(*create_reader_pool, ThreadName::READER_POOL))
{
}

StorageObjectStorageSource::~StorageObjectStorageSource()
{
    create_reader_pool->wait();
}

std::string StorageObjectStorageSource::getUniqueStoragePathIdentifier(
    const StorageObjectStorageConfiguration & configuration, const ObjectInfo & object_info, bool include_connection_info)
{
    auto path = object_info.getPath();
    if (path.starts_with("/"))
        path = path.substr(1);

    if (include_connection_info)
        return fs::path(configuration.getDataSourceDescription()) / path;
    return fs::path(configuration.getNamespace()) / path;
}

std::shared_ptr<IObjectIterator> IcebergStorageSource::createFileIterator(
    StorageObjectStorageConfigurationPtr configuration,
    const StorageObjectStorageQuerySettings & query_settings,
    ObjectStoragePtr object_storage,
    StorageMetadataPtr storage_metadata,
    bool distributed_processing,
    const ContextPtr & local_context,
    const ActionsDAG * filter_actions_dag,
    const NamesAndTypesList & virtual_columns,
    const NamesAndTypesList & hive_columns,
    std::function<void(FileProgress)> file_progress_callback)
{
    if (distributed_processing)
    {

        return std::make_unique<ReadTaskIterator>(
            local_context->getClusterFunctionReadTaskCallback(),
            local_context->getSettingsRef()[Setting::max_threads],
            false,
            object_storage,
            local_context);
    }

    std::unique_ptr<IObjectIterator> iterator;
    const auto & reading_path = configuration->getPathForRead();

    auto iter = configuration->iterate(
        filter_actions_dag, file_progress_callback, query_settings.list_object_keys_size, storage_metadata, local_context);

    auto iterator = iceberg_metadata.iterate(filter_actions_dag, file_progress_callback, query_settings.list_object_keys_size, storage_metadata, local_context);

    if (filter_actions_dag)
    {
        iter = std::make_shared<ObjectIteratorWithPathAndFileFilter>(
            std::move(iter),
            *filter_actions_dag,
            virtual_columns,
            hive_columns,
            configuration->getNamespace(),
            local_context);
    }
    if (local_context->getSettingsRef()[Setting::cluster_table_function_split_granularity] == ObjectStorageGranularityLevel::BUCKET)
    {
        iter = std::make_shared<ObjectIteratorSplitByBuckets>(
            std::move(iter),
            configuration->format,
            object_storage,
            local_context
        );
    }
    return iter;

    return iterator;
}

void StorageObjectStorageSource::lazyInitialize()
{
    if (initialized)
        return;

    reader = createReader();
    if (reader)
        reader_future = createReaderAsync();
    initialized = true;
}

Chunk StorageObjectStorageSource::generate()
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

            const auto reading_path = configuration->getPathForRead().path;

            if (!full_path.starts_with(reading_path))
                full_path = fs::path(reading_path) / object_info->getPath();

            auto object_metadata = object_info->getObjectMetadata();

            chassert(object_metadata);

            const auto path = getUniqueStoragePathIdentifier(*configuration, *object_info, false);

            /// The order is important, hive partition columns must be added before virtual columns
            /// because they are part of the schema
            if (!read_from_format_info.hive_partition_columns_to_read_from_file_path.empty())
            {
                HivePartitioningUtils::addPartitionColumnsToChunk(
                    chunk,
                    read_from_format_info.hive_partition_columns_to_read_from_file_path,
                    path);
            }

            VirtualColumnUtils::addRequestedFileLikeStorageVirtualsToChunk(
                chunk,
                read_from_format_info.requested_virtual_columns,
                {
                    .path = path,
                    .size = object_info->isArchive() ? object_info->fileSizeInArchive() : object_metadata->size_bytes,
                    .filename = &filename,
                    .last_modified = object_metadata->last_modified,
                    .etag = &(object_metadata->etag),
                    .tags = &(object_metadata->tags),
                    .data_lake_snapshot_version = file_iterator->getSnapshotVersion(),
                },
                read_context);

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

        /// Even if task is finished the thread may be not freed in pool.
        /// So wait until it will be freed before scheduling a new task.
        create_reader_pool->wait();
        reader_future = createReaderAsync();
    }

    return {};
}

void StorageObjectStorageSource::addNumRowsToCache(const ObjectInfo & object_info, size_t num_rows)
{
    const auto cache_key = getKeyForSchemaCache(
        getUniqueStoragePathIdentifier(*configuration, object_info),
        object_info.getFileFormat().value_or(configuration->format),
        format_settings,
        read_context);
    schema_cache.addNumRows(cache_key, num_rows);
}

StorageObjectStorageSource::ReaderHolder StorageObjectStorageSource::createReader()
{
    return createReader(
        0,
        file_iterator,
        configuration,
        object_storage,
        read_from_format_info,
        format_settings,
        read_context,
        &schema_cache,
        log,
        max_block_size,
        parser_shared_resources,
        format_filter_info,
        need_only_count);
}

StorageObjectStorageSource::ReaderHolder StorageObjectStorageSource::createReader(
    size_t processor,
    const std::shared_ptr<IObjectIterator> & file_iterator,
    const StorageObjectStorageConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage,
    ReadFromFormatInfo & read_from_format_info,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context_,
    SchemaCache * schema_cache,
    const LoggerPtr & log,
    size_t max_block_size,
    FormatParserSharedResourcesPtr parser_shared_resources,
    FormatFilterInfoPtr format_filter_info,
    bool need_only_count)
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
            const auto & path = object_info->isArchive() ? object_info->getPathToArchive() : object_info->getPath();

            if (query_settings.ignore_non_existent_file)
            {
                auto metadata = object_storage->tryGetObjectMetadata(path, with_tags);
                if (!metadata)
                    return {};

                object_info->setObjectMetadata(metadata.value());
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
            object_info->getFileFormat().value_or(configuration->format),
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
        /// We should not return single chunk with all number of rows,
        /// because there is a chance that this chunk will be materialized later
        /// (it can cause memory problems even with default values in columns or when virtual columns are requested).
        /// Instead, we use special ConstChunkGenerator that will generate chunks
        /// with max_block_size rows until total number of rows is reached.

        auto names_and_types = read_from_format_info.columns_description.getAllPhysical();
        ColumnsWithTypeAndName columns;
        for (const auto & [name, type] : names_and_types)
            columns.emplace_back(type->createColumn(), type, name);
        builder.init(Pipe(std::make_shared<ConstChunkGenerator>(
                              std::make_shared<const Block>(columns), *num_rows_from_cache, max_block_size)));
    }
    else
    {
        CompressionMethod compression_method;
        compression_method = chooseCompressionMethod(object_info->getFileName(), configuration->compression_method);
        read_buf = createReadBuffer(object_info->relative_path_with_metadata, object_storage, context_, log);

        Block initial_header = read_from_format_info.format_header;
        bool schema_changed = false;

        if (auto initial_schema = configuration->getInitialSchemaByPath(context_, object_info))
        {
            Block sample_header;
            for (const auto & [name, type] : *initial_schema)
            {
                sample_header.insert({type->createColumn(), type, name});
            }
            initial_header = sample_header;
            schema_changed = true;
        }
        auto filter_info = [&]()
        {
            if (!schema_changed)
                return format_filter_info;
            auto mapper = configuration->getColumnMapperForObject(object_info);
            if (!mapper)
                return format_filter_info;
            return std::make_shared<FormatFilterInfo>(format_filter_info->filter_actions_dag, format_filter_info->context.lock(), mapper, nullptr, nullptr);
        }();

        LOG_DEBUG(
            log,
            "Reading object '{}', size: {} bytes, with format: {}",
            object_info->getPath(),
            object_info->getObjectMetadata()->size_bytes,
            object_info->getFileFormat().value_or(configuration->format));

        auto input_format = FormatFactory::instance().getInput(
            object_info->getFileFormat().value_or(configuration->format),
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

        input_format->setBucketsToRead(object_info->file_bucket_info);
        input_format->setSerializationHints(read_from_format_info.serialization_hints);

        if (need_only_count)
            input_format->needOnlyCount();

        builder.init(Pipe(input_format));

        configuration->addDeleteTransformers(object_info, builder, format_settings, context_);

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
            if (auto schema_transformer = configuration->getSchemaTransformer(context_, object_info))
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

std::future<StorageObjectStorageSource::ReaderHolder> StorageObjectStorageSource::createReaderAsync()
{
    return create_reader_scheduler([=, this] { return createReader(); }, Priority{});
}

StorageObjectStorageSource::ReaderHolder::ReaderHolder(
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

StorageObjectStorageSource::ReaderHolder &
StorageObjectStorageSource::ReaderHolder::operator=(ReaderHolder && other) noexcept
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
