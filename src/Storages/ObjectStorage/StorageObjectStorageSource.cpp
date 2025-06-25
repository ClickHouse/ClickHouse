#include "StorageObjectStorageSource.h"
#include <memory>
#include <optional>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/parseGlobs.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Context.h>

#include <fmt/ranges.h>


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
    ConfigurationPtr configuration_,
    const ReadFromFormatInfo & info,
    const std::optional<FormatSettings> & format_settings_,
    ContextPtr context_,
    UInt64 max_block_size_,
    std::shared_ptr<IObjectIterator> file_iterator_,
    FormatParserGroupPtr parser_group_,
    bool need_only_count_)
    : ISource(info.source_header, false)
    , name(std::move(name_))
    , object_storage(object_storage_)
    , configuration(configuration_)
    , read_context(context_)
    , format_settings(format_settings_)
    , max_block_size(max_block_size_)
    , need_only_count(need_only_count_)
    , parser_group(std::move(parser_group_))
    , read_from_format_info(info)
    , create_reader_pool(std::make_shared<ThreadPool>(
        CurrentMetrics::StorageObjectStorageThreads,
        CurrentMetrics::StorageObjectStorageThreadsActive,
        CurrentMetrics::StorageObjectStorageThreadsScheduled,
        1 /* max_threads */))
    , file_iterator(file_iterator_)
    , schema_cache(StorageObjectStorage::getSchemaCache(context_, configuration->getTypeName()))
    , create_reader_scheduler(threadPoolCallbackRunnerUnsafe<ReaderHolder>(*create_reader_pool, "Reader"))
{
}

StorageObjectStorageSource::~StorageObjectStorageSource()
{
    create_reader_pool->wait();
}

std::string StorageObjectStorageSource::getUniqueStoragePathIdentifier(
    const Configuration & configuration,
    const ObjectInfo & object_info,
    bool include_connection_info)
{
    auto path = object_info.getPath();
    if (path.starts_with("/"))
        path = path.substr(1);

    if (include_connection_info)
        return fs::path(configuration.getDataSourceDescription()) / path;
    return fs::path(configuration.getNamespace()) / path;
}

std::shared_ptr<IObjectIterator> StorageObjectStorageSource::createFileIterator(
    ConfigurationPtr configuration,
    const StorageObjectStorage::QuerySettings & query_settings,
    ObjectStoragePtr object_storage,
    bool distributed_processing,
    const ContextPtr & local_context,
    const ActionsDAG::Node * predicate,
    const ActionsDAG * filter_actions_dag,
    const NamesAndTypesList & virtual_columns,
    ObjectInfos * read_keys,
    std::function<void(FileProgress)> file_progress_callback,
    bool ignore_archive_globs,
    bool skip_object_metadata)
{
    const bool is_archive = configuration->isArchive();

    if (distributed_processing)
    {
        auto distributed_iterator = std::make_unique<ReadTaskIterator>(
            local_context->getClusterFunctionReadTaskCallback(), local_context->getSettingsRef()[Setting::max_threads]);

        if (is_archive)
            return std::make_shared<ArchiveIterator>(object_storage, configuration, std::move(distributed_iterator), local_context, nullptr);

        return distributed_iterator;
    }

    if (configuration->isNamespaceWithGlobs())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Expression can not have wildcards inside {} name", configuration->getNamespaceType());

    std::unique_ptr<IObjectIterator> iterator;
    if (configuration->isPathWithGlobs())
    {
        auto path = configuration->getPath();
        if (hasExactlyOneBracketsExpansion(path))
        {
            auto paths = expandSelectionGlob(configuration->getPath());
            iterator = std::make_unique<KeysIterator>(
                paths, object_storage, virtual_columns, is_archive ? nullptr : read_keys,
                query_settings.ignore_non_existent_file, skip_object_metadata, file_progress_callback);
        }
        else
            /// Iterate through disclosed globs and make a source for each file
            iterator = std::make_unique<GlobIterator>(
                object_storage, configuration, predicate, virtual_columns,
                local_context, is_archive ? nullptr : read_keys, query_settings.list_object_keys_size,
                query_settings.throw_on_zero_files_match, file_progress_callback);
    }
    else if (configuration->supportsFileIterator())
    {
        return configuration->iterate(
            filter_actions_dag,
            file_progress_callback,
            query_settings.list_object_keys_size,
            local_context);
    }
    else
    {
        Strings paths;

        auto filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);
        if (filter_dag)
        {
            auto keys = configuration->getPaths();
            paths.reserve(keys.size());
            for (const auto & key : keys)
                paths.push_back(fs::path(configuration->getNamespace()) / key);

            VirtualColumnUtils::buildSetsForDAG(*filter_dag, local_context);
            auto actions = std::make_shared<ExpressionActions>(std::move(*filter_dag));
            VirtualColumnUtils::filterByPathOrFile(keys, paths, actions, virtual_columns, local_context);
            paths = keys;
        }
        else
        {
            paths = configuration->getPaths();
        }

        iterator = std::make_unique<KeysIterator>(
            paths, object_storage, virtual_columns, is_archive ? nullptr : read_keys,
            query_settings.ignore_non_existent_file, /*skip_object_metadata=*/false, file_progress_callback);
    }

    if (is_archive)
    {
        return std::make_shared<ArchiveIterator>(object_storage, configuration, std::move(iterator), local_context, read_keys, ignore_archive_globs);
    }

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

            if (!full_path.starts_with(configuration->getPath()))
                full_path = fs::path(configuration->getPath()) / object_info->getPath();

            chassert(object_info->metadata);

            VirtualColumnUtils::addRequestedFileLikeStorageVirtualsToChunk(
                chunk,
                read_from_format_info.requested_virtual_columns,
                {.path = getUniqueStoragePathIdentifier(*configuration, *object_info, false),
                 .size = object_info->isArchive() ? object_info->fileSizeInArchive() : object_info->metadata->size_bytes,
                 .filename = &filename,
                 .last_modified = object_info->metadata->last_modified,
                 .etag = &(object_info->metadata->etag)},
                read_context);

#if USE_PARQUET && USE_AWS_S3
            if (chunk_size && chunk.hasColumns())
            {
                /// Old delta lake code which needs to be deprecated in favour of DeltaLakeMetadataDeltaKernel.
                if (dynamic_cast<const DeltaLakeMetadata *>(configuration.get()))
                {
                    /// This is an awful temporary crutch,
                    /// which will be removed once DeltaKernel is used by default for DeltaLake.
                    /// (Because it does not make sense to support it in a nice way
                    /// because the code will be removed ASAP anyway)
                    if (configuration->isDataLakeConfiguration())
                    {
                        /// A terrible crutch, but it this code will be removed next month.
                        DeltaLakePartitionColumns partition_columns;
                        if (auto * delta_conf_s3 = dynamic_cast<StorageS3DeltaLakeConfiguration *>(configuration.get()))
                        {
                            partition_columns = delta_conf_s3->getDeltaLakePartitionColumns();
                        }
                        else if (auto * delta_conf_local = dynamic_cast<StorageLocalDeltaLakeConfiguration *>(configuration.get()))
                        {
                            partition_columns = delta_conf_local->getDeltaLakePartitionColumns();
                        }
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
            }
#endif

            return chunk;
        }

        if (reader.getInputFormat() && read_context->getSettingsRef()[Setting::use_cache_for_count_from_files]
            && !parser_group->filter_actions_dag)
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
        getUniqueStoragePathIdentifier(*configuration, object_info), configuration->format, format_settings, read_context);
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
        parser_group,
        need_only_count);
}

StorageObjectStorageSource::ReaderHolder StorageObjectStorageSource::createReader(
    size_t processor,
    const std::shared_ptr<IObjectIterator> & file_iterator,
    const ConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage,
    ReadFromFormatInfo & read_from_format_info,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context_,
    SchemaCache * schema_cache,
    const LoggerPtr & log,
    size_t max_block_size,
    FormatParserGroupPtr parser_group,
    bool need_only_count)
{
    ObjectInfoPtr object_info;
    auto query_settings = configuration->getQuerySettings(context_);

    do
    {
        object_info = file_iterator->next(processor);

        if (!object_info || object_info->getPath().empty())
            return {};

        if (!object_info->metadata)
        {
            const auto & path = object_info->isArchive() ? object_info->getPathToArchive() : object_info->getPath();

            if (query_settings.ignore_non_existent_file)
            {
                auto metadata = object_storage->tryGetObjectMetadata(path);
                if (!metadata)
                    return {};

                object_info->metadata = metadata;
            }
            else
                object_info->metadata = object_storage->getObjectMetadata(path);
        }
    }
    while (query_settings.skip_empty_files && object_info->metadata->size_bytes == 0);

    QueryPipelineBuilder builder;
    std::shared_ptr<ISource> source;
    std::unique_ptr<ReadBuffer> read_buf;

    auto try_get_num_rows_from_cache = [&]() -> std::optional<size_t>
    {
        if (!schema_cache)
            return std::nullopt;

        const auto cache_key = getKeyForSchemaCache(
            getUniqueStoragePathIdentifier(*configuration, *object_info),
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
        = need_only_count && context_->getSettingsRef()[Setting::use_cache_for_count_from_files] ? try_get_num_rows_from_cache() : std::nullopt;

    if (num_rows_from_cache)
    {
        /// We should not return single chunk with all number of rows,
        /// because there is a chance that this chunk will be materialized later
        /// (it can cause memory problems even with default values in columns or when virtual columns are requested).
        /// Instead, we use special ConstChunkGenerator that will generate chunks
        /// with max_block_size rows until total number of rows is reached.
        builder.init(Pipe(std::make_shared<ConstChunkGenerator>(
                              read_from_format_info.format_header, *num_rows_from_cache, max_block_size)));
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
            parser_group,
            true /* is_remote_fs */,
            compression_method,
            need_only_count);

        input_format->setSerializationHints(read_from_format_info.serialization_hints);

        if (need_only_count)
            input_format->needOnlyCount();

        builder.init(Pipe(input_format));

        std::shared_ptr<const ActionsDAG> transformer;
        if (object_info->data_lake_metadata)
            transformer = object_info->data_lake_metadata->transform;
        if (!transformer)
            transformer = configuration->getSchemaTransformer(context_, object_info->getPath());

        if (transformer)
        {
            auto schema_modifying_actions = std::make_shared<ExpressionActions>(transformer->clone());
            builder.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, schema_modifying_actions);
            });
        }

        if (read_from_format_info.columns_description.hasDefaults())
        {
            builder.addSimpleTransform(
                [&](const Block & header)
                {
                    return std::make_shared<AddingDefaultsTransform>(header, read_from_format_info.columns_description, *input_format, context_);
                });
        }

        source = input_format;
    }

    /// Add ExtractColumnsTransform to extract requested columns/subcolumns
    /// from chunk read by IInputFormat.
    builder.addSimpleTransform([&](const Block & header)
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

std::unique_ptr<ReadBufferFromFileBase> StorageObjectStorageSource::createReadBuffer(
    ObjectInfo & object_info, const ObjectStoragePtr & object_storage, const ContextPtr & context_, const LoggerPtr & log, const std::optional<ReadSettings> & read_settings)
{
    const auto & settings = context_->getSettingsRef();
    const auto & effective_read_settings = read_settings.has_value() ? read_settings.value() : context_->getReadSettings();

    const auto filesystem_cache_name = settings[Setting::filesystem_cache_name].value;
    bool use_cache = effective_read_settings.enable_filesystem_cache
        && !filesystem_cache_name.empty()
        && (object_storage->getType() == ObjectStorageType::Azure
            || object_storage->getType() == ObjectStorageType::S3);

    if (!object_info.metadata)
    {
        if (!use_cache)
            return object_storage->readObject(StoredObject(object_info.getPath()), effective_read_settings);

        object_info.metadata = object_storage->getObjectMetadata(object_info.getPath());
    }

    const auto & object_size = object_info.metadata->size_bytes;

    auto modified_read_settings = effective_read_settings.adjustBufferSize(object_size);
    /// FIXME: Changing this setting to default value breaks something around parquet reading
    modified_read_settings.remote_read_min_bytes_for_seek = modified_read_settings.remote_fs_buffer_size;
    /// User's object may change, don't cache it.
    modified_read_settings.use_page_cache_for_disks_without_file_cache = false;

    // Create a read buffer that will prefetch the first ~1 MB of the file.
    // When reading lots of tiny files, this prefetching almost doubles the throughput.
    // For bigger files, parallel reading is more useful.
    const bool object_too_small = object_size <= 2 * context_->getSettingsRef()[Setting::max_download_buffer_size];
    const bool use_prefetch = object_too_small
        && modified_read_settings.remote_fs_method == RemoteFSReadMethod::threadpool
        && modified_read_settings.remote_fs_prefetch;

    bool use_async_buffer = false;
    ReadSettings nested_buffer_read_settings;
    if (use_prefetch || use_cache)
    {
        nested_buffer_read_settings.remote_read_buffer_use_external_buffer = true;

        /// FIXME: Use async buffer if use_cache,
        /// because CachedOnDiskReadBufferFromFile does not work as an independent buffer currently.
        use_async_buffer = true;
    }

    std::unique_ptr<ReadBufferFromFileBase> impl;
    if (use_cache)
    {
        chassert(object_info.metadata.has_value());
        if (object_info.metadata->etag.empty())
        {
            LOG_WARNING(log, "Cannot use filesystem cache, no etag specified");
        }
        else
        {
            SipHash hash;
            hash.update(object_info.getPath());
            hash.update(object_info.metadata->etag);

            const auto cache_key = FileCacheKey::fromKey(hash.get128());
            auto cache = FileCacheFactory::instance().get(filesystem_cache_name);

            auto read_buffer_creator = [path = object_info.getPath(), object_size, nested_buffer_read_settings, object_storage]()
            {
                return object_storage->readObject(StoredObject(path, "", object_size), nested_buffer_read_settings);
            };

            modified_read_settings.filesystem_cache_boundary_alignment = settings[Setting::filesystem_cache_boundary_alignment];

            impl = std::make_unique<CachedOnDiskReadBufferFromFile>(
                object_info.getPath(),
                cache_key,
                cache,
                FileCache::getCommonUser(),
                read_buffer_creator,
                use_async_buffer ? nested_buffer_read_settings : effective_read_settings,
                std::string(CurrentThread::getQueryId()),
                object_size,
                /* allow_seeks */true,
                /* use_external_buffer */use_async_buffer,
                /* read_until_position */std::nullopt,
                context_->getFilesystemCacheLog());

            LOG_TEST(
                log,
                "Using filesystem cache `{}` (path: {}, etag: {}, hash: {})",
                filesystem_cache_name,
                object_info.getPath(),
                object_info.metadata->etag,
                toString(hash.get128()));
        }
    }

    if (!impl)
        impl = object_storage->readObject(StoredObject(object_info.getPath(), "", object_size), nested_buffer_read_settings);

    if (!use_async_buffer)
        return impl;

    LOG_TRACE(log, "Downloading object of size {} with initial prefetch", object_size);

    bool prefer_bigger_buffer_size = effective_read_settings.filesystem_cache_prefer_bigger_buffer_size && impl->isCached();
    size_t buffer_size = prefer_bigger_buffer_size
        ? std::max<size_t>(effective_read_settings.remote_fs_buffer_size, DBMS_DEFAULT_BUFFER_SIZE)
        : effective_read_settings.remote_fs_buffer_size;
    if (object_size)
        buffer_size = std::min<size_t>(object_size, buffer_size);

    auto & reader = context_->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
    impl = std::make_unique<AsynchronousBoundedReadBuffer>(
        std::move(impl),
        reader,
        modified_read_settings,
        buffer_size,
        modified_read_settings.remote_read_min_bytes_for_seek,
        context_->getAsyncReadCounters(),
        context_->getFilesystemReadPrefetchesLog());

    if (use_prefetch)
    {
        impl->setReadUntilEnd();
        impl->prefetch(DEFAULT_PREFETCH_PRIORITY);
    }

    return impl;
}

StorageObjectStorageSource::GlobIterator::GlobIterator(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns_,
    ContextPtr context_,
    ObjectInfos * read_keys_,
    size_t list_object_keys_size,
    bool throw_on_zero_files_match_,
    std::function<void(FileProgress)> file_progress_callback_)
    : WithContext(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , virtual_columns(virtual_columns_)
    , throw_on_zero_files_match(throw_on_zero_files_match_)
    , log(getLogger("GlobIterator"))
    , read_keys(read_keys_)
    , local_context(context_)
    , file_progress_callback(file_progress_callback_)
{
    if (configuration->isNamespaceWithGlobs())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression can not have wildcards inside namespace name");
    }
    if (configuration->isPathWithGlobs())
    {
        const auto key_with_globs = configuration_->getPath();
        const auto key_prefix = configuration->getPathWithoutGlobs();

        object_storage_iterator = object_storage->iterate(key_prefix, list_object_keys_size);

        matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(key_with_globs));
        if (!matcher->ok())
        {
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP, "Cannot compile regex from glob ({}): {}", key_with_globs, matcher->error());
        }

        recursive = key_with_globs == "/**";
        if (auto filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns))
        {
            VirtualColumnUtils::buildSetsForDAG(*filter_dag, getContext());
            filter_expr = std::make_shared<ExpressionActions>(std::move(*filter_dag));
        }
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Using glob iterator with path without globs is not allowed (used path: {})",
            configuration->getPath());
    }
}

size_t StorageObjectStorageSource::GlobIterator::estimatedKeysCount()
{
    if (object_infos.empty() && !is_finished && object_storage_iterator->isValid())
    {
        /// 1000 files were listed, and we cannot make any estimation of _how many more_ there are (because we list bucket lazily);
        /// If there are more objects in the bucket, limiting the number of streams is the last thing we may want to do
        /// as it would lead to serious slow down of the execution, since objects are going
        /// to be fetched sequentially rather than in-parallel with up to <max_threads> times.
        return std::numeric_limits<size_t>::max();
    }
    return object_infos.size();
}

StorageObjectStorage::ObjectInfoPtr StorageObjectStorageSource::GlobIterator::next(size_t processor)
{
    std::lock_guard lock(next_mutex);
    auto object_info = nextUnlocked(processor);
    if (first_iteration && !object_info && throw_on_zero_files_match)
    {
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
                        "Can not match any files with path {}",
                        configuration->getPath());
    }
    first_iteration = false;
    return object_info;
}

StorageObjectStorage::ObjectInfoPtr StorageObjectStorageSource::GlobIterator::nextUnlocked(size_t /* processor */)
{
    bool current_batch_processed = object_infos.empty() || index >= object_infos.size();
    if (is_finished && current_batch_processed)
        return {};

    if (current_batch_processed)
    {
        ObjectInfos new_batch;
        while (new_batch.empty())
        {
            auto result = object_storage_iterator->getCurrentBatchAndScheduleNext();
            if (!result.has_value())
            {
                is_finished = true;
                return {};
            }

            new_batch = std::move(result.value());
            for (auto it = new_batch.begin(); it != new_batch.end();)
            {
                if (!recursive && !re2::RE2::FullMatch((*it)->getPath(), *matcher))
                    it = new_batch.erase(it);
                else
                    ++it;
            }

            if (filter_expr)
            {
                std::vector<String> paths;
                paths.reserve(new_batch.size());
                for (const auto & object_info : new_batch)
                    paths.push_back(getUniqueStoragePathIdentifier(*configuration, *object_info, false));

                VirtualColumnUtils::filterByPathOrFile(new_batch, paths, filter_expr, virtual_columns, local_context);

                LOG_TEST(log, "Filtered files: {} -> {}", paths.size(), new_batch.size());
            }
        }

        index = 0;

        if (read_keys)
            read_keys->insert(read_keys->end(), new_batch.begin(), new_batch.end());

        object_infos = std::move(new_batch);

        if (file_progress_callback)
        {
            for (const auto & object_info : object_infos)
            {
                chassert(object_info->metadata);
                file_progress_callback(FileProgress(0, object_info->metadata->size_bytes));
            }
        }
    }

    if (index >= object_infos.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Index out of bound for blob metadata. Index: {}, size: {}",
            index, object_infos.size());
    }

    return object_infos[index++];
}

StorageObjectStorageSource::KeysIterator::KeysIterator(
    const Strings & keys_,
    ObjectStoragePtr object_storage_,
    const NamesAndTypesList & virtual_columns_,
    ObjectInfos * read_keys_,
    bool ignore_non_existent_files_,
    bool skip_object_metadata_,
    std::function<void(FileProgress)> file_progress_callback_)
    : object_storage(object_storage_)
    , virtual_columns(virtual_columns_)
    , file_progress_callback(file_progress_callback_)
    , keys(keys_)
    , ignore_non_existent_files(ignore_non_existent_files_)
    , skip_object_metadata(skip_object_metadata_)
{
    if (read_keys_)
    {
        /// TODO: should we add metadata if we anyway fetch it if file_progress_callback is passed?
        for (auto && key : keys)
        {
            auto object_info = std::make_shared<ObjectInfo>(key);
            read_keys_->emplace_back(object_info);
        }
    }
}

StorageObjectStorage::ObjectInfoPtr StorageObjectStorageSource::KeysIterator::next(size_t /* processor */)
{
    while (true)
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= keys.size())
            return nullptr;

        auto key = keys[current_index];

        ObjectMetadata object_metadata{};
        if (!skip_object_metadata)
        {
            if (ignore_non_existent_files)
            {
                auto metadata = object_storage->tryGetObjectMetadata(key);
                if (!metadata)
                    continue;
                object_metadata = *metadata;
            }
            else
                object_metadata = object_storage->getObjectMetadata(key);
        }

        if (file_progress_callback)
            file_progress_callback(FileProgress(0, object_metadata.size_bytes));

        return std::make_shared<ObjectInfo>(key, object_metadata);
    }
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

StorageObjectStorageSource::ReadTaskIterator::ReadTaskIterator(
    const ClusterFunctionReadTaskCallback & callback_, size_t max_threads_count)
    : callback(callback_)
{
    ThreadPool pool(
        CurrentMetrics::StorageObjectStorageThreads,
        CurrentMetrics::StorageObjectStorageThreadsActive,
        CurrentMetrics::StorageObjectStorageThreadsScheduled, max_threads_count);

    auto pool_scheduler = threadPoolCallbackRunnerUnsafe<ObjectInfoPtr>(pool, "ReadTaskIter");

    std::vector<std::future<ObjectInfoPtr>> objects;
    objects.reserve(max_threads_count);
    for (size_t i = 0; i < max_threads_count; ++i)
        objects.push_back(pool_scheduler([this]() -> ObjectInfoPtr
        {
            auto task = callback();
            if (!task || task->isEmpty())
                return nullptr;
            return task->getObjectInfo();
        }, Priority{}));

    pool.wait();
    buffer.reserve(max_threads_count);
    for (auto & object_future : objects)
    {
        auto object = object_future.get();
        if (object)
            buffer.push_back(object);
    }
}

StorageObjectStorage::ObjectInfoPtr StorageObjectStorageSource::ReadTaskIterator::next(size_t)
{
    size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
    if (current_index >= buffer.size())
    {
        auto task = callback();
        if (!task || task->isEmpty())
            return nullptr;
        return task->getObjectInfo();
    }

    return buffer[current_index];
}

static IArchiveReader::NameFilter createArchivePathFilter(const std::string & archive_pattern)
{
    auto matcher = std::make_shared<re2::RE2>(makeRegexpPatternFromGlobs(archive_pattern));
    if (!matcher->ok())
    {
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                        "Cannot compile regex from glob ({}): {}",
                        archive_pattern, matcher->error());
    }
    return [matcher](const std::string & p) mutable { return re2::RE2::FullMatch(p, *matcher); };
}

StorageObjectStorageSource::ArchiveIterator::ObjectInfoInArchive::ObjectInfoInArchive(
    ObjectInfoPtr archive_object_,
    const std::string & path_in_archive_,
    std::shared_ptr<IArchiveReader> archive_reader_,
    IArchiveReader::FileInfo && file_info_)
    : archive_object(archive_object_), path_in_archive(path_in_archive_), archive_reader(archive_reader_), file_info(file_info_)
{
}

StorageObjectStorageSource::ArchiveIterator::ArchiveIterator(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    std::unique_ptr<IObjectIterator> archives_iterator_,
    ContextPtr context_,
    ObjectInfos * read_keys_,
    bool ignore_archive_globs_)
    : WithContext(context_)
    , object_storage(object_storage_)
    , is_path_in_archive_with_globs(configuration_->isPathInArchiveWithGlobs())
    , archives_iterator(std::move(archives_iterator_))
    , filter(is_path_in_archive_with_globs ? createArchivePathFilter(configuration_->getPathInArchive()) : IArchiveReader::NameFilter{})
    , log(getLogger("ArchiveIterator"))
    , path_in_archive(is_path_in_archive_with_globs ? "" : configuration_->getPathInArchive())
    , read_keys(read_keys_)
    , ignore_archive_globs(ignore_archive_globs_)
{
}

std::shared_ptr<IArchiveReader>
StorageObjectStorageSource::ArchiveIterator::createArchiveReader(ObjectInfoPtr object_info) const
{
    const auto size = object_info->metadata->size_bytes;
    return DB::createArchiveReader(
        /* path_to_archive */object_info->getPath(),
        /* archive_read_function */[=, this]()
        {
            return StorageObjectStorageSource::createReadBuffer(*object_info, object_storage, getContext(), log);
        },
        /* archive_size */size);
}

ObjectInfoPtr StorageObjectStorageSource::ArchiveIterator::next(size_t processor)
{
    std::unique_lock lock{next_mutex};
    IArchiveReader::FileInfo current_file_info{};
    while (true)
    {
        if (filter)
        {
            if (!file_enumerator)
            {
                archive_object = archives_iterator->next(processor);
                if (!archive_object)
                    return {};

                if (!archive_object->metadata)
                    archive_object->metadata = object_storage->getObjectMetadata(archive_object->getPath());

                archive_reader = createArchiveReader(archive_object);
                file_enumerator = archive_reader->firstFile();
                if (!file_enumerator)
                    continue;
            }
            else if (!file_enumerator->nextFile() || ignore_archive_globs)
            {
                file_enumerator.reset();
                continue;
            }

            path_in_archive = file_enumerator->getFileName();
            if (!filter(path_in_archive))
                continue;
            current_file_info = file_enumerator->getFileInfo();
        }
        else
        {
            archive_object = archives_iterator->next(processor);
            if (!archive_object)
                return {};

            if (!archive_object->metadata)
                archive_object->metadata = object_storage->getObjectMetadata(archive_object->getPath());

            archive_reader = createArchiveReader(archive_object);
            if (!archive_reader->fileExists(path_in_archive))
                continue;
            current_file_info = archive_reader->getFileInfo(path_in_archive);
        }
        break;
    }

    auto object_in_archive
        = std::make_shared<ObjectInfoInArchive>(archive_object, path_in_archive, archive_reader, std::move(current_file_info));

    if (read_keys != nullptr)
        read_keys->push_back(object_in_archive);

    return object_in_archive;
}

size_t StorageObjectStorageSource::ArchiveIterator::estimatedKeysCount()
{
    return archives_iterator->estimatedKeysCount();
}

}
