#include "StorageObjectStorageSource.h"
#include <Storages/VirtualColumnUtils.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/Archives/createArchiveReader.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/Cache/SchemaCache.h>
#include <Common/parseGlobs.h>
#include <Core/Settings.h>

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
    std::shared_ptr<IIterator> file_iterator_,
    size_t max_parsing_threads_,
    bool need_only_count_)
    : SourceWithKeyCondition(info.source_header, false)
    , WithContext(context_)
    , name(std::move(name_))
    , object_storage(object_storage_)
    , configuration(configuration_)
    , format_settings(format_settings_)
    , max_block_size(max_block_size_)
    , need_only_count(need_only_count_)
    , max_parsing_threads(max_parsing_threads_)
    , read_from_format_info(info)
    , create_reader_pool(std::make_shared<ThreadPool>(
        CurrentMetrics::StorageObjectStorageThreads,
        CurrentMetrics::StorageObjectStorageThreadsActive,
        CurrentMetrics::StorageObjectStorageThreadsScheduled,
        1/* max_threads */))
    , file_iterator(file_iterator_)
    , schema_cache(StorageObjectStorage::getSchemaCache(context_, configuration->getTypeName()))
    , create_reader_scheduler(threadPoolCallbackRunnerUnsafe<ReaderHolder>(*create_reader_pool, "Reader"))
{
}

StorageObjectStorageSource::~StorageObjectStorageSource()
{
    create_reader_pool->wait();
}

void StorageObjectStorageSource::setKeyCondition(const std::optional<ActionsDAG> & filter_actions_dag, ContextPtr context_)
{
    setKeyConditionImpl(filter_actions_dag, context_, read_from_format_info.format_header);
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
    else
        return fs::path(configuration.getNamespace()) / path;
}

std::shared_ptr<StorageObjectStorageSource::IIterator> StorageObjectStorageSource::createFileIterator(
    ConfigurationPtr configuration,
    ObjectStoragePtr object_storage,
    bool distributed_processing,
    const ContextPtr & local_context,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns,
    ObjectInfos * read_keys,
    std::function<void(FileProgress)> file_progress_callback)
{
    if (distributed_processing)
        return std::make_shared<ReadTaskIterator>(
            local_context->getReadTaskCallback(),
            local_context->getSettingsRef().max_threads);

    if (configuration->isNamespaceWithGlobs())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Expression can not have wildcards inside {} name", configuration->getNamespaceType());

    auto settings = configuration->getQuerySettings(local_context);
    const bool is_archive = configuration->isArchive();

    std::unique_ptr<IIterator> iterator;
    if (configuration->isPathWithGlobs())
    {
        /// Iterate through disclosed globs and make a source for each file
        iterator = std::make_unique<GlobIterator>(
            object_storage, configuration, predicate, virtual_columns,
            local_context, is_archive ? nullptr : read_keys, settings.list_object_keys_size,
            settings.throw_on_zero_files_match, file_progress_callback);
    }
    else
    {
        ConfigurationPtr copy_configuration = configuration->clone();
        auto filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);
        if (filter_dag)
        {
            auto keys = configuration->getPaths();
            std::vector<String> paths;
            paths.reserve(keys.size());
            for (const auto & key : keys)
                paths.push_back(fs::path(configuration->getNamespace()) / key);

            VirtualColumnUtils::buildSetsForDAG(*filter_dag, local_context);
            auto actions = std::make_shared<ExpressionActions>(std::move(*filter_dag));
            VirtualColumnUtils::filterByPathOrFile(keys, paths, actions, virtual_columns);
            copy_configuration->setPaths(keys);
        }

        iterator = std::make_unique<KeysIterator>(
            object_storage, copy_configuration, virtual_columns, is_archive ? nullptr : read_keys,
            settings.ignore_non_existent_file, file_progress_callback);
    }

    if (is_archive)
    {
        return std::make_shared<ArchiveIterator>(object_storage, configuration, std::move(iterator), local_context, read_keys);
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
            chassert(object_info->metadata);
            VirtualColumnUtils::addRequestedFileLikeStorageVirtualsToChunk(
                chunk,
                read_from_format_info.requested_virtual_columns,
                {.path = getUniqueStoragePathIdentifier(*configuration, *object_info, false),
                 .size = object_info->isArchive() ? object_info->fileSizeInArchive() : object_info->metadata->size_bytes,
                 .filename = &filename,
                 .last_modified = object_info->metadata->last_modified,
                 .etag = &(object_info->metadata->etag)
                 });

            const auto & partition_columns = configuration->getPartitionColumns();
            if (!partition_columns.empty() && chunk_size && chunk.hasColumns())
            {
                auto partition_values = partition_columns.find(filename);
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
            return chunk;
        }

        if (reader.getInputFormat() && getContext()->getSettingsRef().use_cache_for_count_from_files)
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
        configuration->format,
        format_settings,
        getContext());
    schema_cache.addNumRows(cache_key, num_rows);
}

StorageObjectStorageSource::ReaderHolder StorageObjectStorageSource::createReader()
{
    return createReader(
        0, file_iterator, configuration, object_storage, read_from_format_info, format_settings,
        key_condition, getContext(), &schema_cache, log, max_block_size, max_parsing_threads, need_only_count);
}

StorageObjectStorageSource::ReaderHolder StorageObjectStorageSource::createReader(
    size_t processor,
    const std::shared_ptr<IIterator> & file_iterator,
    const ConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage,
    const ReadFromFormatInfo & read_from_format_info,
    const std::optional<FormatSettings> & format_settings,
    const std::shared_ptr<const KeyCondition> & key_condition_,
    const ContextPtr & context_,
    SchemaCache * schema_cache,
    const LoggerPtr & log,
    size_t max_block_size,
    size_t max_parsing_threads,
    bool need_only_count)
{
    ObjectInfoPtr object_info;
    auto query_settings = configuration->getQuerySettings(context_);

    do
    {
        object_info = file_iterator->next(processor);

        if (!object_info || object_info->getFileName().empty())
            return {};

        if (!object_info->metadata)
        {
            const auto & path = object_info->isArchive() ? object_info->getPathToArchive() : object_info->getPath();
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

    std::optional<size_t> num_rows_from_cache = need_only_count
        && context_->getSettingsRef().use_cache_for_count_from_files
        ? try_get_num_rows_from_cache()
        : std::nullopt;

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

        auto input_format = FormatFactory::instance().getInput(
            configuration->format,
            *read_buf,
            read_from_format_info.format_header,
            context_,
            max_block_size,
            format_settings,
            need_only_count ? 1 : max_parsing_threads,
            std::nullopt,
            true/* is_remote_fs */,
            compression_method,
            need_only_count);

        if (key_condition_)
            input_format->setKeyCondition(key_condition_);

        if (need_only_count)
            input_format->needOnlyCount();

        builder.init(Pipe(input_format));

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

std::unique_ptr<ReadBuffer> StorageObjectStorageSource::createReadBuffer(
    const ObjectInfo & object_info,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context_,
    const LoggerPtr & log)
{
    const auto & object_size = object_info.metadata->size_bytes;

    auto read_settings = context_->getReadSettings().adjustBufferSize(object_size);
    read_settings.enable_filesystem_cache = false;
    /// FIXME: Changing this setting to default value breaks something around parquet reading
    read_settings.remote_read_min_bytes_for_seek = read_settings.remote_fs_buffer_size;

    const bool object_too_small = object_size <= 2 * context_->getSettingsRef().max_download_buffer_size;
    const bool use_prefetch = object_too_small && read_settings.remote_fs_method == RemoteFSReadMethod::threadpool;
    read_settings.remote_fs_method = use_prefetch ? RemoteFSReadMethod::threadpool : RemoteFSReadMethod::read;
    /// User's object may change, don't cache it.
    read_settings.use_page_cache_for_disks_without_file_cache = false;

    // Create a read buffer that will prefetch the first ~1 MB of the file.
    // When reading lots of tiny files, this prefetching almost doubles the throughput.
    // For bigger files, parallel reading is more useful.
    if (use_prefetch)
    {
        LOG_TRACE(log, "Downloading object of size {} with initial prefetch", object_size);

        auto async_reader = object_storage->readObjects(
            StoredObjects{StoredObject{object_info.getPath(), /* local_path */ "", object_size}}, read_settings);

        async_reader->setReadUntilEnd();
        if (read_settings.remote_fs_prefetch)
            async_reader->prefetch(DEFAULT_PREFETCH_PRIORITY);

        return async_reader;
    }
    else
    {
        /// FIXME: this is inconsistent that readObject always reads synchronously ignoring read_method setting.
        return object_storage->readObject(StoredObject(object_info.getPath(), "", object_size), read_settings);
    }
}

StorageObjectStorageSource::IIterator::IIterator(const std::string & logger_name_)
    : logger(getLogger(logger_name_))
{
}

StorageObjectStorage::ObjectInfoPtr StorageObjectStorageSource::IIterator::next(size_t processor)
{
    auto object_info = nextImpl(processor);

    if (object_info)
    {
        LOG_TEST(logger, "Next key: {}", object_info->getFileName());
    }

    return object_info;
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
    : IIterator("GlobIterator")
    , WithContext(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , virtual_columns(virtual_columns_)
    , throw_on_zero_files_match(throw_on_zero_files_match_)
    , read_keys(read_keys_)
    , file_progress_callback(file_progress_callback_)
{
    if (configuration->isNamespaceWithGlobs())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression can not have wildcards inside namespace name");
    }
    else if (configuration->isPathWithGlobs())
    {
        const auto key_with_globs = configuration_->getPath();
        const auto key_prefix = configuration->getPathWithoutGlobs();
        object_storage_iterator = object_storage->iterate(key_prefix, list_object_keys_size);

        matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(key_with_globs));
        if (!matcher->ok())
        {
            throw Exception(
                ErrorCodes::CANNOT_COMPILE_REGEXP,
                "Cannot compile regex from glob ({}): {}", key_with_globs, matcher->error());
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
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

StorageObjectStorage::ObjectInfoPtr StorageObjectStorageSource::GlobIterator::nextImpl(size_t processor)
{
    std::lock_guard lock(next_mutex);
    auto object_info = nextImplUnlocked(processor);
    if (first_iteration && !object_info && throw_on_zero_files_match)
    {
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
                        "Can not match any files with path {}",
                        configuration->getPath());
    }
    first_iteration = false;
    return object_info;
}

StorageObjectStorage::ObjectInfoPtr StorageObjectStorageSource::GlobIterator::nextImplUnlocked(size_t /* processor */)
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

                VirtualColumnUtils::filterByPathOrFile(new_batch, paths, filter_expr, virtual_columns);

                LOG_TEST(logger, "Filtered files: {} -> {}", paths.size(), new_batch.size());
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
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    const NamesAndTypesList & virtual_columns_,
    ObjectInfos * read_keys_,
    bool ignore_non_existent_files_,
    std::function<void(FileProgress)> file_progress_callback_)
    : IIterator("KeysIterator")
    , object_storage(object_storage_)
    , configuration(configuration_)
    , virtual_columns(virtual_columns_)
    , file_progress_callback(file_progress_callback_)
    , keys(configuration->getPaths())
    , ignore_non_existent_files(ignore_non_existent_files_)
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

StorageObjectStorage::ObjectInfoPtr StorageObjectStorageSource::KeysIterator::nextImpl(size_t /* processor */)
{
    while (true)
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= keys.size())
            return {};

        auto key = keys[current_index];

        ObjectMetadata object_metadata{};
        if (ignore_non_existent_files)
        {
            auto metadata = object_storage->tryGetObjectMetadata(key);
            if (!metadata)
                continue;
        }
        else
            object_metadata = object_storage->getObjectMetadata(key);

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
    const ReadTaskCallback & callback_, size_t max_threads_count)
    : IIterator("ReadTaskIterator")
    , callback(callback_)
{
    ThreadPool pool(
        CurrentMetrics::StorageObjectStorageThreads,
        CurrentMetrics::StorageObjectStorageThreadsActive,
        CurrentMetrics::StorageObjectStorageThreadsScheduled, max_threads_count);

    auto pool_scheduler = threadPoolCallbackRunnerUnsafe<String>(pool, "ReadTaskIter");

    std::vector<std::future<String>> keys;
    keys.reserve(max_threads_count);
    for (size_t i = 0; i < max_threads_count; ++i)
        keys.push_back(pool_scheduler([this] { return callback(); }, Priority{}));

    pool.wait();
    buffer.reserve(max_threads_count);
    for (auto & key_future : keys)
    {
        auto key = key_future.get();
        if (!key.empty())
            buffer.emplace_back(std::make_shared<ObjectInfo>(key, std::nullopt));
    }
}

StorageObjectStorage::ObjectInfoPtr StorageObjectStorageSource::ReadTaskIterator::nextImpl(size_t)
{
    size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
    if (current_index >= buffer.size())
        return std::make_shared<ObjectInfo>(callback());

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
    std::unique_ptr<IIterator> archives_iterator_,
    ContextPtr context_,
    ObjectInfos * read_keys_)
    : IIterator("ArchiveIterator")
    , WithContext(context_)
    , object_storage(object_storage_)
    , is_path_in_archive_with_globs(configuration_->isPathInArchiveWithGlobs())
    , archives_iterator(std::move(archives_iterator_))
    , filter(is_path_in_archive_with_globs ? createArchivePathFilter(configuration_->getPathInArchive()) : IArchiveReader::NameFilter{})
    , path_in_archive(is_path_in_archive_with_globs ? "" : configuration_->getPathInArchive())
    , read_keys(read_keys_)
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
            StoredObject stored_object(object_info->getPath(), "", size);
            return object_storage->readObject(stored_object, getContext()->getReadSettings());
        },
        /* archive_size */size);
}

StorageObjectStorageSource::ObjectInfoPtr
StorageObjectStorageSource::ArchiveIterator::nextImpl(size_t processor)
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

                archive_reader = createArchiveReader(archive_object);
                file_enumerator = archive_reader->firstFile();
                if (!file_enumerator)
                    continue;
            }
            else if (!file_enumerator->nextFile())
            {
                file_enumerator.reset();
                continue;
            }

            path_in_archive = file_enumerator->getFileName();
            if (!filter(path_in_archive))
                continue;
            else
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
            else
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
