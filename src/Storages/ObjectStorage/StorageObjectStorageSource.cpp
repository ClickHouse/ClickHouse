#include "StorageObjectStorageSource.h"
#include <Storages/VirtualColumnUtils.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorageQuerySettings.h>
#include <Storages/Cache/SchemaCache.h>
#include <Common/parseGlobs.h>


namespace ProfileEvents
{
    extern const Event EngineFileLikeReadFiles;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

StorageObjectStorageSource::StorageObjectStorageSource(
    String name_,
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    const ReadFromFormatInfo & info,
    std::optional<FormatSettings> format_settings_,
    const StorageObjectStorageSettings & query_settings_,
    ContextPtr context_,
    UInt64 max_block_size_,
    std::shared_ptr<IIterator> file_iterator_,
    bool need_only_count_,
    SchemaCache & schema_cache_,
    std::shared_ptr<ThreadPool> reader_pool_,
    CurrentMetrics::Metric metric_threads_,
    CurrentMetrics::Metric metric_threads_active_,
    CurrentMetrics::Metric metric_threads_scheduled_)
    : SourceWithKeyCondition(info.source_header, false)
    , WithContext(context_)
    , name(std::move(name_))
    , object_storage(object_storage_)
    , configuration(configuration_)
    , format_settings(format_settings_)
    , query_settings(query_settings_)
    , max_block_size(max_block_size_)
    , need_only_count(need_only_count_)
    , read_from_format_info(info)
    , create_reader_pool(reader_pool_)
    , columns_desc(info.columns_description)
    , file_iterator(file_iterator_)
    , schema_cache(schema_cache_)
    , metric_threads(metric_threads_)
    , metric_threads_active(metric_threads_active_)
    , metric_threads_scheduled(metric_threads_scheduled_)
    , create_reader_scheduler(threadPoolCallbackRunner<ReaderHolder>(*create_reader_pool, "Reader"))
{
}

StorageObjectStorageSource::~StorageObjectStorageSource()
{
    create_reader_pool->wait();
}

std::shared_ptr<StorageObjectStorageSource::IIterator> StorageObjectStorageSource::createFileIterator(
    ConfigurationPtr configuration,
    ObjectStoragePtr object_storage,
    bool distributed_processing,
    const ContextPtr & local_context,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns,
    ObjectInfos * read_keys,
    size_t list_object_keys_size,
    CurrentMetrics::Metric metric_threads_,
    CurrentMetrics::Metric metric_threads_active_,
    CurrentMetrics::Metric metric_threads_scheduled_,
    std::function<void(FileProgress)> file_progress_callback)
{
    if (distributed_processing)
        return std::make_shared<ReadTaskIterator>(
            local_context->getReadTaskCallback(),
            local_context->getSettingsRef().max_threads,
            metric_threads_, metric_threads_active_, metric_threads_scheduled_);

    if (configuration->isNamespaceWithGlobs())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression can not have wildcards inside namespace name");

    if (configuration->isPathWithGlobs())
    {
        /// Iterate through disclosed globs and make a source for each file
        return std::make_shared<GlobIterator>(
            object_storage, configuration, predicate, virtual_columns, local_context, read_keys, list_object_keys_size, file_progress_callback);
    }
    else
    {
        return std::make_shared<KeysIterator>(
            object_storage, configuration, virtual_columns, read_keys, file_progress_callback);
    }
}

void StorageObjectStorageSource::lazyInitialize(size_t processor)
{
    if (initialized)
        return;

    reader = createReader(processor);
    if (reader)
        reader_future = createReaderAsync(processor);
    initialized = true;
}

Chunk StorageObjectStorageSource::generate()
{
    lazyInitialize(0);

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
            chassert(object_info.metadata);
            VirtualColumnUtils::addRequestedPathFileAndSizeVirtualsToChunk(
                chunk,
                read_from_format_info.requested_virtual_columns,
                fs::path(configuration->getNamespace()) / reader.getRelativePath(),
                object_info.metadata->size_bytes);

            return chunk;
        }

        if (reader.getInputFormat() && getContext()->getSettingsRef().use_cache_for_count_from_files)
            addNumRowsToCache(reader.getRelativePath(), total_rows_in_file);

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

void StorageObjectStorageSource::addNumRowsToCache(const String & path, size_t num_rows)
{
    const auto cache_key = getKeyForSchemaCache(
        fs::path(configuration->getDataSourceDescription()) / path,
        configuration->format,
        format_settings,
        getContext());

    schema_cache.addNumRows(cache_key, num_rows);
}

std::optional<size_t> StorageObjectStorageSource::tryGetNumRowsFromCache(const ObjectInfoPtr & object_info)
{
    const auto cache_key = getKeyForSchemaCache(
        fs::path(configuration->getDataSourceDescription()) / object_info->relative_path,
        configuration->format,
        format_settings,
        getContext());

    auto get_last_mod_time = [&]() -> std::optional<time_t>
    {
        return object_info->metadata
            ? object_info->metadata->last_modified.epochMicroseconds()
            : 0;
    };
    return schema_cache.tryGetNumRows(cache_key, get_last_mod_time);
}

StorageObjectStorageSource::ReaderHolder StorageObjectStorageSource::createReader(size_t processor)
{
    ObjectInfoPtr object_info;
    do
    {
        object_info = file_iterator->next(processor);
        if (!object_info || object_info->relative_path.empty())
            return {};

        if (!object_info->metadata)
            object_info->metadata = object_storage->getObjectMetadata(object_info->relative_path);
    }
    while (query_settings.skip_empty_files && object_info->metadata->size_bytes == 0);

    QueryPipelineBuilder builder;
    std::shared_ptr<ISource> source;
    std::unique_ptr<ReadBuffer> read_buf;

    std::optional<size_t> num_rows_from_cache = need_only_count
        && getContext()->getSettingsRef().use_cache_for_count_from_files
        ? tryGetNumRowsFromCache(object_info)
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
        const auto compression_method = chooseCompressionMethod(object_info->relative_path, configuration->compression_method);
        const auto max_parsing_threads = need_only_count ? std::optional<size_t>(1) : std::nullopt;
        read_buf = createReadBuffer(object_info->relative_path, object_info->metadata->size_bytes);

        auto input_format = FormatFactory::instance().getInput(
            configuration->format, *read_buf, read_from_format_info.format_header,
            getContext(), max_block_size, format_settings, max_parsing_threads,
            std::nullopt, /* is_remote_fs */ true, compression_method);

        if (key_condition)
            input_format->setKeyCondition(key_condition);

        if (need_only_count)
            input_format->needOnlyCount();

        builder.init(Pipe(input_format));

        if (columns_desc.hasDefaults())
        {
            builder.addSimpleTransform(
                [&](const Block & header)
                {
                    return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext());
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

std::future<StorageObjectStorageSource::ReaderHolder> StorageObjectStorageSource::createReaderAsync(size_t processor)
{
    return create_reader_scheduler([=, this] { return createReader(processor); }, Priority{});
}

std::unique_ptr<ReadBuffer> StorageObjectStorageSource::createReadBuffer(const String & key, size_t object_size)
{
    auto read_settings = getContext()->getReadSettings().adjustBufferSize(object_size);
    read_settings.enable_filesystem_cache = false;
    read_settings.remote_read_min_bytes_for_seek = read_settings.remote_fs_buffer_size;

    const bool object_too_small = object_size <= 2 * getContext()->getSettings().max_download_buffer_size;
    const bool use_prefetch = object_too_small && read_settings.remote_fs_method == RemoteFSReadMethod::threadpool;
    read_settings.remote_fs_method = use_prefetch ? RemoteFSReadMethod::threadpool : RemoteFSReadMethod::read;

    // Create a read buffer that will prefetch the first ~1 MB of the file.
    // When reading lots of tiny files, this prefetching almost doubles the throughput.
    // For bigger files, parallel reading is more useful.
    if (use_prefetch)
    {
        LOG_TRACE(log, "Downloading object of size {} with initial prefetch", object_size);

        auto async_reader = object_storage->readObjects(
            StoredObjects{StoredObject{key, /* local_path */ "", object_size}}, read_settings);

        async_reader->setReadUntilEnd();
        if (read_settings.remote_fs_prefetch)
            async_reader->prefetch(DEFAULT_PREFETCH_PRIORITY);

        return async_reader;
    }
    else
    {
        /// FIXME: this is inconsistent that readObject always reads synchronously ignoring read_method setting.
        return object_storage->readObject(StoredObject(key), read_settings);
    }
}

StorageObjectStorageSource::GlobIterator::GlobIterator(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns_,
    ContextPtr context_,
    ObjectInfos * read_keys_,
    size_t list_object_keys_size,
    std::function<void(FileProgress)> file_progress_callback_)
    : WithContext(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , virtual_columns(virtual_columns_)
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
        const auto key_prefix = configuration->getPathWithoutGlob();
        object_storage_iterator = object_storage->iterate(key_prefix, list_object_keys_size);

        matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(key_with_globs));
        if (matcher->ok())
        {
            recursive = key_with_globs == "/**";
            filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);
        }
        else
        {
            throw Exception(
                ErrorCodes::CANNOT_COMPILE_REGEXP,
                "Cannot compile regex from glob ({}): {}", key_with_globs, matcher->error());
        }
    }
    else
    {
        const auto key_with_globs = configuration_->getPath();
        auto object_metadata = object_storage->getObjectMetadata(key_with_globs);
        auto object_info = std::make_shared<ObjectInfo>(key_with_globs, object_metadata);

        object_infos.emplace_back(object_info);
        if (read_keys)
            read_keys->emplace_back(object_info);

        if (file_progress_callback)
            file_progress_callback(FileProgress(0, object_metadata.size_bytes));

        is_finished = true;
    }
}

ObjectInfoPtr StorageObjectStorageSource::GlobIterator::next(size_t /* processor */)
{
    std::lock_guard lock(next_mutex);

    if (is_finished)
        return {};

    bool need_new_batch = object_infos.empty() || index >= object_infos.size();

    if (need_new_batch)
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
                chassert(*it);
                if (!recursive && !re2::RE2::FullMatch((*it)->relative_path, *matcher))
                    it = new_batch.erase(it);
                else
                    ++it;
            }
        }

        index = 0;

        if (filter_dag)
        {
            std::vector<String> paths;
            paths.reserve(new_batch.size());
            for (const auto & object_info : new_batch)
            {
                chassert(object_info);
                paths.push_back(fs::path(configuration->getNamespace()) / object_info->relative_path);
            }

            VirtualColumnUtils::filterByPathOrFile(new_batch, paths, filter_dag, virtual_columns, getContext());
        }

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

    size_t current_index = index++;
    if (current_index >= object_infos.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index out of bound for blob metadata");

    return object_infos[current_index];
}

StorageObjectStorageSource::KeysIterator::KeysIterator(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    const NamesAndTypesList & virtual_columns_,
    ObjectInfos * read_keys_,
    std::function<void(FileProgress)> file_progress_callback_)
    : object_storage(object_storage_)
    , configuration(configuration_)
    , virtual_columns(virtual_columns_)
    , file_progress_callback(file_progress_callback_)
    , keys(configuration->getPaths())
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

ObjectInfoPtr StorageObjectStorageSource::KeysIterator::next(size_t /* processor */)
{
    size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
    if (current_index >= keys.size())
        return {};

    auto key = keys[current_index];

    ObjectMetadata metadata{};
    if (file_progress_callback)
    {
        metadata = object_storage->getObjectMetadata(key);
        file_progress_callback(FileProgress(0, metadata.size_bytes));
    }

    return std::make_shared<ObjectInfo>(key, metadata);
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

StorageObjectStorageSource::ReaderHolder & StorageObjectStorageSource::ReaderHolder::operator=(ReaderHolder && other) noexcept
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
    const ReadTaskCallback & callback_,
    size_t max_threads_count,
    CurrentMetrics::Metric metric_threads_,
    CurrentMetrics::Metric metric_threads_active_,
    CurrentMetrics::Metric metric_threads_scheduled_)
    : callback(callback_)
{
    ThreadPool pool(metric_threads_, metric_threads_active_, metric_threads_scheduled_, max_threads_count);
    auto pool_scheduler = threadPoolCallbackRunner<String>(pool, "ReadTaskIter");

    std::vector<std::future<String>> keys;
    keys.reserve(max_threads_count);
    for (size_t i = 0; i < max_threads_count; ++i)
        keys.push_back(pool_scheduler([this] { return callback(); }, Priority{}));

    pool.wait();
    buffer.reserve(max_threads_count);
    for (auto & key_future : keys)
        buffer.emplace_back(std::make_shared<ObjectInfo>(key_future.get(), std::nullopt));
}

ObjectInfoPtr StorageObjectStorageSource::ReadTaskIterator::next(size_t)
{
    size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
    if (current_index >= buffer.size())
        return std::make_shared<ObjectInfo>(callback());

    return buffer[current_index];
}

}
