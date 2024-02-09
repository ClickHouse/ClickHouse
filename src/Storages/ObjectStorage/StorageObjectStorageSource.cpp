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
#include <Storages/ObjectStorage/Configuration.h>
#include <Storages/ObjectStorage/Settings.h>
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
}

template <typename StorageSettings>
std::shared_ptr<typename StorageObjectStorageSource<StorageSettings>::IIterator>
StorageObjectStorageSource<StorageSettings>::createFileIterator(
    Storage::ConfigurationPtr configuration,
    ObjectStoragePtr object_storage,
    bool distributed_processing,
    const ContextPtr & local_context,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns,
    ObjectInfos * read_keys,
    std::function<void(FileProgress)> file_progress_callback)
{
    if (distributed_processing)
        return std::make_shared<typename Source::ReadTaskIterator>(local_context->getReadTaskCallback());

    if (configuration->isNamespaceWithGlobs())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression can not have wildcards inside namespace name");

    if (configuration->isPathWithGlobs())
    {
        /// Iterate through disclosed globs and make a source for each file
        return std::make_shared<typename Source::GlobIterator>(
            object_storage, configuration, predicate, virtual_columns, read_keys, file_progress_callback);
    }
    else
    {
        return std::make_shared<typename Source::KeysIterator>(
            object_storage, configuration, virtual_columns, read_keys, file_progress_callback);
    }
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::GlobIterator::GlobIterator(
    ObjectStoragePtr object_storage_,
    Storage::ConfigurationPtr configuration_,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns_,
    ObjectInfos * read_keys_,
    std::function<void(FileProgress)> file_progress_callback_)
    : object_storage(object_storage_)
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
        object_storage_iterator = object_storage->iterate(key_prefix);

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

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::ObjectInfoPtr
StorageObjectStorageSource<StorageSettings>::GlobIterator::next(size_t /* processor */)
{
    std::lock_guard lock(next_mutex);

    if (is_finished && index >= object_infos.size())
        return {};

    bool need_new_batch = object_infos.empty() || index >= object_infos.size();

    if (need_new_batch)
    {
        ObjectInfos new_batch;
        while (new_batch.empty())
        {
            auto result = object_storage_iterator->getCurrentBatchAndScheduleNext();
            if (result.has_value())
            {
                new_batch = result.value();
            }
            else
            {
                is_finished = true;
                return {};
            }

            for (auto it = new_batch.begin(); it != new_batch.end();)
            {
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
            for (auto & object_info : new_batch)
                paths.push_back(fs::path(configuration->getNamespace()) / object_info->relative_path);

            VirtualColumnUtils::filterByPathOrFile(new_batch, paths, filter_dag, virtual_columns, getContext());
        }

        if (read_keys)
            read_keys->insert(read_keys->end(), new_batch.begin(), new_batch.end());

        object_infos = std::move(new_batch);
        if (file_progress_callback)
        {
            for (const auto & object_info : object_infos)
            {
                file_progress_callback(FileProgress(0, object_info->metadata.size_bytes));
            }
        }
    }

    size_t current_index = index++;
    if (current_index >= object_infos.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index out of bound for blob metadata");

    return object_infos[current_index];
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::KeysIterator::KeysIterator(
    ObjectStoragePtr object_storage_,
    Storage::ConfigurationPtr configuration_,
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
            auto object_info = std::make_shared<ObjectInfo>(key, ObjectMetadata{});
            read_keys_->emplace_back(object_info);
        }
    }
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::ObjectInfoPtr
StorageObjectStorageSource<StorageSettings>::KeysIterator::next(size_t /* processor */)
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

template <typename StorageSettings>
Chunk StorageObjectStorageSource<StorageSettings>::generate()
{
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

            VirtualColumnUtils::addRequestedPathFileAndSizeVirtualsToChunk(
                chunk,
                read_from_format_info.requested_virtual_columns,
                fs::path(configuration->getNamespace()) / reader.getRelativePath(),
                reader.getObjectInfo().metadata.size_bytes);

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
        create_reader_pool.wait();
        reader_future = createReaderAsync();
    }

    return {};
}

template <typename StorageSettings>
void StorageObjectStorageSource<StorageSettings>::addNumRowsToCache(const String & path, size_t num_rows)
{
    String source = fs::path(configuration->getDataSourceDescription()) / path;
    auto cache_key = getKeyForSchemaCache(source, configuration->format, format_settings, getContext());
    Storage::getSchemaCache(getContext()).addNumRows(cache_key, num_rows);
}

template <typename StorageSettings>
std::optional<size_t> StorageObjectStorageSource<StorageSettings>::tryGetNumRowsFromCache(const ObjectInfoPtr & object_info)
{
    String source = fs::path(configuration->getDataSourceDescription()) / object_info->relative_path;
    auto cache_key = getKeyForSchemaCache(source, configuration->format, format_settings, getContext());
    auto get_last_mod_time = [&]() -> std::optional<time_t>
    {
        auto last_mod = object_info->metadata.last_modified;
        if (last_mod)
            return last_mod->epochTime();
        else
        {
            object_info->metadata = object_storage->getObjectMetadata(object_info->relative_path);
            return object_info->metadata.last_modified->epochMicroseconds();
        }
    };
    return Storage::getSchemaCache(getContext()).tryGetNumRows(cache_key, get_last_mod_time);
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::StorageObjectStorageSource(
    String name_,
    ObjectStoragePtr object_storage_,
    Storage::ConfigurationPtr configuration_,
    const ReadFromFormatInfo & info,
    std::optional<FormatSettings> format_settings_,
    ContextPtr context_,
    UInt64 max_block_size_,
    std::shared_ptr<IIterator> file_iterator_,
    bool need_only_count_)
    :ISource(info.source_header, false)
    , WithContext(context_)
    , name(std::move(name_))
    , object_storage(object_storage_)
    , configuration(configuration_)
    , format_settings(format_settings_)
    , max_block_size(max_block_size_)
    , need_only_count(need_only_count_)
    , read_from_format_info(info)
    , columns_desc(info.columns_description)
    , file_iterator(file_iterator_)
    , create_reader_pool(StorageSettings::ObjectStorageThreads(),
                         StorageSettings::ObjectStorageThreadsActive(),
                         StorageSettings::ObjectStorageThreadsScheduled(), 1)
    , create_reader_scheduler(threadPoolCallbackRunner<ReaderHolder>(create_reader_pool, "Reader"))
{
    reader = createReader();
    if (reader)
        reader_future = createReaderAsync();
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::~StorageObjectStorageSource()
{
    create_reader_pool.wait();
}

template <typename StorageSettings>
StorageObjectStorageSource<StorageSettings>::ReaderHolder
StorageObjectStorageSource<StorageSettings>::createReader(size_t processor)
{
    auto object_info = file_iterator->next(processor);
    if (object_info->relative_path.empty())
        return {};

    if (object_info->metadata.size_bytes == 0)
        object_info->metadata = object_storage->getObjectMetadata(object_info->relative_path);

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
        source = std::make_shared<ConstChunkGenerator>(
            read_from_format_info.format_header, *num_rows_from_cache, max_block_size);
        builder.init(Pipe(source));
    }
    else
    {
        std::optional<size_t> max_parsing_threads;
        if (need_only_count)
            max_parsing_threads = 1;

        auto compression_method = chooseCompressionMethod(
            object_info->relative_path, configuration->compression_method);

        read_buf = createReadBuffer(object_info->relative_path, object_info->metadata.size_bytes);

        auto input_format = FormatFactory::instance().getInput(
            configuration->format, *read_buf, read_from_format_info.format_header,
            getContext(), max_block_size, format_settings, max_parsing_threads,
            std::nullopt, /* is_remote_fs */ true, compression_method);

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

    return ReaderHolder{object_info, std::move(read_buf),
                        std::move(source), std::move(pipeline), std::move(current_reader)};
}

template <typename StorageSettings>
std::future<typename StorageObjectStorageSource<StorageSettings>::ReaderHolder>
StorageObjectStorageSource<StorageSettings>::createReaderAsync(size_t processor)
{
    return create_reader_scheduler([=, this] { return createReader(processor); }, Priority{});
}

template <typename StorageSettings>
std::unique_ptr<ReadBuffer> StorageObjectStorageSource<StorageSettings>::createReadBuffer(const String & key, size_t object_size)
{
    auto read_settings = getContext()->getReadSettings().adjustBufferSize(object_size);
    read_settings.enable_filesystem_cache = false;
    read_settings.remote_read_min_bytes_for_seek = read_settings.remote_fs_buffer_size;

    // auto download_buffer_size = getContext()->getSettings().max_download_buffer_size;
    // const bool object_too_small = object_size <= 2 * download_buffer_size;

    // Create a read buffer that will prefetch the first ~1 MB of the file.
    // When reading lots of tiny files, this prefetching almost doubles the throughput.
    // For bigger files, parallel reading is more useful.
    // if (object_too_small && read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    // {
    //     LOG_TRACE(log, "Downloading object of size {} with initial prefetch", object_size);

    //     auto async_reader = object_storage->readObjects(
    //         StoredObjects{StoredObject{key, /* local_path */ "", object_size}}, read_settings);

    //     async_reader->setReadUntilEnd();
    //     if (read_settings.remote_fs_prefetch)
    //         async_reader->prefetch(DEFAULT_PREFETCH_PRIORITY);

    //     return async_reader;
    // }
    // else
    return object_storage->readObject(StoredObject(key), read_settings);
}

template class StorageObjectStorageSource<S3StorageSettings>;
template class StorageObjectStorageSource<AzureStorageSettings>;
template class StorageObjectStorageSource<HDFSStorageSettings>;

}
