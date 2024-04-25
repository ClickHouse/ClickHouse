#include "config.h"

#if USE_AWS_S3

#include <Common/isValidUTF8.h>

#include <IO/S3Common.h>
#include <IO/S3/Requests.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/PartitionedSink.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/StoredObject.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>

#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>


#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Planner/Utils.h>
#include <Analyzer/QueryNode.h>

#include <DataTypes/DataTypeString.h>

#include <aws/core/auth/AWSCredentials.h>

#include <Common/NamedCollections/NamedCollections.h>
#include <Common/parseGlobs.h>
#include <Common/quoteString.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <filesystem>

#include <boost/algorithm/string.hpp>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#include <re2/re2.h>
#pragma clang diagnostic pop

namespace fs = std::filesystem;


namespace CurrentMetrics
{
    extern const Metric StorageS3Threads;
    extern const Metric StorageS3ThreadsActive;
    extern const Metric StorageS3ThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event S3DeleteObjects;
    extern const Event S3ListObjects;
    extern const Event EngineFileLikeReadFiles;
}

namespace DB
{

static const std::unordered_set<std::string_view> required_configuration_keys = {
    "url",
};
static const std::unordered_set<std::string_view> optional_configuration_keys = {
    "format",
    "compression",
    "compression_method",
    "structure",
    "access_key_id",
    "secret_access_key",
    "session_token",
    "filename",
    "use_environment_credentials",
    "max_single_read_retries",
    "min_upload_part_size",
    "upload_part_size_multiply_factor",
    "upload_part_size_multiply_parts_count_threshold",
    "max_single_part_upload_size",
    "max_connections",
    "expiration_window_seconds",
    "no_sign_request"
};

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int S3_ERROR;
    extern const int UNEXPECTED_EXPRESSION;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int CANNOT_DETECT_FORMAT;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int FILE_DOESNT_EXIST;
    extern const int NO_ELEMENTS_IN_CONFIG;
}


class ReadFromStorageS3Step : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromStorageS3Step"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    ReadFromStorageS3Step(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        StorageS3 & storage_,
        ReadFromFormatInfo read_from_format_info_,
        bool need_only_count_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(DataStream{.header = std::move(sample_block)}, column_names_, query_info_, storage_snapshot_, context_)
        , column_names(column_names_)
        , storage(storage_)
        , read_from_format_info(std::move(read_from_format_info_))
        , need_only_count(need_only_count_)
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {
        query_configuration = storage.updateConfigurationAndGetCopy(context);
        virtual_columns = storage.getVirtualsList();
    }

private:
    Names column_names;
    StorageS3 & storage;
    ReadFromFormatInfo read_from_format_info;
    bool need_only_count;
    StorageS3::Configuration query_configuration;
    NamesAndTypesList virtual_columns;

    size_t max_block_size;
    size_t num_streams;

    std::shared_ptr<StorageS3Source::IIterator> iterator_wrapper;

    void createIterator(const ActionsDAG::Node * predicate);
};


class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

class StorageS3Source::DisclosedGlobIterator::Impl : WithContext
{
public:
    Impl(
        const S3::Client & client_,
        const S3::URI & globbed_uri_,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList & virtual_columns_,
        ContextPtr context_,
        KeysWithInfo * read_keys_,
        const S3Settings::RequestSettings & request_settings_,
        std::function<void(FileProgress)> file_progress_callback_)
        : WithContext(context_)
        , client(client_.clone())
        , globbed_uri(globbed_uri_)
        , virtual_columns(virtual_columns_)
        , read_keys(read_keys_)
        , request_settings(request_settings_)
        , list_objects_pool(CurrentMetrics::StorageS3Threads, CurrentMetrics::StorageS3ThreadsActive, CurrentMetrics::StorageS3ThreadsScheduled, 1)
        , list_objects_scheduler(threadPoolCallbackRunnerUnsafe<ListObjectsOutcome>(list_objects_pool, "ListObjects"))
        , file_progress_callback(file_progress_callback_)
    {
        if (globbed_uri.bucket.find_first_of("*?{") != std::string::npos)
            throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "Expression can not have wildcards inside bucket name");

        const String key_prefix = globbed_uri.key.substr(0, globbed_uri.key.find_first_of("*?{"));

        /// We don't have to list bucket, because there is no asterisks.
        if (key_prefix.size() == globbed_uri.key.size())
        {
            buffer.emplace_back(std::make_shared<KeyWithInfo>(globbed_uri.key, std::nullopt));
            buffer_iter = buffer.begin();
            is_finished = true;
            return;
        }

        request.SetBucket(globbed_uri.bucket);
        request.SetPrefix(key_prefix);
        request.SetMaxKeys(static_cast<int>(request_settings.list_object_keys_size));

        outcome_future = listObjectsAsync();

        matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(globbed_uri.key));
        if (!matcher->ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                "Cannot compile regex from glob ({}): {}", globbed_uri.key, matcher->error());

        recursive = globbed_uri.key == "/**" ? true : false;

        filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);
        fillInternalBufferAssumeLocked();
    }

    KeyWithInfoPtr next(size_t)
    {
        std::lock_guard lock(mutex);
        return nextAssumeLocked();
    }

    size_t objectsCount()
    {
        return buffer.size();
    }

    ~Impl()
    {
        list_objects_pool.wait();
    }

private:
    using ListObjectsOutcome = Aws::S3::Model::ListObjectsV2Outcome;

    KeyWithInfoPtr nextAssumeLocked()
    {
        do
        {
            if (buffer_iter != buffer.end())
            {
                auto answer = *buffer_iter;
                ++buffer_iter;

                /// If url doesn't contain globs, we didn't list s3 bucket and didn't get object info for the key.
                /// So we get object info lazily here on 'next()' request.
                if (!answer->info)
                {
                    answer->info = S3::getObjectInfo(*client, globbed_uri.bucket, answer->key, globbed_uri.version_id, request_settings);
                    if (file_progress_callback)
                        file_progress_callback(FileProgress(0, answer->info->size));
                }

                return answer;
            }

            if (is_finished)
                return {};

            try
            {
                fillInternalBufferAssumeLocked();
            }
            catch (...)
            {
                /// In case of exception thrown while listing new batch of files
                /// iterator may be partially initialized and its further using may lead to UB.
                /// Iterator is used by several processors from several threads and
                /// it may take some time for threads to stop processors and they
                /// may still use this iterator after exception is thrown.
                /// To avoid this UB, reset the buffer and return defaults for further calls.
                is_finished = true;
                buffer.clear();
                buffer_iter = buffer.begin();
                throw;
            }
        } while (true);
    }

    void fillInternalBufferAssumeLocked()
    {
        buffer.clear();
        assert(outcome_future.valid());
        auto outcome = outcome_future.get();

        if (!outcome.IsSuccess())
        {
            throw S3Exception(outcome.GetError().GetErrorType(), "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
                            quoteString(request.GetBucket()), quoteString(request.GetPrefix()),
                            backQuote(outcome.GetError().GetExceptionName()), quoteString(outcome.GetError().GetMessage()));
        }

        const auto & result_batch = outcome.GetResult().GetContents();

        /// It returns false when all objects were returned
        is_finished = !outcome.GetResult().GetIsTruncated();

        if (!is_finished)
        {
            /// Even if task is finished the thread may be not freed in pool.
            /// So wait until it will be freed before scheduling a new task.
            list_objects_pool.wait();
            outcome_future = listObjectsAsync();
        }

        if (request_settings.throw_on_zero_files_match && result_batch.empty())
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Can not match any files using prefix {}", request.GetPrefix());

        KeysWithInfo temp_buffer;
        temp_buffer.reserve(result_batch.size());

        for (const auto & row : result_batch)
        {
            String key = row.GetKey();
            if (recursive || re2::RE2::FullMatch(key, *matcher))
            {
                S3::ObjectInfo info =
                {
                    .size = size_t(row.GetSize()),
                    .last_modification_time = row.GetLastModified().Millis() / 1000,
                };

                temp_buffer.emplace_back(std::make_shared<KeyWithInfo>(std::move(key), std::move(info)));
            }
        }

        if (temp_buffer.empty())
        {
            buffer_iter = buffer.begin();
            return;
        }

        if (filter_dag)
        {
            std::vector<String> paths;
            paths.reserve(temp_buffer.size());
            for (const auto & key_with_info : temp_buffer)
                paths.push_back(fs::path(globbed_uri.bucket) / key_with_info->key);

            VirtualColumnUtils::filterByPathOrFile(temp_buffer, paths, filter_dag, virtual_columns, getContext());
        }

        buffer = std::move(temp_buffer);

        if (file_progress_callback)
        {
            for (const auto & key_with_info : buffer)
                file_progress_callback(FileProgress(0, key_with_info->info->size));
        }

        /// Set iterator only after the whole batch is processed
        buffer_iter = buffer.begin();

        if (read_keys)
            read_keys->insert(read_keys->end(), buffer.begin(), buffer.end());
    }

    std::future<ListObjectsOutcome> listObjectsAsync()
    {
        return list_objects_scheduler([this]
        {
            ProfileEvents::increment(ProfileEvents::S3ListObjects);
            auto outcome = client->ListObjectsV2(request);

            /// Outcome failure will be handled on the caller side.
            if (outcome.IsSuccess())
                request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

            return outcome;
        }, Priority{});
    }

    std::mutex mutex;

    KeysWithInfo buffer;
    KeysWithInfo::iterator buffer_iter;

    std::unique_ptr<S3::Client> client;
    S3::URI globbed_uri;
    ASTPtr query;
    NamesAndTypesList virtual_columns;
    ActionsDAGPtr filter_dag;
    std::unique_ptr<re2::RE2> matcher;
    bool recursive{false};
    bool is_finished{false};
    KeysWithInfo * read_keys;

    S3::ListObjectsV2Request request;
    S3Settings::RequestSettings request_settings;

    ThreadPool list_objects_pool;
    ThreadPoolCallbackRunnerUnsafe<ListObjectsOutcome> list_objects_scheduler;
    std::future<ListObjectsOutcome> outcome_future;
    std::function<void(FileProgress)> file_progress_callback;
};

StorageS3Source::DisclosedGlobIterator::DisclosedGlobIterator(
    const S3::Client & client_,
    const S3::URI & globbed_uri_,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns_,
    const ContextPtr & context,
    KeysWithInfo * read_keys_,
    const S3Settings::RequestSettings & request_settings_,
    std::function<void(FileProgress)> file_progress_callback_)
    : pimpl(std::make_shared<StorageS3Source::DisclosedGlobIterator::Impl>(client_, globbed_uri_, predicate, virtual_columns_, context, read_keys_, request_settings_, file_progress_callback_))
{
}

StorageS3Source::KeyWithInfoPtr StorageS3Source::DisclosedGlobIterator::next(size_t idx) /// NOLINT
{
    return pimpl->next(idx);
}

size_t StorageS3Source::DisclosedGlobIterator::estimatedKeysCount()
{
    return pimpl->objectsCount();
}

class StorageS3Source::KeysIterator::Impl
{
public:
    explicit Impl(
        const S3::Client & client_,
        const std::string & version_id_,
        const std::vector<String> & keys_,
        const String & bucket_,
        const S3Settings::RequestSettings & request_settings_,
        KeysWithInfo * read_keys_,
        std::function<void(FileProgress)> file_progress_callback_)
        : keys(keys_)
        , client(client_.clone())
        , version_id(version_id_)
        , bucket(bucket_)
        , request_settings(request_settings_)
        , file_progress_callback(file_progress_callback_)
    {
        if (read_keys_)
        {
            for (const auto & key : keys)
                read_keys_->push_back(std::make_shared<KeyWithInfo>(key));
        }
    }

    KeyWithInfoPtr next(size_t)
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= keys.size())
            return {};
        auto key = keys[current_index];
        std::optional<S3::ObjectInfo> info;
        if (file_progress_callback)
        {
            info = S3::getObjectInfo(*client, bucket, key, version_id, request_settings);
            file_progress_callback(FileProgress(0, info->size));
        }

        return std::make_shared<KeyWithInfo>(key, info);
    }

    size_t objectsCount()
    {
        return keys.size();
    }

private:
    Strings keys;
    std::atomic_size_t index = 0;
    std::unique_ptr<S3::Client> client;
    String version_id;
    String bucket;
    S3Settings::RequestSettings request_settings;
    std::function<void(FileProgress)> file_progress_callback;
};

StorageS3Source::KeysIterator::KeysIterator(
    const S3::Client & client_,
    const std::string & version_id_,
    const std::vector<String> & keys_,
    const String & bucket_,
    const S3Settings::RequestSettings & request_settings_,
    KeysWithInfo * read_keys,
    std::function<void(FileProgress)> file_progress_callback_)
    : pimpl(std::make_shared<StorageS3Source::KeysIterator::Impl>(
        client_, version_id_, keys_, bucket_, request_settings_,
        read_keys, file_progress_callback_))
{
}

StorageS3Source::KeyWithInfoPtr StorageS3Source::KeysIterator::next(size_t idx) /// NOLINT
{
    return pimpl->next(idx);
}

size_t StorageS3Source::KeysIterator::estimatedKeysCount()
{
    return pimpl->objectsCount();
}

StorageS3Source::ReadTaskIterator::ReadTaskIterator(
    const DB::ReadTaskCallback & callback_,
    size_t max_threads_count)
    : callback(callback_)
{
    ThreadPool pool(CurrentMetrics::StorageS3Threads, CurrentMetrics::StorageS3ThreadsActive, CurrentMetrics::StorageS3ThreadsScheduled, max_threads_count);
    auto pool_scheduler = threadPoolCallbackRunnerUnsafe<String>(pool, "S3ReadTaskItr");

    std::vector<std::future<String>> keys;
    keys.reserve(max_threads_count);
    for (size_t i = 0; i < max_threads_count; ++i)
        keys.push_back(pool_scheduler([this] { return callback(); }, Priority{}));

    pool.wait();
    buffer.reserve(max_threads_count);
    for (auto & key_future : keys)
        buffer.emplace_back(std::make_shared<KeyWithInfo>(key_future.get(), std::nullopt));
}

StorageS3Source::KeyWithInfoPtr StorageS3Source::ReadTaskIterator::next(size_t) /// NOLINT
{
    size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
    if (current_index >= buffer.size())
        return std::make_shared<KeyWithInfo>(callback());

    while (current_index < buffer.size())
    {
        if (const auto & key_info = buffer[current_index]; key_info && !key_info->key.empty())
            return buffer[current_index];

        current_index = index.fetch_add(1, std::memory_order_relaxed);
    }

    return nullptr;
}

size_t StorageS3Source::ReadTaskIterator::estimatedKeysCount()
{
    return buffer.size();
}

StorageS3Source::StorageS3Source(
    const ReadFromFormatInfo & info,
    const String & format_,
    String name_,
    const ContextPtr & context_,
    std::optional<FormatSettings> format_settings_,
    UInt64 max_block_size_,
    const S3Settings::RequestSettings & request_settings_,
    String compression_hint_,
    const std::shared_ptr<const S3::Client> & client_,
    const String & bucket_,
    const String & version_id_,
    const String & url_host_and_port_,
    std::shared_ptr<IIterator> file_iterator_,
    const size_t max_parsing_threads_,
    bool need_only_count_)
    : SourceWithKeyCondition(info.source_header, false)
    , WithContext(context_)
    , name(std::move(name_))
    , bucket(bucket_)
    , version_id(version_id_)
    , url_host_and_port(url_host_and_port_)
    , format(format_)
    , columns_desc(info.columns_description)
    , requested_columns(info.requested_columns)
    , max_block_size(max_block_size_)
    , request_settings(request_settings_)
    , compression_hint(std::move(compression_hint_))
    , client(client_)
    , sample_block(info.format_header)
    , format_settings(format_settings_)
    , requested_virtual_columns(info.requested_virtual_columns)
    , file_iterator(file_iterator_)
    , max_parsing_threads(max_parsing_threads_)
    , need_only_count(need_only_count_)
    , create_reader_pool(CurrentMetrics::StorageS3Threads, CurrentMetrics::StorageS3ThreadsActive, CurrentMetrics::StorageS3ThreadsScheduled, 1)
    , create_reader_scheduler(threadPoolCallbackRunnerUnsafe<ReaderHolder>(create_reader_pool, "CreateS3Reader"))
{
}

void StorageS3Source::lazyInitialize(size_t idx)
{
    if (initialized)
        return;

    reader = createReader(idx);
    if (reader)
        reader_future = createReaderAsync(idx);
    initialized = true;
}

StorageS3Source::ReaderHolder StorageS3Source::createReader(size_t idx)
{
    KeyWithInfoPtr key_with_info;
    do
    {
        key_with_info = file_iterator->next(idx);
        if (!key_with_info || key_with_info->key.empty())
            return {};

        if (!key_with_info->info)
            key_with_info->info = S3::getObjectInfo(*client, bucket, key_with_info->key, version_id, request_settings);
    }
    while (getContext()->getSettingsRef().s3_skip_empty_files && key_with_info->info->size == 0);

    QueryPipelineBuilder builder;
    std::shared_ptr<ISource> source;
    std::unique_ptr<ReadBuffer> read_buf;
    std::optional<size_t> num_rows_from_cache = need_only_count && getContext()->getSettingsRef().use_cache_for_count_from_files ? tryGetNumRowsFromCache(*key_with_info) : std::nullopt;
    if (num_rows_from_cache)
    {
        /// We should not return single chunk with all number of rows,
        /// because there is a chance that this chunk will be materialized later
        /// (it can cause memory problems even with default values in columns or when virtual columns are requested).
        /// Instead, we use special ConstChunkGenerator that will generate chunks
        /// with max_block_size rows until total number of rows is reached.
        source = std::make_shared<ConstChunkGenerator>(sample_block, *num_rows_from_cache, max_block_size);
        builder.init(Pipe(source));
    }
    else
    {
        auto compression_method = chooseCompressionMethod(key_with_info->key, compression_hint);
        read_buf = createS3ReadBuffer(key_with_info->key, key_with_info->info->size);

        auto input_format = FormatFactory::instance().getInput(
            format,
            *read_buf,
            sample_block,
            getContext(),
            max_block_size,
            format_settings,
            max_parsing_threads,
            /* max_download_threads= */ std::nullopt,
            /* is_remote_fs */ true,
            compression_method,
            need_only_count);

        if (key_condition)
            input_format->setKeyCondition(key_condition);

        if (need_only_count)
            input_format->needOnlyCount();

        builder.init(Pipe(input_format));

        if (columns_desc.hasDefaults())
        {
            builder.addSimpleTransform(
                [&](const Block & header)
                { return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext()); });
        }

        source = input_format;
    }

    /// Add ExtractColumnsTransform to extract requested columns/subcolumns
    /// from chunk read by IInputFormat.
    builder.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExtractColumnsTransform>(header, requested_columns);
    });

    auto pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    auto current_reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    ProfileEvents::increment(ProfileEvents::EngineFileLikeReadFiles);

    return ReaderHolder{key_with_info, bucket, std::move(read_buf), std::move(source), std::move(pipeline), std::move(current_reader)};
}

std::future<StorageS3Source::ReaderHolder> StorageS3Source::createReaderAsync(size_t idx)
{
    return create_reader_scheduler([=, this] { return createReader(idx); }, Priority{});
}

std::unique_ptr<ReadBuffer> StorageS3Source::createS3ReadBuffer(const String & key, size_t object_size)
{
    auto read_settings = getContext()->getReadSettings().adjustBufferSize(object_size);
    read_settings.enable_filesystem_cache = false;
    auto download_buffer_size = getContext()->getSettings().max_download_buffer_size;
    const bool object_too_small = object_size <= 2 * download_buffer_size;

    // Create a read buffer that will prefetch the first ~1 MB of the file.
    // When reading lots of tiny files, this prefetching almost doubles the throughput.
    // For bigger files, parallel reading is more useful.
    if (object_too_small && read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        LOG_TRACE(log, "Downloading object of size {} from S3 with initial prefetch", object_size);
        return createAsyncS3ReadBuffer(key, read_settings, object_size);
    }

    return std::make_unique<ReadBufferFromS3>(
        client, bucket, key, version_id, request_settings, read_settings,
        /*use_external_buffer*/ false, /*offset_*/ 0, /*read_until_position_*/ 0,
        /*restricted_seek_*/ false, object_size);
}

std::unique_ptr<ReadBuffer> StorageS3Source::createAsyncS3ReadBuffer(
    const String & key, const ReadSettings & read_settings, size_t object_size)
{
    auto context = getContext();
    auto read_buffer_creator =
        [this, read_settings, object_size]
        (bool restricted_seek, const StoredObject & object) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<ReadBufferFromS3>(
            client,
            bucket,
            object.remote_path,
            version_id,
            request_settings,
            read_settings,
            /* use_external_buffer */true,
            /* offset */0,
            /* read_until_position */0,
            restricted_seek,
            object_size);
    };

    auto modified_settings{read_settings};
    /// User's S3 object may change, don't cache it.
    modified_settings.use_page_cache_for_disks_without_file_cache = false;

    /// FIXME: Changing this setting to default value breaks something around parquet reading
    modified_settings.remote_read_min_bytes_for_seek = modified_settings.remote_fs_buffer_size;

    auto s3_impl = std::make_unique<ReadBufferFromRemoteFSGather>(
        std::move(read_buffer_creator),
        StoredObjects{StoredObject{key, /* local_path */ "", object_size}},
        "",
        read_settings,
        /* cache_log */nullptr, /* use_external_buffer */true);

    auto & pool_reader = context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
    auto async_reader = std::make_unique<AsynchronousBoundedReadBuffer>(
        std::move(s3_impl), pool_reader, modified_settings,
        context->getAsyncReadCounters(), context->getFilesystemReadPrefetchesLog());

    async_reader->setReadUntilEnd();
    if (read_settings.remote_fs_prefetch)
        async_reader->prefetch(DEFAULT_PREFETCH_PRIORITY);

    return async_reader;
}

StorageS3Source::~StorageS3Source()
{
    create_reader_pool.wait();
}

String StorageS3Source::getName() const
{
    return name;
}

Chunk StorageS3Source::generate()
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
                chunk_size = reader.getInputFormat()->getApproxBytesReadForChunk();
            progress(num_rows, chunk_size ? chunk_size : chunk.bytes());
            VirtualColumnUtils::addRequestedPathFileAndSizeVirtualsToChunk(chunk, requested_virtual_columns, reader.getPath(), reader.getFileSize());
            return chunk;
        }

        if (reader.getInputFormat() && getContext()->getSettingsRef().use_cache_for_count_from_files)
            addNumRowsToCache(reader.getFile(), total_rows_in_file);

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

void StorageS3Source::addNumRowsToCache(const String & key, size_t num_rows)
{
    String source = fs::path(url_host_and_port) / bucket / key;
    auto cache_key = getKeyForSchemaCache(source, format, format_settings, getContext());
    StorageS3::getSchemaCache(getContext()).addNumRows(cache_key, num_rows);
}

std::optional<size_t> StorageS3Source::tryGetNumRowsFromCache(const KeyWithInfo & key_with_info)
{
    String source = fs::path(url_host_and_port) / bucket / key_with_info.key;
    auto cache_key = getKeyForSchemaCache(source, format, format_settings, getContext());
    auto get_last_mod_time = [&]() -> std::optional<time_t>
    {
        return key_with_info.info->last_modification_time;
    };

    return StorageS3::getSchemaCache(getContext()).tryGetNumRows(cache_key, get_last_mod_time);
}

class StorageS3Sink : public SinkToStorage
{
public:
    StorageS3Sink(
        const String & format,
        const Block & sample_block_,
        const ContextPtr & context,
        std::optional<FormatSettings> format_settings_,
        const CompressionMethod compression_method,
        const StorageS3::Configuration & configuration_,
        const String & bucket,
        const String & key)
        : SinkToStorage(sample_block_)
        , sample_block(sample_block_)
        , format_settings(format_settings_)
    {
        BlobStorageLogWriterPtr blob_log = nullptr;
        if (auto blob_storage_log = context->getBlobStorageLog())
        {
            blob_log = std::make_shared<BlobStorageLogWriter>(std::move(blob_storage_log));
            blob_log->query_id = context->getCurrentQueryId();
        }

        const auto & settings = context->getSettingsRef();
        write_buf = wrapWriteBufferWithCompressionMethod(
            std::make_unique<WriteBufferFromS3>(
                configuration_.client,
                bucket,
                key,
                DBMS_DEFAULT_BUFFER_SIZE,
                configuration_.request_settings,
                std::move(blob_log),
                std::nullopt,
                threadPoolCallbackRunnerUnsafe<void>(getIOThreadPool().get(), "S3ParallelWrite"),
                context->getWriteSettings()),
            compression_method,
            static_cast<int>(settings.output_format_compression_level),
            static_cast<int>(settings.output_format_compression_zstd_window_log));
        writer
            = FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, sample_block, context, format_settings);
    }

    String getName() const override { return "StorageS3Sink"; }

    void consume(Chunk chunk) override
    {
        std::lock_guard lock(cancel_mutex);
        if (cancelled)
            return;
        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onCancel() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
        cancelled = true;
    }

    void onException(std::exception_ptr exception) override
    {
        std::lock_guard lock(cancel_mutex);
        try
        {
            std::rethrow_exception(exception);
        }
        catch (...)
        {
            /// An exception context is needed to proper delete write buffers without finalization
            release();
        }
    }

    void onFinish() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
    }

private:
    void finalize()
    {
        if (!writer)
            return;

        try
        {
            writer->finalize();
            writer->flush();
            write_buf->finalize();
        }
        catch (...)
        {
            /// Stop ParallelFormattingOutputFormat correctly.
            release();
            throw;
        }
    }

    void release()
    {
        writer.reset();
        write_buf.reset();
    }

    Block sample_block;
    std::optional<FormatSettings> format_settings;
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    bool cancelled = false;
    std::mutex cancel_mutex;
};

namespace
{
    std::optional<String> checkAndGetNewFileOnInsertIfNeeded(const ContextPtr & context, const StorageS3::Configuration & configuration, const String & key, size_t sequence_number)
    {
        if (context->getSettingsRef().s3_truncate_on_insert || !S3::objectExists(*configuration.client, configuration.url.bucket, key, configuration.url.version_id, configuration.request_settings))
            return std::nullopt;

        if (context->getSettingsRef().s3_create_new_file_on_insert)
        {
            auto pos = key.find_first_of('.');
            String new_key;
            do
            {
                new_key = key.substr(0, pos) + "." + std::to_string(sequence_number) + (pos == std::string::npos ? "" : key.substr(pos));
                ++sequence_number;
            }
            while (S3::objectExists(*configuration.client, configuration.url.bucket, new_key, configuration.url.version_id, configuration.request_settings));

            return new_key;
        }

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Object in bucket {} with key {} already exists. "
            "If you want to overwrite it, enable setting s3_truncate_on_insert, if you "
            "want to create a new file on each insert, enable setting s3_create_new_file_on_insert",
            configuration.url.bucket, key);
    }
}


class PartitionedStorageS3Sink : public PartitionedSink, WithContext
{
public:
    PartitionedStorageS3Sink(
        const ASTPtr & partition_by,
        const String & format_,
        const Block & sample_block_,
        const ContextPtr & context_,
        std::optional<FormatSettings> format_settings_,
        const CompressionMethod compression_method_,
        const StorageS3::Configuration & configuration_,
        const String & bucket_,
        const String & key_)
        : PartitionedSink(partition_by, context_, sample_block_), WithContext(context_)
        , format(format_)
        , sample_block(sample_block_)
        , compression_method(compression_method_)
        , configuration(configuration_)
        , bucket(bucket_)
        , key(key_)
        , format_settings(format_settings_)
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto partition_bucket = replaceWildcards(bucket, partition_id);
        validateBucket(partition_bucket);

        auto partition_key = replaceWildcards(key, partition_id);
        validateKey(partition_key);

        if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(getContext(), configuration, partition_key, /* sequence_number */1))
            partition_key = *new_key;

        return std::make_shared<StorageS3Sink>(
            format,
            sample_block,
            getContext(),
            format_settings,
            compression_method,
            configuration,
            partition_bucket,
            partition_key
        );
    }

private:
    const String format;
    const Block sample_block;
    const CompressionMethod compression_method;
    const StorageS3::Configuration configuration;
    const String bucket;
    const String key;
    const std::optional<FormatSettings> format_settings;

    static void validateBucket(const String & str)
    {
        S3::URI::validateBucket(str, {});

        if (!DB::UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()))
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in bucket name");

        validatePartitionKey(str, false);
    }

    static void validateKey(const String & str)
    {
        /// See:
        /// - https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
        /// - https://cloud.ibm.com/apidocs/cos/cos-compatibility#putobject

        if (str.empty() || str.size() > 1024)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect key length (not empty, max 1023 characters), got: {}", str.size());

        if (!DB::UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()))
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in key");

        validatePartitionKey(str, true);
    }
};


StorageS3::StorageS3(
    const Configuration & configuration_,
    const ContextPtr & context_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , configuration(configuration_)
    , name(configuration.url.storage_name)
    , distributed_processing(distributed_processing_)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
{
    updateConfiguration(context_); // NOLINT(clang-analyzer-optin.cplusplus.VirtualCall)

    if (configuration.format != "auto")
        FormatFactory::instance().checkFormatName(configuration.format);
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(configuration.url.uri);
    context_->getGlobalContext()->getHTTPHeaderFilter().checkHeaders(configuration.headers_from_ast);

    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        ColumnsDescription columns;
        if (configuration.format == "auto")
            std::tie(columns, configuration.format) = getTableStructureAndFormatFromData(configuration, format_settings, context_);
        else
            columns = getTableStructureFromData(configuration, format_settings, context_);

        storage_metadata.setColumns(columns);
    }
    else
    {
        if (configuration.format == "auto")
            configuration.format = getTableStructureAndFormatFromData(configuration, format_settings, context_).second;

        /// We don't allow special columns in S3 storage.
        if (!columns_.hasOnlyOrdinary())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table engine S3 doesn't support special columns like MATERIALIZED, ALIAS or EPHEMERAL");
        storage_metadata.setColumns(columns_);
    }

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.getColumns()));
}

static std::shared_ptr<StorageS3Source::IIterator> createFileIterator(
    const StorageS3::Configuration & configuration,
    bool distributed_processing,
    ContextPtr local_context,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns,
    StorageS3::KeysWithInfo * read_keys = nullptr,
    std::function<void(FileProgress)> file_progress_callback = {})
{
    if (distributed_processing)
    {
        return std::make_shared<StorageS3Source::ReadTaskIterator>(local_context->getReadTaskCallback(), local_context->getSettingsRef().max_threads);
    }
    else if (configuration.withGlobs())
    {
        /// Iterate through disclosed globs and make a source for each file
        return std::make_shared<StorageS3Source::DisclosedGlobIterator>(
            *configuration.client, configuration.url, predicate, virtual_columns,
            local_context, read_keys, configuration.request_settings, file_progress_callback);
    }
    else
    {
        Strings keys = configuration.keys;
        auto filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);
        if (filter_dag)
        {
            std::vector<String> paths;
            paths.reserve(keys.size());
            for (const auto & key : keys)
                paths.push_back(fs::path(configuration.url.bucket) / key);
            VirtualColumnUtils::filterByPathOrFile(keys, paths, filter_dag, virtual_columns, local_context);
        }

        return std::make_shared<StorageS3Source::KeysIterator>(
            *configuration.client, configuration.url.version_id, keys,
            configuration.url.bucket, configuration.request_settings, read_keys, file_progress_callback);
    }
}

bool StorageS3::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration.format, context, format_settings);
}

bool StorageS3::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(configuration.format);
}

bool StorageS3::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(configuration.format, context);
}

void StorageS3::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(local_context));

    bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && local_context->getSettingsRef().optimize_count_from_files;

    auto reading = std::make_unique<ReadFromStorageS3Step>(
        column_names,
        query_info,
        storage_snapshot,
        local_context,
        read_from_format_info.source_header,
        *this,
        std::move(read_from_format_info),
        need_only_count,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(reading));
}

void ReadFromStorageS3Step::applyFilters(ActionDAGNodes added_filter_nodes)
{
    filter_actions_dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes);
    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    createIterator(predicate);
}

void ReadFromStorageS3Step::createIterator(const ActionsDAG::Node * predicate)
{
    if (iterator_wrapper)
        return;

    iterator_wrapper = createFileIterator(
        query_configuration, storage.distributed_processing, context, predicate,
        virtual_columns, nullptr, context->getFileProgressCallback());
}

void ReadFromStorageS3Step::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (storage.partition_by && query_configuration.withWildcard())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading from a partitioned S3 storage is not implemented yet");

    createIterator(nullptr);

    size_t estimated_keys_count = iterator_wrapper->estimatedKeysCount();
    if (estimated_keys_count > 1)
        num_streams = std::min(num_streams, estimated_keys_count);
    else
        /// Disclosed glob iterator can underestimate the amount of keys in some cases. We will keep one stream for this particular case.
        num_streams = 1;

    const size_t max_threads = context->getSettingsRef().max_threads;
    const size_t max_parsing_threads = num_streams >= max_threads ? 1 : (max_threads / std::max(num_streams, 1ul));
    LOG_DEBUG(getLogger("StorageS3"), "Reading in {} streams, {} threads per stream", num_streams, max_parsing_threads);

    Pipes pipes;
    pipes.reserve(num_streams);
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<StorageS3Source>(
            read_from_format_info,
            query_configuration.format,
            storage.getName(),
            context,
            storage.format_settings,
            max_block_size,
            query_configuration.request_settings,
            query_configuration.compression_method,
            query_configuration.client,
            query_configuration.url.bucket,
            query_configuration.url.version_id,
            query_configuration.url.uri.getHost() + std::to_string(query_configuration.url.uri.getPort()),
            iterator_wrapper,
            max_parsing_threads,
            need_only_count);

        source->setKeyCondition(filter_actions_dag, context);
        pipes.emplace_back(std::move(source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(read_from_format_info.source_header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

SinkToStoragePtr StorageS3::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    auto query_configuration = updateConfigurationAndGetCopy(local_context);
    auto key = query_configuration.keys.front();

    auto sample_block = metadata_snapshot->getSampleBlock();
    auto chosen_compression_method = chooseCompressionMethod(query_configuration.keys.back(), query_configuration.compression_method);
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query);

    auto partition_by_ast = insert_query ? (insert_query->partition_by ? insert_query->partition_by : partition_by) : nullptr;
    bool is_partitioned_implementation = partition_by_ast && query_configuration.withWildcard();

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedStorageS3Sink>(
            partition_by_ast,
            query_configuration.format,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            query_configuration,
            query_configuration.url.bucket,
            key);
    }
    else
    {
        if (query_configuration.withGlobs())
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                            "S3 key '{}' contains globs, so the table is in readonly mode", query_configuration.url.key);

        if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(local_context, configuration, query_configuration.keys.front(), query_configuration.keys.size()))
        {
            query_configuration.keys.push_back(*new_key);
            configuration.keys.push_back(*new_key);
            key = *new_key;
        }

        return std::make_shared<StorageS3Sink>(
            query_configuration.format,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            query_configuration,
            query_configuration.url.bucket,
            key);
    }
}

void StorageS3::truncate(const ASTPtr & /* query */, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    auto query_configuration = updateConfigurationAndGetCopy(local_context);

    if (query_configuration.withGlobs())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "S3 key '{}' contains globs, so the table is in readonly mode",
            query_configuration.url.key);
    }

    Aws::S3::Model::Delete delkeys;

    for (const auto & key : query_configuration.keys)
    {
        Aws::S3::Model::ObjectIdentifier obj;
        obj.SetKey(key);
        delkeys.AddObjects(std::move(obj));
    }

    ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
    S3::DeleteObjectsRequest request;
    request.SetBucket(query_configuration.url.bucket);
    request.SetDelete(delkeys);

    auto response = query_configuration.client->DeleteObjects(request);

    const auto * response_error = response.IsSuccess() ? nullptr : &response.GetError();
    auto time_now = std::chrono::system_clock::now();
    if (auto blob_storage_log = BlobStorageLogWriter::create())
    {
        for (const auto & key : query_configuration.keys)
            blob_storage_log->addEvent(BlobStorageLogElement::EventType::Delete, query_configuration.url.bucket, key, {}, 0, response_error, time_now);
    }

    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw S3Exception(err.GetMessage(), err.GetErrorType());
    }

    for (const auto & error : response.GetResult().GetErrors())
        LOG_WARNING(getLogger("StorageS3"), "Failed to delete {}, error: {}", error.GetKey(), error.GetMessage());
}

StorageS3::Configuration StorageS3::updateConfigurationAndGetCopy(const ContextPtr & local_context)
{
    std::lock_guard lock(configuration_update_mutex);
    configuration.update(local_context);
    return configuration;
}

void StorageS3::updateConfiguration(const ContextPtr & local_context)
{
    std::lock_guard lock(configuration_update_mutex);
    configuration.update(local_context);
}

void StorageS3::useConfiguration(const Configuration & new_configuration)
{
    std::lock_guard lock(configuration_update_mutex);
    configuration = new_configuration;
}

const StorageS3::Configuration & StorageS3::getConfiguration()
{
    std::lock_guard lock(configuration_update_mutex);
    return configuration;
}

bool StorageS3::Configuration::update(const ContextPtr & context)
{
    auto s3_settings = context->getStorageS3Settings().getSettings(url.uri.toString(), context->getUserName());
    request_settings = s3_settings.request_settings;
    request_settings.updateFromSettings(context->getSettings());

    if (client && (static_configuration || !auth_settings.hasUpdates(s3_settings.auth_settings)))
        return false;

    auth_settings.updateFrom(s3_settings.auth_settings);
    keys[0] = url.key;
    connect(context);
    return true;
}

void StorageS3::Configuration::connect(const ContextPtr & context)
{
    const Settings & global_settings = context->getGlobalContext()->getSettingsRef();
    const Settings & local_settings = context->getSettingsRef();

    if (S3::isS3ExpressEndpoint(url.endpoint) && auth_settings.region.empty())
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Region should be explicitly specified for directory buckets");

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        auth_settings.region,
        context->getRemoteHostFilter(),
        static_cast<unsigned>(global_settings.s3_max_redirects),
        static_cast<unsigned>(global_settings.s3_retry_attempts),
        global_settings.enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        request_settings.get_request_throttler,
        request_settings.put_request_throttler,
        url.uri.getScheme());

    client_configuration.endpointOverride = url.endpoint;
    /// seems as we don't use it
    client_configuration.maxConnections = static_cast<unsigned>(request_settings.max_connections);
    client_configuration.connectTimeoutMs = local_settings.s3_connect_timeout_ms;
    client_configuration.http_keep_alive_timeout = S3::DEFAULT_KEEP_ALIVE_TIMEOUT;
    client_configuration.http_keep_alive_max_requests = S3::DEFAULT_KEEP_ALIVE_MAX_REQUESTS;

    auto headers = auth_settings.headers;
    if (!headers_from_ast.empty())
        headers.insert(headers.end(), headers_from_ast.begin(), headers_from_ast.end());

    client_configuration.requestTimeoutMs = request_settings.request_timeout_ms;

    S3::ClientSettings client_settings{
        .use_virtual_addressing = url.is_virtual_hosted_style,
        .disable_checksum = local_settings.s3_disable_checksum,
        .gcs_issue_compose_request = context->getConfigRef().getBool("s3.gcs_issue_compose_request", false),
        .is_s3express_bucket = S3::isS3ExpressEndpoint(url.endpoint),
    };

    auto credentials = Aws::Auth::AWSCredentials(auth_settings.access_key_id, auth_settings.secret_access_key, auth_settings.session_token);
    client = S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        auth_settings.server_side_encryption_customer_key_base64,
        auth_settings.server_side_encryption_kms_config,
        std::move(headers),
        S3::CredentialsConfiguration{
            auth_settings.use_environment_credentials.value_or(context->getConfigRef().getBool("s3.use_environment_credentials", true)),
            auth_settings.use_insecure_imds_request.value_or(context->getConfigRef().getBool("s3.use_insecure_imds_request", false)),
            auth_settings.expiration_window_seconds.value_or(
                context->getConfigRef().getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
            auth_settings.no_sign_request.value_or(context->getConfigRef().getBool("s3.no_sign_request", false)),
        },
        credentials.GetSessionToken());
}

void StorageS3::processNamedCollectionResult(StorageS3::Configuration & configuration, const NamedCollection & collection)
{
    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys);

    auto filename = collection.getOrDefault<String>("filename", "");
    if (!filename.empty())
        configuration.url = S3::URI(std::filesystem::path(collection.get<String>("url")) / filename);
    else
        configuration.url = S3::URI(collection.get<String>("url"));

    configuration.auth_settings.access_key_id = collection.getOrDefault<String>("access_key_id", "");
    configuration.auth_settings.secret_access_key = collection.getOrDefault<String>("secret_access_key", "");
    configuration.auth_settings.use_environment_credentials = collection.getOrDefault<UInt64>("use_environment_credentials", 1);
    configuration.auth_settings.no_sign_request = collection.getOrDefault<bool>("no_sign_request", false);
    configuration.auth_settings.expiration_window_seconds = collection.getOrDefault<UInt64>("expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS);

    configuration.format = collection.getOrDefault<String>("format", configuration.format);
    configuration.compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    configuration.structure = collection.getOrDefault<String>("structure", "auto");

    configuration.request_settings = S3Settings::RequestSettings(collection);
}

StorageS3::Configuration StorageS3::getConfiguration(ASTs & engine_args, const ContextPtr & local_context, bool get_format_from_file)
{
    StorageS3::Configuration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
    {
        processNamedCollectionResult(configuration, *named_collection);
    }
    else
    {
        /// Supported signatures:
        ///
        /// S3('url')
        /// S3('url', 'format')
        /// S3('url', 'format', 'compression')
        /// S3('url', NOSIGN)
        /// S3('url', NOSIGN, 'format')
        /// S3('url', NOSIGN, 'format', 'compression')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'session_token')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'session_token', 'format')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'session_token', 'format', 'compression')
        /// with optional headers() function

        size_t count = StorageURL::evalArgsAndCollectHeaders(engine_args, configuration.headers_from_ast, local_context);

        if (count == 0 || count > 6)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage S3 requires 1 to 5 arguments: "
                            "url, [NOSIGN | access_key_id, secret_access_key], name of used format and [compression_method]");

        std::unordered_map<std::string_view, size_t> engine_args_to_idx;
        bool no_sign_request = false;

        /// For 2 arguments we support 2 possible variants:
        /// - s3(source, format)
        /// - s3(source, NOSIGN)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or not.
        if (count == 2)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
                no_sign_request = true;
            else
                engine_args_to_idx = {{"format", 1}};
        }
        /// For 3 arguments we support 2 possible variants:
        /// - s3(source, format, compression_method)
        /// - s3(source, access_key_id, secret_access_key)
        /// - s3(source, NOSIGN, format)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or format name.
        else if (count == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "format/access_key_id/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                engine_args_to_idx = {{"format", 2}};
            }
            else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
                engine_args_to_idx = {{"format", 1}, {"compression_method", 2}};
            else
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
        }
        /// For 4 arguments we support 3 possible variants:
        /// - s3(source, access_key_id, secret_access_key, session_token)
        /// - s3(source, access_key_id, secret_access_key, format)
        /// - s3(source, NOSIGN, format, compression_method)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN or not.
        else if (count == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                engine_args_to_idx = {{"format", 2}, {"compression_method", 3}};
            }
            else
            {
                auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "session_token/format");
                if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
                {
                    engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
                }
                else
                {
                    engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}};
                }
            }
        }
        /// For 5 arguments we support 2 possible variants:
        /// - s3(source, access_key_id, secret_access_key, session_token, format)
        /// - s3(source, access_key_id, secret_access_key, format, compression)
        else if (count == 5)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "session_token/format");
            if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"compression", 4}};
            }
            else
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}};
            }
        }
        else if (count == 6)
        {
            engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"compression_method", 5}};
        }

        /// This argument is always the first
        configuration.url = S3::URI(checkAndGetLiteralArgument<String>(engine_args[0], "url"));

        if (engine_args_to_idx.contains("format"))
            configuration.format = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["format"]], "format");

        if (engine_args_to_idx.contains("compression_method"))
            configuration.compression_method = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["compression_method"]], "compression_method");

        if (engine_args_to_idx.contains("access_key_id"))
            configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["access_key_id"]], "access_key_id");

        if (engine_args_to_idx.contains("secret_access_key"))
            configuration.auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["secret_access_key"]], "secret_access_key");

        if (engine_args_to_idx.contains("session_token"))
            configuration.auth_settings.session_token = checkAndGetLiteralArgument<String>(engine_args[engine_args_to_idx["session_token"]], "session_token");

        if (no_sign_request)
            configuration.auth_settings.no_sign_request = no_sign_request;
    }

    configuration.static_configuration = !configuration.auth_settings.access_key_id.empty() || configuration.auth_settings.no_sign_request.has_value();

    configuration.keys = {configuration.url.key};

    if (configuration.format == "auto" && get_format_from_file)
        configuration.format = FormatFactory::instance().tryGetFormatFromFileName(configuration.url.key).value_or("auto");

    return configuration;
}

ColumnsDescription StorageS3::getTableStructureFromData(
    const StorageS3::Configuration & configuration,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & ctx)
{
    return getTableStructureAndFormatFromDataImpl(configuration.format, configuration, format_settings, ctx).first;
}

std::pair<ColumnsDescription, String> StorageS3::getTableStructureAndFormatFromData(
    const StorageS3::Configuration & configuration,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & ctx)
{
    return getTableStructureAndFormatFromDataImpl(std::nullopt, configuration, format_settings, ctx);
}

namespace
{
    class ReadBufferIterator : public IReadBufferIterator, WithContext
    {
    public:
        ReadBufferIterator(
            std::shared_ptr<StorageS3Source::IIterator> file_iterator_,
            const StorageS3Source::KeysWithInfo & read_keys_,
            const StorageS3::Configuration & configuration_,
            std::optional<String> format_,
            const std::optional<FormatSettings> & format_settings_,
            const ContextPtr & context_)
            : WithContext(context_)
            , file_iterator(file_iterator_)
            , read_keys(read_keys_)
            , configuration(configuration_)
            , format(std::move(format_))
            , format_settings(format_settings_)
            , prev_read_keys_size(read_keys_.size())
        {
        }

        Data next() override
        {
            if (first)
            {
                /// If format is unknown we iterate through all currently read keys on first iteration and
                /// try to determine format by file name.
                if (!format)
                {
                    for (const auto & key_with_info : read_keys)
                    {
                        if (auto format_from_file_name = FormatFactory::instance().tryGetFormatFromFileName(key_with_info->key))
                        {
                            format = format_from_file_name;
                            break;
                        }
                    }
                }

                /// For default mode check cached columns for currently read keys on first iteration.
                if (first && getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT)
                {
                    if (auto cached_columns = tryGetColumnsFromCache(read_keys.begin(), read_keys.end()))
                        return {nullptr, cached_columns, format};
                }
            }

            while (true)
            {
                current_key_with_info = (*file_iterator)();

                if (!current_key_with_info || current_key_with_info->key.empty())
                {
                    if (first)
                    {
                        if (format)
                            throw Exception(
                                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                                "The table structure cannot be extracted from a {} format file, because there are no files with provided path "
                                "in S3 or all files are empty. You can specify table structure manually",
                                *format);

                        throw Exception(
                            ErrorCodes::CANNOT_DETECT_FORMAT,
                            "The data format cannot be detected by the contents of the files, because there are no files with provided path "
                            "in S3 or all files are empty. You can specify the format manually");
                    }

                    return {nullptr, std::nullopt, format};
                }

                /// S3 file iterator could get new keys after new iteration
                if (read_keys.size() > prev_read_keys_size)
                {
                    /// If format is unknown we can try to determine it by new file names.
                    if (!format)
                    {
                        for (auto it = read_keys.begin() + prev_read_keys_size; it != read_keys.end(); ++it)
                        {
                            if (auto format_from_file_name = FormatFactory::instance().tryGetFormatFromFileName((*it)->key))
                            {
                                format = format_from_file_name;
                                break;
                            }
                        }
                    }

                    /// Check new files in schema cache if schema inference mode is default.
                    if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT)
                    {
                        auto columns_from_cache = tryGetColumnsFromCache(read_keys.begin() + prev_read_keys_size, read_keys.end());
                        if (columns_from_cache)
                            return {nullptr, columns_from_cache, format};
                    }

                    prev_read_keys_size = read_keys.size();
                }

                if (getContext()->getSettingsRef().s3_skip_empty_files && current_key_with_info->info && current_key_with_info->info->size == 0)
                    continue;

                /// In union mode, check cached columns only for current key.
                if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::UNION)
                {
                    StorageS3::KeysWithInfo keys = {current_key_with_info};
                    if (auto columns_from_cache = tryGetColumnsFromCache(keys.begin(), keys.end()))
                    {
                        first = false;
                        return {nullptr, columns_from_cache, format};
                    }
                }

                int zstd_window_log_max = static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max);
                auto impl = std::make_unique<ReadBufferFromS3>(configuration.client, configuration.url.bucket, current_key_with_info->key, configuration.url.version_id, configuration.request_settings, getContext()->getReadSettings());
                if (!getContext()->getSettingsRef().s3_skip_empty_files || !impl->eof())
                {
                    first = false;
                    return {wrapReadBufferWithCompressionMethod(std::move(impl), chooseCompressionMethod(current_key_with_info->key, configuration.compression_method), zstd_window_log_max), std::nullopt, format};
                }
            }
        }

        void setNumRowsToLastFile(size_t num_rows) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_s3)
                return;

            String source = fs::path(configuration.url.uri.getHost() + std::to_string(configuration.url.uri.getPort())) / configuration.url.bucket / current_key_with_info->key;
            auto key = getKeyForSchemaCache(source, *format, format_settings, getContext());
            StorageS3::getSchemaCache(getContext()).addNumRows(key, num_rows);
        }

        void setSchemaToLastFile(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_s3
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::UNION)
                return;

            String source = fs::path(configuration.url.uri.getHost() + std::to_string(configuration.url.uri.getPort())) / configuration.url.bucket / current_key_with_info->key;
            auto cache_key = getKeyForSchemaCache(source, *format, format_settings, getContext());
            StorageS3::getSchemaCache(getContext()).addColumns(cache_key, columns);
        }

        void setResultingSchema(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_s3
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::DEFAULT)
                return;

            auto host_and_bucket = fs::path(configuration.url.uri.getHost() + std::to_string(configuration.url.uri.getPort())) / configuration.url.bucket;
            Strings sources;
            sources.reserve(read_keys.size());
            std::transform(read_keys.begin(), read_keys.end(), std::back_inserter(sources), [&](const auto & elem){ return host_and_bucket / elem->key; });
            auto cache_keys = getKeysForSchemaCache(sources, *format, format_settings, getContext());
            StorageS3::getSchemaCache(getContext()).addManyColumns(cache_keys, columns);
        }

        void setFormatName(const String & format_name) override
        {
            format = format_name;
        }

        String getLastFileName() const override
        {
            if (current_key_with_info)
                return current_key_with_info->key;
            return "";
        }

        bool supportsLastReadBufferRecreation() const override { return true; }

        std::unique_ptr<ReadBuffer> recreateLastReadBuffer() override
        {
            chassert(current_key_with_info);
            int zstd_window_log_max = static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max);
            auto impl = std::make_unique<ReadBufferFromS3>(configuration.client, configuration.url.bucket, current_key_with_info->key, configuration.url.version_id, configuration.request_settings, getContext()->getReadSettings());
            return wrapReadBufferWithCompressionMethod(std::move(impl), chooseCompressionMethod(current_key_with_info->key, configuration.compression_method), zstd_window_log_max);
        }

    private:
        std::optional<ColumnsDescription> tryGetColumnsFromCache(
            const StorageS3::KeysWithInfo::const_iterator & begin,
            const StorageS3::KeysWithInfo::const_iterator & end)
        {
            auto context = getContext();
            if (!context->getSettingsRef().schema_inference_use_cache_for_s3)
                return std::nullopt;

            auto & schema_cache = StorageS3::getSchemaCache(context);
            for (auto it = begin; it < end; ++it)
            {
                auto get_last_mod_time = [&]
                {
                    time_t last_modification_time = 0;
                    if ((*it)->info)
                    {
                        last_modification_time = (*it)->info->last_modification_time;
                    }
                    else
                    {
                        /// Note that in case of exception in getObjectInfo returned info will be empty,
                        /// but schema cache will handle this case and won't return columns from cache
                        /// because we can't say that it's valid without last modification time.
                        last_modification_time = S3::getObjectInfo(
                             *configuration.client,
                             configuration.url.bucket,
                             (*it)->key,
                             configuration.url.version_id,
                             configuration.request_settings,
                             /*with_metadata=*/ false,
                             /*for_disk_s3=*/ false,
                             /*throw_on_error= */ false).last_modification_time;
                    }

                    return last_modification_time ? std::make_optional(last_modification_time) : std::nullopt;
                };

                String path = fs::path(configuration.url.bucket) / (*it)->key;
                String source = fs::path(configuration.url.uri.getHost() + std::to_string(configuration.url.uri.getPort())) / path;

                if (format)
                {
                    auto cache_key = getKeyForSchemaCache(source, *format, format_settings, context);
                    if (auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time))
                        return columns;
                }
                else
                {
                    /// If format is unknown, we can iterate through all possible input formats
                    /// and check if we have an entry with this format and this file in schema cache.
                    /// If we have such entry for some format, we can use this format to read the file.
                    for (const auto & format_name : FormatFactory::instance().getAllInputFormats())
                    {
                        auto cache_key = getKeyForSchemaCache(source, format_name, format_settings, context);
                        if (auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time))
                        {
                            /// Now format is known. It should be the same for all files.
                            format = format_name;
                            return columns;
                        }
                    }
                }
            }

            return std::nullopt;
        }

        std::shared_ptr<StorageS3Source::IIterator> file_iterator;
        const StorageS3Source::KeysWithInfo & read_keys;
        const StorageS3::Configuration & configuration;
        std::optional<String> format;
        const std::optional<FormatSettings> & format_settings;
        StorageS3Source::KeyWithInfoPtr current_key_with_info;
        size_t prev_read_keys_size;
        bool first = true;
    };

}

std::pair<ColumnsDescription, String> StorageS3::getTableStructureAndFormatFromDataImpl(
    std::optional<String> format,
    const Configuration & configuration,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & ctx)
{
    KeysWithInfo read_keys;

    auto file_iterator = createFileIterator(configuration, false, ctx, {}, {}, &read_keys);

    ReadBufferIterator read_buffer_iterator(file_iterator, read_keys, configuration, format, format_settings, ctx);
    if (format)
        return {readSchemaFromFormat(*format, format_settings, read_buffer_iterator, ctx), *format};
    return detectFormatAndReadSchema(format_settings, read_buffer_iterator, ctx);
}

void registerStorageS3Impl(const String & name, StorageFactory & factory)
{
    factory.registerStorage(name, [](const StorageFactory::Arguments & args)
    {
        auto & engine_args = args.engine_args;
        if (engine_args.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

        auto configuration = StorageS3::getConfiguration(engine_args, args.getLocalContext());
        // Use format settings from global server context + settings from
        // the SETTINGS clause of the create query. Settings from current
        // session and user are ignored.
        std::optional<FormatSettings> format_settings;
        if (args.storage_def->settings)
        {
            FormatFactorySettings user_format_settings;

            // Apply changed settings from global context, but ignore the
            // unknown ones, because we only have the format settings here.
            const auto & changes = args.getContext()->getSettingsRef().changes();
            for (const auto & change : changes)
            {
                if (user_format_settings.has(change.name))
                    user_format_settings.set(change.name, change.value);
            }

            // Apply changes from SETTINGS clause, with validation.
            user_format_settings.applyChanges(args.storage_def->settings->changes);
            format_settings = getFormatSettings(args.getContext(), user_format_settings);
        }
        else
        {
            format_settings = getFormatSettings(args.getContext());
        }

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            partition_by = args.storage_def->partition_by->clone();

        return std::make_shared<StorageS3>(
            std::move(configuration),
            args.getContext(),
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            format_settings,
            /* distributed_processing_ */false,
            partition_by);
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessType::S3,
    });
}

void registerStorageS3(StorageFactory & factory)
{
    return registerStorageS3Impl("S3", factory);
}

void registerStorageCOS(StorageFactory & factory)
{
    return registerStorageS3Impl("COSN", factory);
}

void registerStorageOSS(StorageFactory & factory)
{
    return registerStorageS3Impl("OSS", factory);
}

bool StorageS3::supportsPartitionBy() const
{
    return true;
}

SchemaCache & StorageS3::getSchemaCache(const ContextPtr & ctx)
{
    static SchemaCache schema_cache(ctx->getConfigRef().getUInt("schema_inference_cache_max_elements_for_s3", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

}

#endif
