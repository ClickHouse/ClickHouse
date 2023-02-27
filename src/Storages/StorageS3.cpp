#include "config.h"
#include <Common/ProfileEvents.h>
#include "IO/ParallelReadBuffer.h"
#include "IO/IOThreadPool.h"
#include "Parsers/ASTCreateQuery.h"

#if USE_AWS_S3

#include <Common/isValidUTF8.h>

#include <Functions/FunctionsConversion.h>

#include <IO/S3Common.h>
#include <IO/S3/Requests.h>

#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/PartitionedSink.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/getVirtualsForStorage.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Storages/ReadFromStorageProgress.h>

#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/StoredObject.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>

#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/narrowPipe.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <DataTypes/DataTypeString.h>

#include <aws/core/auth/AWSCredentials.h>

#include <Common/parseGlobs.h>
#include <Common/quoteString.h>
#include <re2/re2.h>

#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace ProfileEvents
{
    extern const Event S3DeleteObjects;
    extern const Event S3ListObjects;
}

namespace DB
{

static const String PARTITION_ID_WILDCARD = "{_partition_id}";

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
    "filename",
    "use_environment_credentials",
    "max_single_read_retries",
    "min_upload_part_size",
    "upload_part_size_multiply_factor",
    "upload_part_size_multiply_parts_count_threshold",
    "max_single_part_upload_size",
    "max_connections",
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
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int FILE_DOESNT_EXIST;
}

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

static void addPathToVirtualColumns(Block & block, const String & path, size_t idx)
{
    if (block.has("_path"))
        block.getByName("_path").column->assumeMutableRef().insert(path);

    if (block.has("_file"))
    {
        auto pos = path.find_last_of('/');
        assert(pos != std::string::npos);

        auto file = path.substr(pos + 1);
        block.getByName("_file").column->assumeMutableRef().insert(file);
    }

    block.getByName("_idx").column->assumeMutableRef().insert(idx);
}

class StorageS3Source::DisclosedGlobIterator::Impl : WithContext
{
public:
    Impl(
        const S3::Client & client_,
        const S3::URI & globbed_uri_,
        ASTPtr & query_,
        const Block & virtual_header_,
        ContextPtr context_,
        ObjectInfos * object_infos_,
        Strings * read_keys_,
        const S3Settings::RequestSettings & request_settings_)
        : WithContext(context_)
        , client(S3::Client::create(client_))
        , globbed_uri(globbed_uri_)
        , query(query_)
        , virtual_header(virtual_header_)
        , object_infos(object_infos_)
        , read_keys(read_keys_)
        , request_settings(request_settings_)
        , list_objects_pool(1)
        , list_objects_scheduler(threadPoolCallbackRunner<ListObjectsOutcome>(list_objects_pool, "ListObjects"))
    {
        if (globbed_uri.bucket.find_first_of("*?{") != globbed_uri.bucket.npos)
            throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "Expression can not have wildcards inside bucket name");

        const String key_prefix = globbed_uri.key.substr(0, globbed_uri.key.find_first_of("*?{"));

        /// We don't have to list bucket, because there is no asterisks.
        if (key_prefix.size() == globbed_uri.key.size())
        {
            buffer.emplace_back(globbed_uri.key, std::nullopt);
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
        fillInternalBufferAssumeLocked();
    }

    KeyWithInfo next()
    {
        std::lock_guard lock(mutex);
        return nextAssumeLocked();
    }

    size_t getTotalSize() const
    {
        return total_size.load(std::memory_order_relaxed);
    }

    ~Impl()
    {
        list_objects_pool.wait();
    }

private:
    using ListObjectsOutcome = Aws::S3::Model::ListObjectsV2Outcome;

    KeyWithInfo nextAssumeLocked()
    {
        if (buffer_iter != buffer.end())
        {
            auto answer = *buffer_iter;
            ++buffer_iter;
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

        return nextAssumeLocked();
    }

    void fillInternalBufferAssumeLocked()
    {
        buffer.clear();

        assert(outcome_future.valid());
        auto outcome = outcome_future.get();

        if (!outcome.IsSuccess())
        {
            throw Exception(ErrorCodes::S3_ERROR, "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
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

                if (object_infos)
                    (*object_infos)[fs::path(globbed_uri.bucket) / key] = info;

                temp_buffer.emplace_back(std::move(key), std::move(info));
            }
        }

        if (temp_buffer.empty())
        {
            buffer_iter = buffer.begin();
            return;
        }

        if (!is_initialized)
        {
            createFilterAST(temp_buffer.front().key);
            is_initialized = true;
        }

        if (filter_ast)
        {
            auto block = virtual_header.cloneEmpty();
            for (size_t i = 0; i < temp_buffer.size(); ++i)
                addPathToVirtualColumns(block, fs::path(globbed_uri.bucket) / temp_buffer[i].key, i);

            VirtualColumnUtils::filterBlockWithQuery(query, block, getContext(), filter_ast);
            const auto & idxs = typeid_cast<const ColumnUInt64 &>(*block.getByName("_idx").column);

            buffer.reserve(block.rows());
            for (UInt64 idx : idxs.getData())
            {
                total_size.fetch_add(temp_buffer[idx].info->size, std::memory_order_relaxed);
                buffer.emplace_back(std::move(temp_buffer[idx]));
            }
        }
        else
        {
            buffer = std::move(temp_buffer);
            for (const auto & [_, info] : buffer)
                total_size.fetch_add(info->size, std::memory_order_relaxed);
        }

        /// Set iterator only after the whole batch is processed
        buffer_iter = buffer.begin();

        if (read_keys)
        {
            read_keys->reserve(read_keys->size() + buffer.size());
            for (const auto & [key, _] : buffer)
                read_keys->push_back(key);
        }
    }

    void createFilterAST(const String & any_key)
    {
        if (!query || !virtual_header)
            return;

        /// Create a virtual block with one row to construct filter
        /// Append "idx" column as the filter result
        virtual_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});

        auto block = virtual_header.cloneEmpty();
        addPathToVirtualColumns(block, fs::path(globbed_uri.bucket) / any_key, 0);
        VirtualColumnUtils::prepareFilterBlockWithQuery(query, getContext(), block, filter_ast);
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
        }, 0);
    }

    std::mutex mutex;

    KeysWithInfo buffer;
    KeysWithInfo::iterator buffer_iter;

    std::unique_ptr<S3::Client> client;
    S3::URI globbed_uri;
    ASTPtr query;
    Block virtual_header;
    bool is_initialized{false};
    ASTPtr filter_ast;
    std::unique_ptr<re2::RE2> matcher;
    bool recursive{false};
    bool is_finished{false};
    ObjectInfos * object_infos;
    Strings * read_keys;

    S3::ListObjectsV2Request request;
    S3Settings::RequestSettings request_settings;

    ThreadPool list_objects_pool;
    ThreadPoolCallbackRunner<ListObjectsOutcome> list_objects_scheduler;
    std::future<ListObjectsOutcome> outcome_future;
    std::atomic<size_t> total_size = 0;
};

StorageS3Source::DisclosedGlobIterator::DisclosedGlobIterator(
    const S3::Client & client_,
    const S3::URI & globbed_uri_,
    ASTPtr query,
    const Block & virtual_header,
    ContextPtr context,
    ObjectInfos * object_infos_,
    Strings * read_keys_,
    const S3Settings::RequestSettings & request_settings_)
    : pimpl(std::make_shared<StorageS3Source::DisclosedGlobIterator::Impl>(client_, globbed_uri_, query, virtual_header, context, object_infos_, read_keys_, request_settings_))
{
}

StorageS3Source::KeyWithInfo StorageS3Source::DisclosedGlobIterator::next()
{
    return pimpl->next();
}

size_t StorageS3Source::DisclosedGlobIterator::getTotalSize() const
{
    return pimpl->getTotalSize();
}

class StorageS3Source::KeysIterator::Impl : WithContext
{
public:
    explicit Impl(
        const S3::Client & client_,
        const std::string & version_id_,
        const std::vector<String> & keys_,
        const String & bucket_,
        const S3Settings::RequestSettings & request_settings_,
        ASTPtr query_,
        const Block & virtual_header_,
        ContextPtr context_,
        ObjectInfos * object_infos_,
        Strings * read_keys_)
        : WithContext(context_)
        , bucket(bucket_)
        , query(query_)
        , virtual_header(virtual_header_)
    {
        Strings all_keys = keys_;

        /// Create a virtual block with one row to construct filter
        if (query && virtual_header && !all_keys.empty())
        {
            /// Append "idx" column as the filter result
            virtual_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});

            auto block = virtual_header.cloneEmpty();
            addPathToVirtualColumns(block, fs::path(bucket) / all_keys.front(), 0);

            ASTPtr filter_ast;
            VirtualColumnUtils::prepareFilterBlockWithQuery(query, getContext(), block, filter_ast);

            if (filter_ast)
            {
                block = virtual_header.cloneEmpty();
                for (size_t i = 0; i < all_keys.size(); ++i)
                    addPathToVirtualColumns(block, fs::path(bucket) / all_keys[i], i);

                VirtualColumnUtils::filterBlockWithQuery(query, block, getContext(), filter_ast);
                const auto & idxs = typeid_cast<const ColumnUInt64 &>(*block.getByName("_idx").column);

                Strings filtered_keys;
                filtered_keys.reserve(block.rows());
                for (UInt64 idx : idxs.getData())
                    filtered_keys.emplace_back(std::move(all_keys[idx]));

                all_keys = std::move(filtered_keys);
            }
        }

        if (read_keys_)
            *read_keys_ = all_keys;

        for (auto && key : all_keys)
        {
            std::optional<S3::ObjectInfo> info;

            /// To avoid extra requests update total_size only if object_infos != nullptr
            /// (which means we eventually need this info anyway, so it should be ok to do it now)
            if (object_infos_)
            {
                info = S3::getObjectInfo(client_, bucket, key, version_id_, request_settings_);
                total_size += info->size;

                String path = fs::path(bucket) / key;
                (*object_infos_)[std::move(path)] = *info;
            }

            keys.emplace_back(std::move(key), std::move(info));
        }
    }

    KeyWithInfo next()
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= keys.size())
            return {};

        return keys[current_index];
    }

    size_t getTotalSize() const
    {
        return total_size;
    }

private:
    KeysWithInfo keys;
    std::atomic_size_t index = 0;

    String bucket;
    ASTPtr query;
    Block virtual_header;

    size_t total_size = 0;
};

StorageS3Source::KeysIterator::KeysIterator(
    const S3::Client & client_,
    const std::string & version_id_,
    const std::vector<String> & keys_,
    const String & bucket_,
    const S3Settings::RequestSettings & request_settings_,
    ASTPtr query,
    const Block & virtual_header,
    ContextPtr context,
    ObjectInfos * object_infos,
    Strings * read_keys)
    : pimpl(std::make_shared<StorageS3Source::KeysIterator::Impl>(
        client_, version_id_, keys_, bucket_, request_settings_,
        query, virtual_header, context, object_infos, read_keys))
{
}

StorageS3Source::KeyWithInfo StorageS3Source::KeysIterator::next()
{
    return pimpl->next();
}

size_t StorageS3Source::KeysIterator::getTotalSize() const
{
    return pimpl->getTotalSize();
}

Block StorageS3Source::getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns)
{
    for (const auto & virtual_column : requested_virtual_columns)
        sample_block.insert({virtual_column.type->createColumn(), virtual_column.type, virtual_column.name});

    return sample_block;
}

StorageS3Source::StorageS3Source(
    const std::vector<NameAndTypePair> & requested_virtual_columns_,
    const String & format_,
    String name_,
    const Block & sample_block_,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    const ColumnsDescription & columns_,
    UInt64 max_block_size_,
    const S3Settings::RequestSettings & request_settings_,
    String compression_hint_,
    const std::shared_ptr<const S3::Client> & client_,
    const String & bucket_,
    const String & version_id_,
    std::shared_ptr<IIterator> file_iterator_,
    const size_t download_thread_num_)
    : ISource(getHeader(sample_block_, requested_virtual_columns_))
    , WithContext(context_)
    , name(std::move(name_))
    , bucket(bucket_)
    , version_id(version_id_)
    , format(format_)
    , columns_desc(columns_)
    , max_block_size(max_block_size_)
    , request_settings(request_settings_)
    , compression_hint(std::move(compression_hint_))
    , client(client_)
    , sample_block(sample_block_)
    , format_settings(format_settings_)
    , requested_virtual_columns(requested_virtual_columns_)
    , file_iterator(file_iterator_)
    , download_thread_num(download_thread_num_)
    , create_reader_pool(1)
    , create_reader_scheduler(threadPoolCallbackRunner<ReaderHolder>(create_reader_pool, "CreateS3Reader"))
{
    reader = createReader();
    if (reader)
        reader_future = createReaderAsync();
}

StorageS3Source::ReaderHolder StorageS3Source::createReader()
{
    auto [current_key, info] = (*file_iterator)();
    if (current_key.empty())
        return {};

    size_t object_size = info ? info->size : S3::getObjectSize(*client, bucket, current_key, version_id, request_settings);

    int zstd_window_log_max = static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max);
    auto read_buf = wrapReadBufferWithCompressionMethod(
        createS3ReadBuffer(current_key, object_size),
        chooseCompressionMethod(current_key, compression_hint),
        zstd_window_log_max);

    auto input_format = getContext()->getInputFormat(format, *read_buf, sample_block, max_block_size, format_settings);
    QueryPipelineBuilder builder;
    builder.init(Pipe(input_format));

    if (columns_desc.hasDefaults())
    {
        builder.addSimpleTransform(
            [&](const Block & header)
            { return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext()); });
    }

    auto pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    auto current_reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    return ReaderHolder{fs::path(bucket) / current_key, std::move(read_buf), std::move(pipeline), std::move(current_reader)};
}

std::future<StorageS3Source::ReaderHolder> StorageS3Source::createReaderAsync()
{
    return create_reader_scheduler([this] { return createReader(); }, 0);
}

std::unique_ptr<ReadBuffer> StorageS3Source::createS3ReadBuffer(const String & key, size_t object_size)
{
    auto read_settings = getContext()->getReadSettings().adjustBufferSize(object_size);
    read_settings.enable_filesystem_cache = false;

    auto download_buffer_size = getContext()->getSettings().max_download_buffer_size;
    const bool use_parallel_download = download_buffer_size > 0 && download_thread_num > 1;
    const bool object_too_small = object_size < download_thread_num * download_buffer_size;

    if (!use_parallel_download || object_too_small)
    {
        LOG_TRACE(log, "Downloading object of size {} from S3 in single thread", object_size);
        if (read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
            return createAsyncS3ReadBuffer(key, read_settings, object_size);

        return std::make_unique<ReadBufferFromS3>(client, bucket, key, version_id, request_settings, read_settings);
    }

    assert(object_size > 0);
    if (download_buffer_size < DBMS_DEFAULT_BUFFER_SIZE)
    {
        LOG_WARNING(log, "Downloading buffer {} bytes too small, set at least {} bytes", download_buffer_size, DBMS_DEFAULT_BUFFER_SIZE);
        download_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    }

    auto factory = std::make_unique<ReadBufferS3Factory>(
        client, bucket, key, version_id, download_buffer_size, object_size, request_settings, read_settings);

    LOG_TRACE(log,
        "Downloading from S3 in {} threads. Object size: {}, Range size: {}.",
        download_thread_num, object_size, download_buffer_size);

    return std::make_unique<ParallelReadBuffer>(std::move(factory), threadPoolCallbackRunner<void>(IOThreadPool::get(), "S3ParallelRead"), download_thread_num);
}

std::unique_ptr<ReadBuffer> StorageS3Source::createAsyncS3ReadBuffer(
    const String & key, const ReadSettings & read_settings, size_t object_size)
{
    auto read_buffer_creator =
        [this, read_settings]
        (const std::string & path, size_t read_until_position) -> std::shared_ptr<ReadBufferFromFileBase>
    {
        return std::make_shared<ReadBufferFromS3>(
            client,
            bucket,
            path,
            version_id,
            request_settings,
            read_settings,
            /* use_external_buffer */true,
            /* offset */0,
            read_until_position,
            /* restricted_seek */true);
    };

    auto s3_impl = std::make_unique<ReadBufferFromRemoteFSGather>(
        std::move(read_buffer_creator),
        StoredObjects{StoredObject{key, object_size}},
        read_settings);

    auto & pool_reader = getContext()->getThreadPoolReader(Context::FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
    auto async_reader = std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(pool_reader, read_settings, std::move(s3_impl));

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

            const auto & file_path = reader.getPath();
            size_t total_size = file_iterator->getTotalSize();
            if (num_rows && total_size)
            {
                updateRowsProgressApprox(
                    *this, chunk, total_size, total_rows_approx_accumulated, total_rows_count_times, total_rows_approx_max);
            }

            for (const auto & virtual_column : requested_virtual_columns)
            {
                if (virtual_column.name == "_path")
                {
                    chunk.addColumn(virtual_column.type->createColumnConst(num_rows, file_path)->convertToFullColumnIfConst());
                }
                else if (virtual_column.name == "_file")
                {
                    size_t last_slash_pos = file_path.find_last_of('/');
                    auto column = virtual_column.type->createColumnConst(num_rows, file_path.substr(last_slash_pos + 1));
                    chunk.addColumn(column->convertToFullColumnIfConst());
                }
            }

            return chunk;
        }


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

class StorageS3Sink : public SinkToStorage
{
public:
    StorageS3Sink(
        const String & format,
        const Block & sample_block_,
        ContextPtr context,
        std::optional<FormatSettings> format_settings_,
        const CompressionMethod compression_method,
        const StorageS3::Configuration & s3_configuration_,
        const String & bucket,
        const String & key)
        : SinkToStorage(sample_block_)
        , sample_block(sample_block_)
        , format_settings(format_settings_)
    {
        write_buf = wrapWriteBufferWithCompressionMethod(
            std::make_unique<WriteBufferFromS3>(
                s3_configuration_.client,
                bucket,
                key,
                s3_configuration_.request_settings,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                threadPoolCallbackRunner<void>(IOThreadPool::get(), "S3ParallelRead"),
                context->getWriteSettings()),
            compression_method,
            3);
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

    void onException() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
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
            writer.reset();
            throw;
        }
    }

    Block sample_block;
    std::optional<FormatSettings> format_settings;
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    bool cancelled = false;
    std::mutex cancel_mutex;
};


class PartitionedStorageS3Sink : public PartitionedSink
{
public:
    PartitionedStorageS3Sink(
        const ASTPtr & partition_by,
        const String & format_,
        const Block & sample_block_,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        const CompressionMethod compression_method_,
        const StorageS3::Configuration & s3_configuration_,
        const String & bucket_,
        const String & key_)
        : PartitionedSink(partition_by, context_, sample_block_)
        , format(format_)
        , sample_block(sample_block_)
        , context(context_)
        , compression_method(compression_method_)
        , s3_configuration(s3_configuration_)
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

        return std::make_shared<StorageS3Sink>(
            format,
            sample_block,
            context,
            format_settings,
            compression_method,
            s3_configuration,
            partition_bucket,
            partition_key
        );
    }

private:
    const String format;
    const Block sample_block;
    ContextPtr context;
    const CompressionMethod compression_method;
    const StorageS3::Configuration & s3_configuration;
    const String bucket;
    const String key;
    std::optional<FormatSettings> format_settings;

    ExpressionActionsPtr partition_by_expr;

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
    const StorageS3::Configuration & configuration_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , s3_configuration{configuration_}
    , keys({s3_configuration.url.key})
    , format_name(configuration_.format)
    , compression_method(configuration_.compression_method)
    , name(s3_configuration.url.storage_name)
    , distributed_processing(distributed_processing_)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
    , is_key_with_globs(s3_configuration.url.key.find_first_of("*?{") != std::string::npos)
{
    FormatFactory::instance().checkFormatName(format_name);
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(s3_configuration.url.uri);
    StorageInMemoryMetadata storage_metadata;

    updateConfiguration(context_, s3_configuration);
    if (columns_.empty())
    {
        auto columns = getTableStructureFromDataImpl(
            format_name,
            s3_configuration,
            compression_method,
            is_key_with_globs,
            format_settings,
            context_,
            &object_infos);

        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    auto columns = storage_metadata.getSampleBlock().getNamesAndTypesList();
    virtual_columns = getVirtualsForStorage(columns, default_virtuals);
    for (const auto & column : virtual_columns)
        virtual_block.insert({column.type->createColumn(), column.type, column.name});
}

std::shared_ptr<StorageS3Source::IIterator> StorageS3::createFileIterator(
    const Configuration & s3_configuration,
    const std::vector<String> & keys,
    bool is_key_with_globs,
    bool distributed_processing,
    ContextPtr local_context,
    ASTPtr query,
    const Block & virtual_block,
    ObjectInfos * object_infos,
    Strings * read_keys)
{
    if (distributed_processing)
    {
        return std::make_shared<StorageS3Source::ReadTaskIterator>(local_context->getReadTaskCallback());
    }
    else if (is_key_with_globs)
    {
        /// Iterate through disclosed globs and make a source for each file
        return std::make_shared<StorageS3Source::DisclosedGlobIterator>(
            *s3_configuration.client, s3_configuration.url, query, virtual_block,
            local_context, object_infos, read_keys, s3_configuration.request_settings);
    }
    else
    {
        return std::make_shared<StorageS3Source::KeysIterator>(
            *s3_configuration.client, s3_configuration.url.version_id, keys,
            s3_configuration.url.bucket, s3_configuration.request_settings, query, virtual_block, local_context,
            object_infos, read_keys);
    }
}

bool StorageS3::supportsSubcolumns() const
{
    return FormatFactory::instance().checkIfFormatSupportsSubcolumns(format_name);
}

bool StorageS3::supportsSubsetOfColumns() const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name);
}

Pipe StorageS3::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    bool has_wildcards = s3_configuration.url.bucket.find(PARTITION_ID_WILDCARD) != String::npos
        || keys.back().find(PARTITION_ID_WILDCARD) != String::npos;
    if (partition_by && has_wildcards)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading from a partitioned S3 storage is not implemented yet");

    updateConfiguration(local_context, s3_configuration);

    Pipes pipes;

    std::unordered_set<String> column_names_set(column_names.begin(), column_names.end());
    std::vector<NameAndTypePair> requested_virtual_columns;

    for (const auto & virtual_column : getVirtuals())
    {
        if (column_names_set.contains(virtual_column.name))
            requested_virtual_columns.push_back(virtual_column);
    }

    std::shared_ptr<StorageS3Source::IIterator> iterator_wrapper = createFileIterator(
        s3_configuration,
        keys,
        is_key_with_globs,
        distributed_processing,
        local_context,
        query_info.query,
        virtual_block,
        &object_infos);

    ColumnsDescription columns_description;
    Block block_for_format;
    if (supportsSubsetOfColumns())
    {
        auto fetch_columns = column_names;
        const auto & virtuals = getVirtuals();
        std::erase_if(
            fetch_columns,
            [&](const String & col)
            { return std::any_of(virtuals.begin(), virtuals.end(), [&](const NameAndTypePair & virtual_col){ return col == virtual_col.name; }); });

        if (fetch_columns.empty())
            fetch_columns.push_back(ExpressionActions::getSmallestColumn(storage_snapshot->metadata->getColumns().getAllPhysical()).name);

        columns_description = storage_snapshot->getDescriptionForColumns(fetch_columns);
        block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    }
    else
    {
        columns_description = storage_snapshot->metadata->getColumns();
        block_for_format = storage_snapshot->metadata->getSampleBlock();
    }

    const size_t max_download_threads = local_context->getSettingsRef().max_download_threads;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageS3Source>(
            requested_virtual_columns,
            format_name,
            getName(),
            block_for_format,
            local_context,
            format_settings,
            columns_description,
            max_block_size,
            s3_configuration.request_settings,
            compression_method,
            s3_configuration.client,
            s3_configuration.url.bucket,
            s3_configuration.url.version_id,
            iterator_wrapper,
            max_download_threads));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    narrowPipe(pipe, num_streams);
    return pipe;
}

SinkToStoragePtr StorageS3::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    updateConfiguration(local_context, s3_configuration);

    auto sample_block = metadata_snapshot->getSampleBlock();
    auto chosen_compression_method = chooseCompressionMethod(keys.back(), compression_method);
    bool has_wildcards = s3_configuration.url.bucket.find(PARTITION_ID_WILDCARD) != String::npos || keys.back().find(PARTITION_ID_WILDCARD) != String::npos;
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query);

    auto partition_by_ast = insert_query ? (insert_query->partition_by ? insert_query->partition_by : partition_by) : nullptr;
    bool is_partitioned_implementation = partition_by_ast && has_wildcards;

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedStorageS3Sink>(
            partition_by_ast,
            format_name,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            s3_configuration,
            s3_configuration.url.bucket,
            keys.back());
    }
    else
    {
        if (is_key_with_globs)
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                            "S3 key '{}' contains globs, so the table is in readonly mode", s3_configuration.url.key);

        bool truncate_in_insert = local_context->getSettingsRef().s3_truncate_on_insert;

        if (!truncate_in_insert && S3::objectExists(*s3_configuration.client, s3_configuration.url.bucket, keys.back(), s3_configuration.url.version_id, s3_configuration.request_settings))
        {
            if (local_context->getSettingsRef().s3_create_new_file_on_insert)
            {
                size_t index = keys.size();
                auto pos = keys[0].find_first_of('.');
                String new_key;
                do
                {
                    new_key = keys[0].substr(0, pos) + "." + std::to_string(index) + (pos == std::string::npos ? "" : keys[0].substr(pos));
                    ++index;
                }
                while (S3::objectExists(*s3_configuration.client, s3_configuration.url.bucket, new_key, s3_configuration.url.version_id, s3_configuration.request_settings));
                keys.push_back(new_key);
            }
            else
                throw Exception(
                                ErrorCodes::BAD_ARGUMENTS,
                                "Object in bucket {} with key {} already exists. "
                                "If you want to overwrite it, enable setting s3_truncate_on_insert, if you "
                                "want to create a new file on each insert, enable setting s3_create_new_file_on_insert",
                                s3_configuration.url.bucket,
                                keys.back());
        }

        return std::make_shared<StorageS3Sink>(
            format_name,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            s3_configuration,
            s3_configuration.url.bucket,
            keys.back());
    }
}

void StorageS3::truncate(const ASTPtr & /* query */, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    updateConfiguration(local_context, s3_configuration);

    if (is_key_with_globs)
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                            "S3 key '{}' contains globs, so the table is in readonly mode", s3_configuration.url.key);

    Aws::S3::Model::Delete delkeys;

    for (const auto & key : keys)
    {
        Aws::S3::Model::ObjectIdentifier obj;
        obj.SetKey(key);
        delkeys.AddObjects(std::move(obj));
    }

    ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
    S3::DeleteObjectsRequest request;
    request.SetBucket(s3_configuration.url.bucket);
    request.SetDelete(delkeys);

    auto response = s3_configuration.client->DeleteObjects(request);
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw Exception(ErrorCodes::S3_ERROR, "{}: {}", std::to_string(static_cast<int>(err.GetErrorType())), err.GetMessage());
    }

    for (const auto & error : response.GetResult().GetErrors())
        LOG_WARNING(&Poco::Logger::get("StorageS3"), "Failed to delete {}, error: {}", error.GetKey(), error.GetMessage());
}


StorageS3::Configuration StorageS3::updateConfiguration(ContextPtr local_context, const StorageS3::Configuration & configuration)
{
    StorageS3::Configuration new_configuration(configuration);
    updateConfiguration(local_context, new_configuration);
    return new_configuration;
}

void StorageS3::updateConfiguration(ContextPtr ctx, StorageS3::Configuration & upd)
{
    auto settings = ctx->getStorageS3Settings().getSettings(upd.url.uri.toString());
    upd.request_settings = settings.request_settings;
    upd.request_settings.updateFromSettings(ctx->getSettings());

    if (upd.client)
    {
        if (upd.static_configuration)
            return;

        if (settings.auth_settings == upd.auth_settings)
            return;
    }

    upd.auth_settings.updateFrom(settings.auth_settings);

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        upd.auth_settings.region,
        ctx->getRemoteHostFilter(),
        static_cast<unsigned>(ctx->getGlobalContext()->getSettingsRef().s3_max_redirects),
        ctx->getGlobalContext()->getSettingsRef().enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        upd.request_settings.get_request_throttler,
        upd.request_settings.put_request_throttler);

    client_configuration.endpointOverride = upd.url.endpoint;
    client_configuration.maxConnections = static_cast<unsigned>(upd.request_settings.max_connections);

    auto credentials = Aws::Auth::AWSCredentials(upd.auth_settings.access_key_id, upd.auth_settings.secret_access_key);
    auto headers = upd.auth_settings.headers;
    if (!upd.headers_from_ast.empty())
        headers.insert(headers.end(), upd.headers_from_ast.begin(), upd.headers_from_ast.end());

    upd.client = S3::ClientFactory::instance().create(
        client_configuration,
        upd.url.is_virtual_hosted_style,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        upd.auth_settings.server_side_encryption_customer_key_base64,
        std::move(headers),
        upd.auth_settings.use_environment_credentials.value_or(ctx->getConfigRef().getBool("s3.use_environment_credentials", false)),
        upd.auth_settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)));
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
    configuration.auth_settings.use_environment_credentials = collection.getOrDefault<UInt64>("use_environment_credentials", 0);

    configuration.format = collection.getOrDefault<String>("format", "auto");
    configuration.compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    configuration.structure = collection.getOrDefault<String>("structure", "auto");

    configuration.request_settings = S3Settings::RequestSettings(collection);
}

StorageS3::Configuration StorageS3::getConfiguration(ASTs & engine_args, ContextPtr local_context, bool get_format_from_file)
{
    StorageS3::Configuration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args))
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
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format')
        /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')
        /// with optional headers() function

        if (engine_args.empty() || engine_args.size() > 5)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage S3 requires 1 to 5 arguments: "
                            "url, [access_key_id, secret_access_key], name of used format and [compression_method]");

        auto * header_it = StorageURL::collectHeaders(engine_args, configuration.headers_from_ast, local_context);
        if (header_it != engine_args.end())
            engine_args.erase(header_it);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, local_context);

        /// Size -> argument indexes
        static std::unordered_map<size_t, std::unordered_map<std::string_view, size_t>> size_to_engine_args
        {
            {1, {{}}},
            {2, {{"format", 1}}},
            {4, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}}},
            {5, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"compression_method", 4}}}
        };

        std::unordered_map<std::string_view, size_t> engine_args_to_idx;
        /// For 3 arguments we support 2 possible variants:
        /// s3(source, format, compression_method) and s3(source, access_key_id, access_key_id)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        if (engine_args.size() == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "format/access_key_id");
            if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
                engine_args_to_idx = {{"format", 1}, {"compression_method", 2}};

            else
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
        }
        else
        {
            engine_args_to_idx = size_to_engine_args[engine_args.size()];
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
    }

    configuration.static_configuration = !configuration.auth_settings.access_key_id.empty();

    if (configuration.format == "auto" && get_format_from_file)
        configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.url.key, true);

    return configuration;
}

ColumnsDescription StorageS3::getTableStructureFromData(
    StorageS3::Configuration & configuration,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr ctx,
    ObjectInfos * object_infos)
{
    updateConfiguration(ctx, configuration);
    return getTableStructureFromDataImpl(
        configuration.format, configuration, configuration.compression_method,
        configuration.url.key.find_first_of("*?{") != std::string::npos, format_settings, ctx, object_infos);
}

ColumnsDescription StorageS3::getTableStructureFromDataImpl(
    const String & format,
    const Configuration & s3_configuration,
    const String & compression_method,
    bool is_key_with_globs,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr ctx,
    ObjectInfos * object_infos)
{
    std::vector<String> read_keys;

    auto file_iterator = createFileIterator(
        s3_configuration,
        {s3_configuration.url.key},
        is_key_with_globs,
        false,
        ctx, nullptr,
        {}, object_infos, &read_keys);

    std::optional<ColumnsDescription> columns_from_cache;
    size_t prev_read_keys_size = read_keys.size();
    if (ctx->getSettingsRef().schema_inference_use_cache_for_s3)
        columns_from_cache = tryGetColumnsFromCache(read_keys.begin(), read_keys.end(), s3_configuration, object_infos, format, format_settings, ctx);

    ReadBufferIterator read_buffer_iterator = [&, first = true](ColumnsDescription & cached_columns) mutable -> std::unique_ptr<ReadBuffer>
    {
        auto [key, _] = (*file_iterator)();

        if (key.empty())
        {
            if (first)
                throw Exception(
                    ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                    "Cannot extract table structure from {} format file, because there are no files with provided path "
                    "in S3. You must specify table structure manually", format);

            return nullptr;
        }

        /// S3 file iterator could get new keys after new iteration, check them in schema cache.
        if (ctx->getSettingsRef().schema_inference_use_cache_for_s3 && read_keys.size() > prev_read_keys_size)
        {
            columns_from_cache = tryGetColumnsFromCache(read_keys.begin() + prev_read_keys_size, read_keys.end(), s3_configuration, object_infos, format, format_settings, ctx);
            prev_read_keys_size = read_keys.size();
            if (columns_from_cache)
            {
                cached_columns = *columns_from_cache;
                return nullptr;
            }
        }

        first = false;
        int zstd_window_log_max = static_cast<int>(ctx->getSettingsRef().zstd_window_log_max);
        return wrapReadBufferWithCompressionMethod(
            std::make_unique<ReadBufferFromS3>(
                s3_configuration.client, s3_configuration.url.bucket, key, s3_configuration.url.version_id, s3_configuration.request_settings, ctx->getReadSettings()),
            chooseCompressionMethod(key, compression_method),
            zstd_window_log_max);
    };

    ColumnsDescription columns;
    if (columns_from_cache)
        columns = *columns_from_cache;
    else
        columns = readSchemaFromFormat(format, format_settings, read_buffer_iterator, is_key_with_globs, ctx);

    if (ctx->getSettingsRef().schema_inference_use_cache_for_s3)
        addColumnsToCache(read_keys, s3_configuration, columns, format, format_settings, ctx);

    return columns;
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
            configuration,
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
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

NamesAndTypesList StorageS3::getVirtuals() const
{
    return virtual_columns;
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

std::optional<ColumnsDescription> StorageS3::tryGetColumnsFromCache(
    const Strings::const_iterator & begin,
    const Strings::const_iterator & end,
    const Configuration & s3_configuration,
    ObjectInfos * object_infos,
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & ctx)
{
    auto & schema_cache = getSchemaCache(ctx);
    for (auto it = begin; it < end; ++it)
    {
        String path = fs::path(s3_configuration.url.bucket) / *it;
        auto get_last_mod_time = [&]() -> std::optional<time_t>
        {
            S3::ObjectInfo info;
            /// Check if we already have information about this object.
            /// If no, request it and remember for possible future usage.
            if (object_infos && object_infos->contains(path))
                info = (*object_infos)[path];
            else
            {
                /// Note that in case of exception in getObjectInfo returned info will be empty,
                /// but schema cache will handle this case and won't return columns from cache
                /// because we can't say that it's valid without last modification time.
                info = S3::getObjectInfo(*s3_configuration.client, s3_configuration.url.bucket, *it, s3_configuration.url.version_id, s3_configuration.request_settings,
                                         {}, {}, /* throw_on_error= */ false);
                if (object_infos)
                    (*object_infos)[path] = info;
            }

            if (info.last_modification_time)
                return info.last_modification_time;

            return std::nullopt;
        };

        String source = fs::path(s3_configuration.url.uri.getHost() + std::to_string(s3_configuration.url.uri.getPort())) / path;
        auto cache_key = getKeyForSchemaCache(source, format_name, format_settings, ctx);
        auto columns = schema_cache.tryGet(cache_key, get_last_mod_time);
        if (columns)
            return columns;
    }

    return std::nullopt;
}

void StorageS3::addColumnsToCache(
    const Strings & keys,
    const Configuration & s3_configuration,
    const ColumnsDescription & columns,
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & ctx)
{
    auto host_and_bucket = fs::path(s3_configuration.url.uri.getHost() + std::to_string(s3_configuration.url.uri.getPort())) / s3_configuration.url.bucket;
    Strings sources;
    sources.reserve(keys.size());
    std::transform(keys.begin(), keys.end(), std::back_inserter(sources), [&](const String & key){ return host_and_bucket / key; });
    auto cache_keys = getKeysForSchemaCache(sources, format_name, format_settings, ctx);
    auto & schema_cache = getSchemaCache(ctx);
    schema_cache.addMany(cache_keys, columns);
}

}

#endif
