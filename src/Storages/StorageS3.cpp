#include <Common/config.h>
#include "IO/ParallelReadBuffer.h"
#include "IO/IOThreadPool.h"
#include "Parsers/ASTCreateQuery.h"

#if USE_AWS_S3

#include <Common/isValidUTF8.h>

#include <Functions/FunctionsConversion.h>

#include <IO/S3Common.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/threadPoolCallbackRunner.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/PartitionedSink.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/getVirtualsForStorage.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>

#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/narrowPipe.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <DataTypes/DataTypeString.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>

#include <Common/parseGlobs.h>
#include <Common/quoteString.h>
#include <re2/re2.h>

#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <filesystem>

namespace fs = std::filesystem;


static const String PARTITION_ID_WILDCARD = "{_partition_id}";

namespace DB
{

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
}

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

class StorageS3Source::DisclosedGlobIterator::Impl : WithContext
{

public:
    Impl(
        const Aws::S3::S3Client & client_,
        const S3::URI & globbed_uri_,
        ASTPtr & query_,
        const Block & virtual_header_,
        ContextPtr context_)
        : WithContext(context_)
        , client(client_)
        , globbed_uri(globbed_uri_)
        , query(query_)
        , virtual_header(virtual_header_)
    {
        if (globbed_uri.bucket.find_first_of("*?{") != globbed_uri.bucket.npos)
            throw Exception("Expression can not have wildcards inside bucket name", ErrorCodes::UNEXPECTED_EXPRESSION);

        const String key_prefix = globbed_uri.key.substr(0, globbed_uri.key.find_first_of("*?{"));

        /// We don't have to list bucket, because there is no asterisks.
        if (key_prefix.size() == globbed_uri.key.size())
        {
            buffer.emplace_back(globbed_uri.key);
            buffer_iter = buffer.begin();
            is_finished = true;
            return;
        }

        /// Create a virtual block with one row to construct filter
        if (query && virtual_header)
        {
            /// Append "key" column as the filter result
            virtual_header.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "_key"});

            auto block = virtual_header.cloneEmpty();
            MutableColumns columns = block.mutateColumns();
            for (auto & column : columns)
                column->insertDefault();
            block.setColumns(std::move(columns));
            VirtualColumnUtils::prepareFilterBlockWithQuery(query, getContext(), block, filter_ast);
        }

        request.SetBucket(globbed_uri.bucket);
        request.SetPrefix(key_prefix);
        matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(globbed_uri.key));
        fillInternalBufferAssumeLocked();
    }

    String next()
    {
        std::lock_guard lock(mutex);
        return nextAssumeLocked();
    }

private:

    String nextAssumeLocked()
    {
        if (buffer_iter != buffer.end())
        {
            auto answer = *buffer_iter;
            ++buffer_iter;
            return answer;
        }

        if (is_finished)
            return {};

        fillInternalBufferAssumeLocked();

        return nextAssumeLocked();
    }

    void fillInternalBufferAssumeLocked()
    {
        buffer.clear();

        outcome = client.ListObjectsV2(request);
        if (!outcome.IsSuccess())
            throw Exception(ErrorCodes::S3_ERROR, "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
                            quoteString(request.GetBucket()), quoteString(request.GetPrefix()),
                            backQuote(outcome.GetError().GetExceptionName()), quoteString(outcome.GetError().GetMessage()));

        const auto & result_batch = outcome.GetResult().GetContents();

        if (filter_ast)
        {
            auto block = virtual_header.cloneEmpty();
            MutableColumnPtr path_column;
            MutableColumnPtr file_column;
            MutableColumnPtr key_column = block.getByName("_key").column->assumeMutable();

            if (block.has("_path"))
                path_column = block.getByName("_path").column->assumeMutable();

            if (block.has("_file"))
                file_column = block.getByName("_file").column->assumeMutable();

            for (const auto & row : result_batch)
            {
                const String & key = row.GetKey();
                if (re2::RE2::FullMatch(key, *matcher))
                {
                    String path = fs::path(globbed_uri.bucket) / key;
                    String file = path.substr(path.find_last_of('/') + 1);
                    if (path_column)
                        path_column->insert(path);
                    if (file_column)
                        file_column->insert(file);
                    key_column->insert(key);
                }
            }

            VirtualColumnUtils::filterBlockWithQuery(query, block, getContext(), filter_ast);
            const ColumnString & keys = typeid_cast<const ColumnString &>(*block.getByName("_key").column);
            size_t rows = block.rows();
            buffer.reserve(rows);
            for (size_t i = 0; i < rows; ++i)
                buffer.emplace_back(keys.getDataAt(i).toString());
        }
        else
        {
            buffer.reserve(result_batch.size());
            for (const auto & row : result_batch)
            {
                String key = row.GetKey();
                if (re2::RE2::FullMatch(key, *matcher))
                    buffer.emplace_back(std::move(key));
            }
        }

        /// Set iterator only after the whole batch is processed
        buffer_iter = buffer.begin();

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

        /// It returns false when all objects were returned
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    std::mutex mutex;
    Strings buffer;
    Strings::iterator buffer_iter;
    Aws::S3::S3Client client;
    S3::URI globbed_uri;
    ASTPtr query;
    Block virtual_header;
    ASTPtr filter_ast;
    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;
    std::unique_ptr<re2::RE2> matcher;
    bool is_finished{false};
};

StorageS3Source::DisclosedGlobIterator::DisclosedGlobIterator(
    const Aws::S3::S3Client & client_,
    const S3::URI & globbed_uri_,
    ASTPtr query,
    const Block & virtual_header,
    ContextPtr context)
    : pimpl(std::make_shared<StorageS3Source::DisclosedGlobIterator::Impl>(client_, globbed_uri_, query, virtual_header, context))
{
}

String StorageS3Source::DisclosedGlobIterator::next()
{
    return pimpl->next();
}

class StorageS3Source::KeysIterator::Impl : WithContext
{
public:
    explicit Impl(
        const std::vector<String> & keys_, const String & bucket_, ASTPtr query_, const Block & virtual_header_, ContextPtr context_)
        : WithContext(context_), keys(keys_), bucket(bucket_), query(query_), virtual_header(virtual_header_)
    {
        /// Create a virtual block with one row to construct filter
        if (query && virtual_header)
        {
            /// Append "key" column as the filter result
            virtual_header.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "_key"});

            auto block = virtual_header.cloneEmpty();
            MutableColumns columns = block.mutateColumns();
            for (auto & column : columns)
                column->insertDefault();
            block.setColumns(std::move(columns));

            ASTPtr filter_ast;
            VirtualColumnUtils::prepareFilterBlockWithQuery(query, getContext(), block, filter_ast);

            if (filter_ast)
            {
                block = virtual_header.cloneEmpty();
                MutableColumnPtr path_column;
                MutableColumnPtr file_column;
                MutableColumnPtr key_column = block.getByName("_key").column->assumeMutable();

                if (block.has("_path"))
                    path_column = block.getByName("_path").column->assumeMutable();

                if (block.has("_file"))
                    file_column = block.getByName("_file").column->assumeMutable();

                for (const auto & key : keys)
                {
                    String path = fs::path(bucket) / key;
                    String file = path.substr(path.find_last_of('/') + 1);
                    if (path_column)
                        path_column->insert(path);
                    if (file_column)
                        file_column->insert(file);
                    key_column->insert(key);
                }

                VirtualColumnUtils::filterBlockWithQuery(query, block, getContext(), filter_ast);
                const ColumnString & keys_col = typeid_cast<const ColumnString &>(*block.getByName("_key").column);
                size_t rows = block.rows();
                Strings filtered_keys;
                filtered_keys.reserve(rows);
                for (size_t i = 0; i < rows; ++i)
                    filtered_keys.emplace_back(keys_col.getDataAt(i).toString());

                keys = std::move(filtered_keys);
            }
        }
    }

    String next()
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= keys.size())
            return "";
        return keys[current_index];
    }

private:
    Strings keys;
    std::atomic_size_t index = 0;

    String bucket;
    ASTPtr query;
    Block virtual_header;
};

StorageS3Source::KeysIterator::KeysIterator(
    const std::vector<String> & keys_, const String & bucket_, ASTPtr query, const Block & virtual_header, ContextPtr context)
    : pimpl(std::make_shared<StorageS3Source::KeysIterator::Impl>(keys_, bucket_, query, virtual_header, context))
{
}

String StorageS3Source::KeysIterator::next()
{
    return pimpl->next();
}

class StorageS3Source::ReadTasksIterator::Impl
{
public:
    explicit Impl(const std::vector<String> & read_tasks_, const ReadTaskCallback & new_read_tasks_callback_)
        : read_tasks(read_tasks_), new_read_tasks_callback(new_read_tasks_callback_)
    {
    }

    String next()
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= read_tasks.size())
            return new_read_tasks_callback();
        return read_tasks[current_index];
    }

private:
    std::atomic_size_t index = 0;
    std::vector<String> read_tasks;
    ReadTaskCallback new_read_tasks_callback;
};

StorageS3Source::ReadTasksIterator::ReadTasksIterator(
    const std::vector<String> & read_tasks_, const ReadTaskCallback & new_read_tasks_callback_)
    : pimpl(std::make_shared<StorageS3Source::ReadTasksIterator::Impl>(read_tasks_, new_read_tasks_callback_))
{
}

String StorageS3Source::ReadTasksIterator::next()
{
    return pimpl->next();
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
    UInt64 max_single_read_retries_,
    String compression_hint_,
    const std::shared_ptr<const Aws::S3::S3Client> & client_,
    const String & bucket_,
    const String & version_id_,
    std::shared_ptr<IteratorWrapper> file_iterator_,
    const size_t download_thread_num_)
    : ISource(getHeader(sample_block_, requested_virtual_columns_))
    , WithContext(context_)
    , name(std::move(name_))
    , bucket(bucket_)
    , version_id(version_id_)
    , format(format_)
    , columns_desc(columns_)
    , max_block_size(max_block_size_)
    , max_single_read_retries(max_single_read_retries_)
    , compression_hint(std::move(compression_hint_))
    , client(client_)
    , sample_block(sample_block_)
    , format_settings(format_settings_)
    , requested_virtual_columns(requested_virtual_columns_)
    , file_iterator(file_iterator_)
    , download_thread_num(download_thread_num_)
{
    initialize();
}


void StorageS3Source::onCancel()
{
    std::lock_guard lock(reader_mutex);
    if (reader)
        reader->cancel();
}


bool StorageS3Source::initialize()
{
    String current_key = (*file_iterator)();
    if (current_key.empty())
        return false;

    file_path = fs::path(bucket) / current_key;

    read_buf = wrapReadBufferWithCompressionMethod(createS3ReadBuffer(current_key), chooseCompressionMethod(current_key, compression_hint));

    auto input_format = getContext()->getInputFormat(format, *read_buf, sample_block, max_block_size, format_settings);
    QueryPipelineBuilder builder;
    builder.init(Pipe(input_format));

    if (columns_desc.hasDefaults())
    {
        builder.addSimpleTransform(
            [&](const Block & header)
            { return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext()); });
    }

    pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    return true;
}

std::unique_ptr<ReadBuffer> StorageS3Source::createS3ReadBuffer(const String & key)
{
    const size_t object_size = DB::S3::getObjectSize(client, bucket, key, version_id, false);

    auto download_buffer_size = getContext()->getSettings().max_download_buffer_size;
    const bool use_parallel_download = download_buffer_size > 0 && download_thread_num > 1;
    const bool object_too_small = object_size < download_thread_num * download_buffer_size;
    if (!use_parallel_download || object_too_small)
    {
        LOG_TRACE(log, "Downloading object of size {} from S3 in single thread", object_size);
        return std::make_unique<ReadBufferFromS3>(client, bucket, key, version_id, max_single_read_retries, getContext()->getReadSettings());
    }

    assert(object_size > 0);

    if (download_buffer_size < DBMS_DEFAULT_BUFFER_SIZE)
    {
        LOG_WARNING(log, "Downloading buffer {} bytes too small, set at least {} bytes", download_buffer_size, DBMS_DEFAULT_BUFFER_SIZE);
        download_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    }

    auto factory = std::make_unique<ReadBufferS3Factory>(
        client, bucket, key, version_id, download_buffer_size, object_size, max_single_read_retries, getContext()->getReadSettings());
    LOG_TRACE(
        log, "Downloading from S3 in {} threads. Object size: {}, Range size: {}.", download_thread_num, object_size, download_buffer_size);

    return std::make_unique<ParallelReadBuffer>(std::move(factory), threadPoolCallbackRunner(IOThreadPool::get()), download_thread_num);
}

String StorageS3Source::getName() const
{
    return name;
}

Chunk StorageS3Source::generate()
{
    while (true)
    {
        if (!reader || isCancelled())
            break;

        Chunk chunk;
        if (reader->pull(chunk))
        {
            UInt64 num_rows = chunk.getNumRows();

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

        {
            std::lock_guard lock(reader_mutex);
            reader.reset();
            pipeline.reset();
            read_buf.reset();

            if (!initialize())
                break;
        }
    }
    return {};
}

static bool checkIfObjectExists(const std::shared_ptr<const Aws::S3::S3Client> & client, const String & bucket, const String & key)
{
    bool is_finished = false;
    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    request.SetBucket(bucket);
    request.SetPrefix(key);
    while (!is_finished)
    {
        outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess())
            throw Exception(
                ErrorCodes::S3_ERROR,
                "Could not list objects in bucket {} with key {}, S3 exception: {}, message: {}",
                quoteString(bucket),
                quoteString(key),
                backQuote(outcome.GetError().GetExceptionName()),
                quoteString(outcome.GetError().GetMessage()));

        const auto & result_batch = outcome.GetResult().GetContents();
        for (const auto & obj : result_batch)
        {
            if (obj.GetKey() == key)
                return true;
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    return false;
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
        const StorageS3::S3Configuration & s3_configuration_,
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
                s3_configuration_.rw_settings,
                std::nullopt,
                DBMS_DEFAULT_BUFFER_SIZE,
                threadPoolCallbackRunner(IOThreadPool::get())),
            compression_method,
            3);
        writer
            = FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, sample_block, context, {}, format_settings);
    }

    String getName() const override { return "StorageS3Sink"; }

    void consume(Chunk chunk) override
    {
        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onException() override
    {
        if (!writer)
            return;
        onFinish();
    }

    void onFinish() override
    {
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

private:
    Block sample_block;
    std::optional<FormatSettings> format_settings;
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
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
        const StorageS3::S3Configuration & s3_configuration_,
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
    const StorageS3::S3Configuration & s3_configuration;
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
    const S3::URI & uri_,
    const String & access_key_id_,
    const String & secret_access_key_,
    const StorageID & table_id_,
    const String & format_name_,
    const S3Settings::ReadWriteSettings & rw_settings_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    const String & compression_method_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , s3_configuration{uri_, access_key_id_, secret_access_key_, {}, {}, rw_settings_} /// Client and settings will be updated later
    , keys({uri_.key})
    , format_name(format_name_)
    , compression_method(compression_method_)
    , name(uri_.storage_name)
    , distributed_processing(distributed_processing_)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
    , is_key_with_globs(uri_.key.find_first_of("*?{") != std::string::npos)
{
    FormatFactory::instance().checkFormatName(format_name);
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(uri_.uri);
    StorageInMemoryMetadata storage_metadata;

    updateS3Configuration(context_, s3_configuration);
    if (columns_.empty())
    {
        auto columns = getTableStructureFromDataImpl(
            format_name,
            s3_configuration,
            compression_method,
            distributed_processing_,
            is_key_with_globs,
            format_settings,
            context_,
            &read_tasks_used_in_schema_inference);
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

std::shared_ptr<StorageS3Source::IteratorWrapper> StorageS3::createFileIterator(
    const S3Configuration & s3_configuration,
    const std::vector<String> & keys,
    bool is_key_with_globs,
    bool distributed_processing,
    ContextPtr local_context,
    ASTPtr query,
    const Block & virtual_block,
    const std::vector<String> & read_tasks)
{
    if (distributed_processing)
    {
        return std::make_shared<StorageS3Source::IteratorWrapper>(
            [read_tasks_iterator = std::make_shared<StorageS3Source::ReadTasksIterator>(read_tasks, local_context->getReadTaskCallback())]() -> String
        {
                return read_tasks_iterator->next();
        });
    }
    else if (is_key_with_globs)
    {
        /// Iterate through disclosed globs and make a source for each file
        auto glob_iterator = std::make_shared<StorageS3Source::DisclosedGlobIterator>(
            *s3_configuration.client, s3_configuration.uri, query, virtual_block, local_context);
        return std::make_shared<StorageS3Source::IteratorWrapper>([glob_iterator]() { return glob_iterator->next(); });
    }
    else
    {
        auto keys_iterator
            = std::make_shared<StorageS3Source::KeysIterator>(keys, s3_configuration.uri.bucket, query, virtual_block, local_context);
        return std::make_shared<StorageS3Source::IteratorWrapper>([keys_iterator]() { return keys_iterator->next(); });
    }
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
    unsigned num_streams)
{
    bool has_wildcards = s3_configuration.uri.bucket.find(PARTITION_ID_WILDCARD) != String::npos
        || keys.back().find(PARTITION_ID_WILDCARD) != String::npos;
    if (partition_by && has_wildcards)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading from a partitioned S3 storage is not implemented yet");

    updateS3Configuration(local_context, s3_configuration);

    Pipes pipes;

    std::unordered_set<String> column_names_set(column_names.begin(), column_names.end());
    std::vector<NameAndTypePair> requested_virtual_columns;

    for (const auto & virtual_column : getVirtuals())
    {
        if (column_names_set.contains(virtual_column.name))
            requested_virtual_columns.push_back(virtual_column);
    }

    std::shared_ptr<StorageS3Source::IteratorWrapper> iterator_wrapper = createFileIterator(
        s3_configuration,
        keys,
        is_key_with_globs,
        distributed_processing,
        local_context,
        query_info.query,
        virtual_block,
        read_tasks_used_in_schema_inference);

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
            fetch_columns.push_back(ExpressionActions::getSmallestColumn(storage_snapshot->metadata->getColumns().getAllPhysical()));

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
            s3_configuration.rw_settings.max_single_read_retries,
            compression_method,
            s3_configuration.client,
            s3_configuration.uri.bucket,
            s3_configuration.uri.version_id,
            iterator_wrapper,
            max_download_threads));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));

    narrowPipe(pipe, num_streams);
    return pipe;
}

SinkToStoragePtr StorageS3::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    updateS3Configuration(local_context, s3_configuration);

    auto sample_block = metadata_snapshot->getSampleBlock();
    auto chosen_compression_method = chooseCompressionMethod(keys.back(), compression_method);
    bool has_wildcards = s3_configuration.uri.bucket.find(PARTITION_ID_WILDCARD) != String::npos || keys.back().find(PARTITION_ID_WILDCARD) != String::npos;
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
            s3_configuration.uri.bucket,
            keys.back());
    }
    else
    {
        if (is_key_with_globs)
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "S3 key '{}' contains globs, so the table is in readonly mode", s3_configuration.uri.key);

        bool truncate_in_insert = local_context->getSettingsRef().s3_truncate_on_insert;

        if (!truncate_in_insert && checkIfObjectExists(s3_configuration.client, s3_configuration.uri.bucket, keys.back()))
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
                while (checkIfObjectExists(s3_configuration.client, s3_configuration.uri.bucket, new_key));
                keys.push_back(new_key);
            }
            else
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Object in bucket {} with key {} already exists. If you want to overwrite it, enable setting s3_truncate_on_insert, if you "
                    "want to create a new file on each insert, enable setting s3_create_new_file_on_insert",
                    s3_configuration.uri.bucket,
                    keys.back());
        }

        return std::make_shared<StorageS3Sink>(
            format_name,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            s3_configuration,
            s3_configuration.uri.bucket,
            keys.back());
    }
}


void StorageS3::truncate(const ASTPtr & /* query */, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    updateS3Configuration(local_context, s3_configuration);

    if (is_key_with_globs)
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "S3 key '{}' contains globs, so the table is in readonly mode", s3_configuration.uri.key);

    Aws::S3::Model::Delete delkeys;

    for (const auto & key : keys)
    {
        Aws::S3::Model::ObjectIdentifier obj;
        obj.SetKey(key);
        delkeys.AddObjects(std::move(obj));
    }

    Aws::S3::Model::DeleteObjectsRequest request;
    request.SetBucket(s3_configuration.uri.bucket);
    request.SetDelete(delkeys);

    auto response = s3_configuration.client->DeleteObjects(request);
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw Exception(std::to_string(static_cast<int>(err.GetErrorType())) + ": " + err.GetMessage(), ErrorCodes::S3_ERROR);
    }
}


void StorageS3::updateS3Configuration(ContextPtr ctx, StorageS3::S3Configuration & upd)
{
    auto settings = ctx->getStorageS3Settings().getSettings(upd.uri.uri.toString());

    bool need_update_configuration = settings != S3Settings{};
    if (need_update_configuration)
    {
        if (upd.rw_settings != settings.rw_settings)
            upd.rw_settings = settings.rw_settings;
    }

    upd.rw_settings.updateFromSettingsIfEmpty(ctx->getSettings());

    if (upd.client && (!upd.access_key_id.empty() || settings.auth_settings == upd.auth_settings))
        return;

    Aws::Auth::AWSCredentials credentials(upd.access_key_id, upd.secret_access_key);
    HeaderCollection headers;
    if (upd.access_key_id.empty())
    {
        credentials = Aws::Auth::AWSCredentials(settings.auth_settings.access_key_id, settings.auth_settings.secret_access_key);
        headers = settings.auth_settings.headers;
    }

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        settings.auth_settings.region,
        ctx->getRemoteHostFilter(), ctx->getGlobalContext()->getSettingsRef().s3_max_redirects,
        ctx->getGlobalContext()->getSettingsRef().enable_s3_requests_logging);

    client_configuration.endpointOverride = upd.uri.endpoint;
    client_configuration.maxConnections = upd.rw_settings.max_connections;

    upd.client = S3::ClientFactory::instance().create(
        client_configuration,
        upd.uri.is_virtual_hosted_style,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        settings.auth_settings.server_side_encryption_customer_key_base64,
        std::move(headers),
        settings.auth_settings.use_environment_credentials.value_or(ctx->getConfigRef().getBool("s3.use_environment_credentials", false)),
        settings.auth_settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)));

    upd.auth_settings = std::move(settings.auth_settings);
}


void StorageS3::processNamedCollectionResult(StorageS3Configuration & configuration, const std::vector<std::pair<String, ASTPtr>> & key_value_args)
{
    for (const auto & [arg_name, arg_value] : key_value_args)
    {
        if (arg_name == "access_key_id")
            configuration.auth_settings.access_key_id = arg_value->as<ASTLiteral>()->value.safeGet<String>();
        else if (arg_name == "secret_access_key")
            configuration.auth_settings.secret_access_key = arg_value->as<ASTLiteral>()->value.safeGet<String>();
        else if (arg_name == "filename")
            configuration.url = std::filesystem::path(configuration.url) / arg_value->as<ASTLiteral>()->value.safeGet<String>();
        else if (arg_name == "use_environment_credentials")
            configuration.auth_settings.use_environment_credentials = arg_value->as<ASTLiteral>()->value.safeGet<UInt8>();
        else if (arg_name == "max_single_read_retries")
            configuration.rw_settings.max_single_read_retries = arg_value->as<ASTLiteral>()->value.safeGet<UInt64>();
        else if (arg_name == "min_upload_part_size")
            configuration.rw_settings.max_single_read_retries = arg_value->as<ASTLiteral>()->value.safeGet<UInt64>();
        else if (arg_name == "upload_part_size_multiply_factor")
            configuration.rw_settings.max_single_read_retries = arg_value->as<ASTLiteral>()->value.safeGet<UInt64>();
        else if (arg_name == "upload_part_size_multiply_parts_count_threshold")
            configuration.rw_settings.max_single_read_retries = arg_value->as<ASTLiteral>()->value.safeGet<UInt64>();
        else if (arg_name == "max_single_part_upload_size")
            configuration.rw_settings.max_single_read_retries = arg_value->as<ASTLiteral>()->value.safeGet<UInt64>();
        else if (arg_name == "max_connections")
            configuration.rw_settings.max_single_read_retries = arg_value->as<ASTLiteral>()->value.safeGet<UInt64>();
        else
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Unknown key-value argument `{}` for StorageS3, expected: url, [access_key_id, secret_access_key], name of used format and [compression_method].",
                arg_name);
    }
}


StorageS3Configuration StorageS3::getConfiguration(ASTs & engine_args, ContextPtr local_context)
{
    StorageS3Configuration configuration;

    if (auto named_collection = getURLBasedDataSourceConfiguration(engine_args, local_context))
    {
        auto [common_configuration, storage_specific_args] = named_collection.value();
        configuration.set(common_configuration);
        processNamedCollectionResult(configuration, storage_specific_args);
    }
    else
    {
        if (engine_args.empty() || engine_args.size() > 5)
            throw Exception(
                "Storage S3 requires 1 to 5 arguments: url, [access_key_id, secret_access_key], name of used format and [compression_method].",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, local_context);

        configuration.url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        if (engine_args.size() >= 4)
        {
            configuration.auth_settings.access_key_id = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
            configuration.auth_settings.secret_access_key = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        }

        if (engine_args.size() == 3 || engine_args.size() == 5)
        {
            configuration.compression_method = engine_args.back()->as<ASTLiteral &>().value.safeGet<String>();
            configuration.format = engine_args[engine_args.size() - 2]->as<ASTLiteral &>().value.safeGet<String>();
        }
        else if (engine_args.size() != 1)
        {
            configuration.compression_method = "auto";
            configuration.format = engine_args.back()->as<ASTLiteral &>().value.safeGet<String>();
        }
    }

    if (configuration.format == "auto")
        configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.url, true);

    return configuration;
}

ColumnsDescription StorageS3::getTableStructureFromData(
    const String & format,
    const S3::URI & uri,
    const String & access_key_id,
    const String & secret_access_key,
    const String & compression_method,
    bool distributed_processing,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr ctx)
{
    S3Configuration s3_configuration{ uri, access_key_id, secret_access_key, {}, {}, S3Settings::ReadWriteSettings(ctx->getSettingsRef()) };
    updateS3Configuration(ctx, s3_configuration);
    return getTableStructureFromDataImpl(format, s3_configuration, compression_method, distributed_processing, uri.key.find_first_of("*?{") != std::string::npos, format_settings, ctx);
}

ColumnsDescription StorageS3::getTableStructureFromDataImpl(
    const String & format,
    const S3Configuration & s3_configuration,
    const String & compression_method,
    bool distributed_processing,
    bool is_key_with_globs,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr ctx,
    std::vector<String> * read_keys_in_distributed_processing)
{
    auto file_iterator
        = createFileIterator(s3_configuration, {s3_configuration.uri.key}, is_key_with_globs, distributed_processing, ctx, nullptr, {});

    ReadBufferIterator read_buffer_iterator = [&, first = false]() mutable -> std::unique_ptr<ReadBuffer>
    {
        auto key = (*file_iterator)();

        if (key.empty())
        {
            if (first)
                throw Exception(
                    ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                    "Cannot extract table structure from {} format file, because there are no files with provided path in S3. You must specify "
                    "table structure manually",
                    format);

            return nullptr;
        }

        if (distributed_processing && read_keys_in_distributed_processing)
            read_keys_in_distributed_processing->push_back(key);

        first = false;
        return wrapReadBufferWithCompressionMethod(
            std::make_unique<ReadBufferFromS3>(
                s3_configuration.client, s3_configuration.uri.bucket, key, s3_configuration.uri.version_id, s3_configuration.rw_settings.max_single_read_retries, ctx->getReadSettings()),
            chooseCompressionMethod(key, compression_method));
    };

    return readSchemaFromFormat(format, format_settings, read_buffer_iterator, is_key_with_globs, ctx);
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

        S3::URI s3_uri(Poco::URI(configuration.url));

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            partition_by = args.storage_def->partition_by->clone();

        return std::make_shared<StorageS3>(
            s3_uri,
            configuration.auth_settings.access_key_id,
            configuration.auth_settings.secret_access_key,
            args.table_id,
            configuration.format,
            configuration.rw_settings,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
            format_settings,
            configuration.compression_method,
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

NamesAndTypesList StorageS3::getVirtuals() const
{
    return virtual_columns;
}

bool StorageS3::supportsPartitionBy() const
{
    return true;
}

}

#endif
