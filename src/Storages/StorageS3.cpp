#include <Common/config.h>
#include "Parsers/ASTCreateQuery.h"

#if USE_AWS_S3

#include <Common/isValidUTF8.h>

#include <Functions/FunctionsConversion.h>

#include <IO/S3Common.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/PartitionedSink.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>

#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/narrowBlockInputStreams.h>

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

#include <Processors/Sources/SourceWithProgress.h>
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
}

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

class StorageS3Source::DisclosedGlobIterator::Impl
{

public:
    Impl(Aws::S3::S3Client & client_, const S3::URI & globbed_uri_)
        : client(client_), globbed_uri(globbed_uri_)
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

        buffer.reserve(result_batch.size());
        for (const auto & row : result_batch)
        {
            String key = row.GetKey();
            if (re2::RE2::FullMatch(key, *matcher))
                buffer.emplace_back(std::move(key));
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
    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;
    std::unique_ptr<re2::RE2> matcher;
    bool is_finished{false};
};

StorageS3Source::DisclosedGlobIterator::DisclosedGlobIterator(Aws::S3::S3Client & client_, const S3::URI & globbed_uri_)
    : pimpl(std::make_shared<StorageS3Source::DisclosedGlobIterator::Impl>(client_, globbed_uri_)) {}

String StorageS3Source::DisclosedGlobIterator::next()
{
    return pimpl->next();
}

class StorageS3Source::KeysIterator::Impl
{
public:
    explicit Impl(const std::vector<String> & keys_) : keys(keys_), keys_iter(keys.begin())
    {
    }

    String next()
    {
        std::lock_guard lock(mutex);
        if (keys_iter == keys.end())
            return "";
        auto key = *keys_iter;
        ++keys_iter;
        return key;
    }

private:
    std::mutex mutex;
    Strings keys;
    Strings::iterator keys_iter;
};

StorageS3Source::KeysIterator::KeysIterator(const std::vector<String> & keys_) : pimpl(std::make_shared<StorageS3Source::KeysIterator::Impl>(keys_))
{
}

String StorageS3Source::KeysIterator::next()
{
    return pimpl->next();
}

Block StorageS3Source::getHeader(Block sample_block, bool with_path_column, bool with_file_column)
{
    if (with_path_column)
        sample_block.insert(
            {DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn(),
             std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
             "_path"});
    if (with_file_column)
        sample_block.insert(
            {DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn(),
             std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
             "_file"});

    return sample_block;
}

StorageS3Source::StorageS3Source(
    bool need_path,
    bool need_file,
    const String & format_,
    String name_,
    const Block & sample_block_,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    const ColumnsDescription & columns_,
    UInt64 max_block_size_,
    UInt64 max_single_read_retries_,
    String compression_hint_,
    const std::shared_ptr<Aws::S3::S3Client> & client_,
    const String & bucket_,
    std::shared_ptr<IteratorWrapper> file_iterator_)
    : SourceWithProgress(getHeader(sample_block_, need_path, need_file))
    , WithContext(context_)
    , name(std::move(name_))
    , bucket(bucket_)
    , format(format_)
    , columns_desc(columns_)
    , max_block_size(max_block_size_)
    , max_single_read_retries(max_single_read_retries_)
    , compression_hint(std::move(compression_hint_))
    , client(client_)
    , sample_block(sample_block_)
    , format_settings(format_settings_)
    , with_file_column(need_file)
    , with_path_column(need_path)
    , file_iterator(file_iterator_)
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

    read_buf = wrapReadBufferWithCompressionMethod(
        std::make_unique<ReadBufferFromS3>(client, bucket, current_key, max_single_read_retries, getContext()->getReadSettings()),
        chooseCompressionMethod(current_key, compression_hint));
    auto input_format = getContext()->getInputFormat(format, *read_buf, sample_block, max_block_size, format_settings);
    QueryPipelineBuilder builder;
    builder.init(Pipe(input_format));

    if (columns_desc.hasDefaults())
    {
        builder.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext());
        });
    }

    pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    initialized = false;
    return true;
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

            if (with_path_column)
                chunk.addColumn(DataTypeLowCardinality{std::make_shared<DataTypeString>()}
                                    .createColumnConst(num_rows, file_path)
                                    ->convertToFullColumnIfConst());
            if (with_file_column)
            {
                size_t last_slash_pos = file_path.find_last_of('/');
                chunk.addColumn(DataTypeLowCardinality{std::make_shared<DataTypeString>()}
                                    .createColumnConst(num_rows, file_path.substr(last_slash_pos + 1))
                                    ->convertToFullColumnIfConst());
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

static bool checkIfObjectExists(const std::shared_ptr<Aws::S3::S3Client> & client, const String & bucket, const String & key)
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
        const std::shared_ptr<Aws::S3::S3Client> & client,
        const String & bucket,
        const String & key,
        size_t min_upload_part_size,
        size_t upload_part_size_multiply_factor,
        size_t upload_part_size_multiply_parts_count_threshold,
        size_t max_single_part_upload_size)
        : SinkToStorage(sample_block_)
        , sample_block(sample_block_)
        , format_settings(format_settings_)
    {
        write_buf = wrapWriteBufferWithCompressionMethod(
            std::make_unique<WriteBufferFromS3>(
                client, bucket, key, min_upload_part_size,
                upload_part_size_multiply_factor, upload_part_size_multiply_parts_count_threshold,
                max_single_part_upload_size), compression_method, 3);
        writer = FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, sample_block, context, {}, format_settings);
    }

    String getName() const override { return "StorageS3Sink"; }

    void consume(Chunk chunk) override
    {
        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
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
        const std::shared_ptr<Aws::S3::S3Client> & client_,
        const String & bucket_,
        const String & key_,
        size_t min_upload_part_size_,
        size_t upload_part_size_multiply_factor_,
        size_t upload_part_size_multiply_parts_count_threshold_,
        size_t max_single_part_upload_size_)
        : PartitionedSink(partition_by, context_, sample_block_)
        , format(format_)
        , sample_block(sample_block_)
        , context(context_)
        , compression_method(compression_method_)
        , client(client_)
        , bucket(bucket_)
        , key(key_)
        , min_upload_part_size(min_upload_part_size_)
        , upload_part_size_multiply_factor(upload_part_size_multiply_factor_)
        , upload_part_size_multiply_parts_count_threshold(upload_part_size_multiply_parts_count_threshold_)
        , max_single_part_upload_size(max_single_part_upload_size_)
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
            client,
            partition_bucket,
            partition_key,
            min_upload_part_size,
            upload_part_size_multiply_factor,
            upload_part_size_multiply_parts_count_threshold,
            max_single_part_upload_size
        );
    }

private:
    const String format;
    const Block sample_block;
    ContextPtr context;
    const CompressionMethod compression_method;

    std::shared_ptr<Aws::S3::S3Client> client;
    const String bucket;
    const String key;
    size_t min_upload_part_size;
    size_t upload_part_size_multiply_factor;
    size_t upload_part_size_multiply_parts_count_threshold;
    size_t max_single_part_upload_size;
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
    UInt64 max_single_read_retries_,
    UInt64 min_upload_part_size_,
    UInt64 upload_part_size_multiply_factor_,
    UInt64 upload_part_size_multiply_parts_count_threshold_,
    UInt64 max_single_part_upload_size_,
    UInt64 max_connections_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    const String & compression_method_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , client_auth{uri_, access_key_id_, secret_access_key_, max_connections_, {}, {}} /// Client and settings will be updated later
    , keys({uri_.key})
    , format_name(format_name_)
    , max_single_read_retries(max_single_read_retries_)
    , min_upload_part_size(min_upload_part_size_)
    , upload_part_size_multiply_factor(upload_part_size_multiply_factor_)
    , upload_part_size_multiply_parts_count_threshold(upload_part_size_multiply_parts_count_threshold_)
    , max_single_part_upload_size(max_single_part_upload_size_)
    , compression_method(compression_method_)
    , name(uri_.storage_name)
    , distributed_processing(distributed_processing_)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
    , is_key_with_globs(uri_.key.find_first_of("*?{") != std::string::npos)
{
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(uri_.uri);
    StorageInMemoryMetadata storage_metadata;

    updateClientAndAuthSettings(context_, client_auth);
    if (columns_.empty())
    {
        auto columns = getTableStructureFromDataImpl(format_name, client_auth, max_single_read_retries_, compression_method, distributed_processing_, is_key_with_globs, format_settings, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

std::shared_ptr<StorageS3Source::IteratorWrapper> StorageS3::createFileIterator(const ClientAuthentication & client_auth, const std::vector<String> & keys, bool is_key_with_globs, bool distributed_processing, ContextPtr local_context)
{
    if (distributed_processing)
    {
        return std::make_shared<StorageS3Source::IteratorWrapper>(
            [callback = local_context->getReadTaskCallback()]() -> String {
                return callback();
        });
    }
    else if (is_key_with_globs)
    {
        /// Iterate through disclosed globs and make a source for each file
        auto glob_iterator = std::make_shared<StorageS3Source::DisclosedGlobIterator>(*client_auth.client, client_auth.uri);
        return std::make_shared<StorageS3Source::IteratorWrapper>([glob_iterator]()
        {
            return glob_iterator->next();
        });
    }
    else
    {
        auto keys_iterator = std::make_shared<StorageS3Source::KeysIterator>(keys);
        return std::make_shared<StorageS3Source::IteratorWrapper>([keys_iterator]()
        {
            return keys_iterator->next();
        });
    }
}

bool StorageS3::isColumnOriented() const
{
    return FormatFactory::instance().checkIfFormatIsColumnOriented(format_name);
}

Pipe StorageS3::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    updateClientAndAuthSettings(local_context, client_auth);

    Pipes pipes;
    bool need_path_column = false;
    bool need_file_column = false;
    for (const auto & column : column_names)
    {
        if (column == "_path")
            need_path_column = true;
        if (column == "_file")
            need_file_column = true;
    }

    std::shared_ptr<StorageS3Source::IteratorWrapper> iterator_wrapper = createFileIterator(client_auth, keys, is_key_with_globs, distributed_processing, local_context);

    ColumnsDescription columns_description;
    Block block_for_format;
    if (isColumnOriented())
    {
        columns_description = ColumnsDescription{
            storage_snapshot->getSampleBlockForColumns(column_names).getNamesAndTypesList()};
        block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    }
    else
    {
        columns_description = storage_snapshot->metadata->getColumns();
        block_for_format = storage_snapshot->metadata->getSampleBlock();
    }

    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageS3Source>(
            need_path_column,
            need_file_column,
            format_name,
            getName(),
            block_for_format,
            local_context,
            format_settings,
            columns_description,
            max_block_size,
            max_single_read_retries,
            compression_method,
            client_auth.client,
            client_auth.uri.bucket,
            iterator_wrapper));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));

    narrowPipe(pipe, num_streams);
    return pipe;
}

SinkToStoragePtr StorageS3::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    updateClientAndAuthSettings(local_context, client_auth);

    auto sample_block = metadata_snapshot->getSampleBlock();
    auto chosen_compression_method = chooseCompressionMethod(keys.back(), compression_method);
    bool has_wildcards = client_auth.uri.bucket.find(PARTITION_ID_WILDCARD) != String::npos || keys.back().find(PARTITION_ID_WILDCARD) != String::npos;
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
            client_auth.client,
            client_auth.uri.bucket,
            keys.back(),
            min_upload_part_size,
            upload_part_size_multiply_factor,
            upload_part_size_multiply_parts_count_threshold,
            max_single_part_upload_size);
    }
    else
    {
        if (is_key_with_globs)
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "S3 key '{}' contains globs, so the table is in readonly mode", client_auth.uri.key);

        bool truncate_in_insert = local_context->getSettingsRef().s3_truncate_on_insert;

        if (!truncate_in_insert && checkIfObjectExists(client_auth.client, client_auth.uri.bucket, keys.back()))
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
                while (checkIfObjectExists(client_auth.client, client_auth.uri.bucket, new_key));
                keys.push_back(new_key);
            }
            else
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Object in bucket {} with key {} already exists. If you want to overwrite it, enable setting s3_truncate_on_insert, if you "
                    "want to create a new file on each insert, enable setting s3_create_new_file_on_insert",
                    client_auth.uri.bucket,
                    keys.back());
        }

        return std::make_shared<StorageS3Sink>(
            format_name,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            client_auth.client,
            client_auth.uri.bucket,
            keys.back(),
            min_upload_part_size,
            upload_part_size_multiply_factor,
            upload_part_size_multiply_parts_count_threshold,
            max_single_part_upload_size);
    }
}


void StorageS3::truncate(const ASTPtr & /* query */, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    updateClientAndAuthSettings(local_context, client_auth);

    if (is_key_with_globs)
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "S3 key '{}' contains globs, so the table is in readonly mode", client_auth.uri.key);

    Aws::S3::Model::Delete delkeys;

    for (const auto & key : keys)
    {
        Aws::S3::Model::ObjectIdentifier obj;
        obj.SetKey(key);
        delkeys.AddObjects(std::move(obj));
    }

    Aws::S3::Model::DeleteObjectsRequest request;
    request.SetBucket(client_auth.uri.bucket);
    request.SetDelete(delkeys);

    auto response = client_auth.client->DeleteObjects(request);
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw Exception(std::to_string(static_cast<int>(err.GetErrorType())) + ": " + err.GetMessage(), ErrorCodes::S3_ERROR);
    }
}


void StorageS3::updateClientAndAuthSettings(ContextPtr ctx, StorageS3::ClientAuthentication & upd)
{
    auto settings = ctx->getStorageS3Settings().getSettings(upd.uri.uri.toString());
    if (upd.client && (!upd.access_key_id.empty() || settings == upd.auth_settings))
        return;

    Aws::Auth::AWSCredentials credentials(upd.access_key_id, upd.secret_access_key);
    HeaderCollection headers;
    if (upd.access_key_id.empty())
    {
        credentials = Aws::Auth::AWSCredentials(settings.access_key_id, settings.secret_access_key);
        headers = settings.headers;
    }

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        settings.region,
        ctx->getRemoteHostFilter(), ctx->getGlobalContext()->getSettingsRef().s3_max_redirects);

    client_configuration.endpointOverride = upd.uri.endpoint;
    client_configuration.maxConnections = upd.max_connections;

    upd.client = S3::ClientFactory::instance().create(
        client_configuration,
        upd.uri.is_virtual_hosted_style,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        settings.server_side_encryption_customer_key_base64,
        std::move(headers),
        settings.use_environment_credentials.value_or(ctx->getConfigRef().getBool("s3.use_environment_credentials", false)),
        settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)));

    upd.auth_settings = std::move(settings);
}


StorageS3Configuration StorageS3::getConfiguration(ASTs & engine_args, ContextPtr local_context)
{
    StorageS3Configuration configuration;

    if (auto named_collection = getURLBasedDataSourceConfiguration(engine_args, local_context))
    {
        auto [common_configuration, storage_specific_args] = named_collection.value();
        configuration.set(common_configuration);

        for (const auto & [arg_name, arg_value] : storage_specific_args)
        {
            if (arg_name == "access_key_id")
                configuration.access_key_id = arg_value->as<ASTLiteral>()->value.safeGet<String>();
            else if (arg_name == "secret_access_key")
                configuration.secret_access_key = arg_value->as<ASTLiteral>()->value.safeGet<String>();
            else
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Unknown key-value argument `{}` for StorageS3, expected: url, [access_key_id, secret_access_key], name of used format and [compression_method].",
                    arg_name);
        }
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
            configuration.access_key_id = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
            configuration.secret_access_key = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
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
    UInt64 max_connections,
    UInt64 max_single_read_retries,
    const String & compression_method,
    bool distributed_processing,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr ctx)
{
    ClientAuthentication client_auth{uri, access_key_id, secret_access_key, max_connections, {}, {}};
    updateClientAndAuthSettings(ctx, client_auth);
    return getTableStructureFromDataImpl(format, client_auth, max_single_read_retries, compression_method, distributed_processing, uri.key.find_first_of("*?{") != std::string::npos, format_settings, ctx);
}

ColumnsDescription StorageS3::getTableStructureFromDataImpl(
    const String & format,
    const ClientAuthentication & client_auth,
    UInt64 max_single_read_retries,
    const String & compression_method,
    bool distributed_processing,
    bool is_key_with_globs,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr ctx)
{
    std::vector<String> keys = {client_auth.uri.key};
    auto file_iterator = createFileIterator(client_auth, keys, is_key_with_globs, distributed_processing, ctx);

    std::string current_key;
    std::string exception_messages;
    bool read_buffer_creator_was_used = false;
    do
    {
        current_key = (*file_iterator)();
        auto read_buffer_creator = [&]()
        {
            read_buffer_creator_was_used = true;
            if (current_key.empty())
                throw Exception(
                    ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                    "Cannot extract table structure from {} format file, because there are no files with provided path in S3. You must specify "
                    "table structure manually",
                    format);

            return wrapReadBufferWithCompressionMethod(
                std::make_unique<ReadBufferFromS3>(
                    client_auth.client, client_auth.uri.bucket, current_key, max_single_read_retries, ctx->getReadSettings()),
                chooseCompressionMethod(current_key, compression_method));
        };

        try
        {
            return readSchemaFromFormat(format, format_settings, read_buffer_creator, ctx);
        }
        catch (...)
        {
            if (!is_key_with_globs || !read_buffer_creator_was_used)
                throw;

            exception_messages += getCurrentExceptionMessage(false) + "\n";
        }
    } while (!current_key.empty());

    throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "All attempts to extract table structure from s3 files failed. Errors:\n{}", exception_messages);
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
        auto max_single_read_retries = args.getLocalContext()->getSettingsRef().s3_max_single_read_retries;
        auto min_upload_part_size = args.getLocalContext()->getSettingsRef().s3_min_upload_part_size;
        auto upload_part_size_multiply_factor = args.getLocalContext()->getSettingsRef().s3_upload_part_size_multiply_factor;
        auto upload_part_size_multiply_parts_count_threshold = args.getLocalContext()->getSettingsRef().s3_upload_part_size_multiply_parts_count_threshold;
        auto max_single_part_upload_size = args.getLocalContext()->getSettingsRef().s3_max_single_part_upload_size;

        auto max_connections = args.getLocalContext()->getSettingsRef().s3_max_connections;

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            partition_by = args.storage_def->partition_by->clone();

        return StorageS3::create(
            s3_uri,
            configuration.access_key_id,
            configuration.secret_access_key,
            args.table_id,
            configuration.format,
            max_single_read_retries,
            min_upload_part_size,
            upload_part_size_multiply_factor,
            upload_part_size_multiply_parts_count_threshold,
            max_single_part_upload_size,
            max_connections,
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
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};
}

bool StorageS3::supportsPartitionBy() const
{
    return true;
}

}

#endif
