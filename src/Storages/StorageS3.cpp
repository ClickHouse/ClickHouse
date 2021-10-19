#include <Common/config.h>
#include "Parsers/ASTCreateQuery.h"

#if USE_AWS_S3

#include <Columns/ColumnString.h>
#include <Common/isValidUTF8.h>

#include <Functions/FunctionsConversion.h>

#include <IO/S3Common.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageS3Settings.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>

#include <DataStreams/IBlockOutputStream.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <DataStreams/narrowBlockInputStreams.h>

#include <Processors/QueryPipeline.h>
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
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/Pipe.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>

namespace fs = std::filesystem;

#include <boost/algorithm/string.hpp>


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
}
class StorageS3Source::DisclosedGlobIterator::Impl
{

public:
    Impl(Aws::S3::S3Client & client_, const S3::URI & globbed_uri_)
        : client(client_), globbed_uri(globbed_uri_)
    {
        std::lock_guard lock(mutex);

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


Block StorageS3Source::getHeader(Block sample_block, bool with_path_column, bool with_file_column)
{
    if (with_path_column)
        sample_block.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_path"});
    if (with_file_column)
        sample_block.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_file"});

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
    const String compression_hint_,
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
    , compression_hint(compression_hint_)
    , client(client_)
    , sample_block(sample_block_)
    , format_settings(format_settings_)
    , with_file_column(need_file)
    , with_path_column(need_path)
    , file_iterator(file_iterator_)
{
    initialize();
}


bool StorageS3Source::initialize()
{
    String current_key = (*file_iterator)();
    if (current_key.empty())
        return false;

    file_path = fs::path(bucket) / current_key;

    read_buf = wrapReadBufferWithCompressionMethod(
        std::make_unique<ReadBufferFromS3>(client, bucket, current_key, max_single_read_retries, DBMS_DEFAULT_BUFFER_SIZE),
        chooseCompressionMethod(current_key, compression_hint));
    auto input_format = FormatFactory::instance().getInput(format, *read_buf, sample_block, getContext(), max_block_size, format_settings);
    pipeline = std::make_unique<QueryPipeline>();
    pipeline->init(Pipe(input_format));

    if (columns_desc.hasDefaults())
    {
        pipeline->addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext());
        });
    }

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
    if (!reader)
        return {};

    Chunk chunk;
    if (reader->pull(chunk))
    {
        UInt64 num_rows = chunk.getNumRows();

        if (with_path_column)
            chunk.addColumn(DataTypeString().createColumnConst(num_rows, file_path)->convertToFullColumnIfConst());
        if (with_file_column)
        {
            size_t last_slash_pos = file_path.find_last_of('/');
            chunk.addColumn(DataTypeString().createColumnConst(num_rows, file_path.substr(
                    last_slash_pos + 1))->convertToFullColumnIfConst());
        }

        return chunk;
    }

    reader.reset();
    pipeline.reset();
    read_buf.reset();

    if (!initialize())
        return {};

    return generate();
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
        size_t max_single_part_upload_size)
        : SinkToStorage(sample_block_)
        , sample_block(sample_block_)
        , format_settings(format_settings_)
    {
        write_buf = wrapWriteBufferWithCompressionMethod(
            std::make_unique<WriteBufferFromS3>(client, bucket, key, min_upload_part_size, max_single_part_upload_size), compression_method, 3);
        writer = FormatFactory::instance().getOutputStreamParallelIfPossible(format, *write_buf, sample_block, context, {}, format_settings);
    }

    String getName() const override { return "StorageS3Sink"; }

    void consume(Chunk chunk) override
    {
        if (is_first_chunk)
        {
            writer->writePrefix();
            is_first_chunk = false;
        }
        writer->write(getPort().getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onFinish() override
    {
        try
        {
            writer->writeSuffix();
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
    BlockOutputStreamPtr writer;
    bool is_first_chunk = true;
};


class PartitionedStorageS3Sink : public SinkToStorage
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
        size_t max_single_part_upload_size_)
        : SinkToStorage(sample_block_)
        , format(format_)
        , sample_block(sample_block_)
        , context(context_)
        , compression_method(compression_method_)
        , client(client_)
        , bucket(bucket_)
        , key(key_)
        , min_upload_part_size(min_upload_part_size_)
        , max_single_part_upload_size(max_single_part_upload_size_)
        , format_settings(format_settings_)
    {
        std::vector<ASTPtr> arguments(1, partition_by);
        ASTPtr partition_by_string = makeASTFunction(FunctionToString::name, std::move(arguments));

        auto syntax_result = TreeRewriter(context).analyze(partition_by_string, sample_block.getNamesAndTypesList());
        partition_by_expr = ExpressionAnalyzer(partition_by_string, syntax_result, context).getActions(false);
        partition_by_column_name = partition_by_string->getColumnName();
    }

    String getName() const override { return "PartitionedStorageS3Sink"; }

    void consume(Chunk chunk) override
    {
        const auto & columns = chunk.getColumns();

        Block block_with_partition_by_expr = sample_block.cloneWithoutColumns();
        block_with_partition_by_expr.setColumns(columns);
        partition_by_expr->execute(block_with_partition_by_expr);

        const auto * column = block_with_partition_by_expr.getByName(partition_by_column_name).column.get();

        std::unordered_map<String, size_t> sub_chunks_indices;
        IColumn::Selector selector;
        for (size_t row = 0; row < chunk.getNumRows(); ++row)
        {
            auto value = column->getDataAt(row);
            auto [it, inserted] = sub_chunks_indices.emplace(value, sub_chunks_indices.size());
            selector.push_back(it->second);
        }

        Chunks sub_chunks;
        sub_chunks.reserve(sub_chunks_indices.size());
        for (size_t column_index = 0; column_index < columns.size(); ++column_index)
        {
            MutableColumns column_sub_chunks = columns[column_index]->scatter(sub_chunks_indices.size(), selector);
            if (column_index == 0) /// Set sizes for sub-chunks.
            {
                for (const auto & column_sub_chunk : column_sub_chunks)
                {
                    sub_chunks.emplace_back(Columns(), column_sub_chunk->size());
                }
            }
            for (size_t sub_chunk_index = 0; sub_chunk_index < column_sub_chunks.size(); ++sub_chunk_index)
            {
                sub_chunks[sub_chunk_index].addColumn(std::move(column_sub_chunks[sub_chunk_index]));
            }
        }

        for (const auto & [partition_id, sub_chunk_index] : sub_chunks_indices)
        {
            getSinkForPartition(partition_id)->consume(std::move(sub_chunks[sub_chunk_index]));
        }
    }

    void onFinish() override
    {
        for (auto & [partition_id, sink] : sinks)
        {
            sink->onFinish();
        }
    }

private:
    using SinkPtr = std::shared_ptr<StorageS3Sink>;

    const String format;
    const Block sample_block;
    ContextPtr context;
    const CompressionMethod compression_method;
    std::shared_ptr<Aws::S3::S3Client> client;
    const String bucket;
    const String key;
    size_t min_upload_part_size;
    size_t max_single_part_upload_size;
    std::optional<FormatSettings> format_settings;

    ExpressionActionsPtr partition_by_expr;
    String partition_by_column_name;

    std::unordered_map<String, SinkPtr> sinks;

    static String replaceWildcards(const String & haystack, const String & partition_id)
    {
        return boost::replace_all_copy(haystack, PARTITION_ID_WILDCARD, partition_id);
    }

    SinkPtr getSinkForPartition(const String & partition_id)
    {
        auto it = sinks.find(partition_id);
        if (it == sinks.end())
        {
            auto partition_bucket = replaceWildcards(bucket, partition_id);
            validateBucket(partition_bucket);

            auto partition_key = replaceWildcards(key, partition_id);
            validateKey(partition_key);

            std::tie(it, std::ignore) = sinks.emplace(partition_id, std::make_shared<StorageS3Sink>(
                format,
                sample_block,
                context,
                format_settings,
                compression_method,
                client,
                partition_bucket,
                partition_key,
                min_upload_part_size,
                max_single_part_upload_size
            ));
        }

        return it->second;
    }

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

    static void validatePartitionKey(const StringRef & str, bool allow_slash)
    {
        for (const char * i = str.data; i != str.data + str.size; ++i)
        {
            if (static_cast<UInt8>(*i) < 0x20 || *i == '{' || *i == '}' || *i == '*' || *i == '?' || (!allow_slash && *i == '/'))
            {
                /// Need to convert to UInt32 because UInt8 can't be passed to format due to "mixing character types is disallowed".
                UInt32 invalid_char_byte = static_cast<UInt32>(static_cast<UInt8>(*i));
                throw DB::Exception(
                    ErrorCodes::CANNOT_PARSE_TEXT, "Illegal character '\\x{:02x}' in partition id starting with '{}'",
                    invalid_char_byte, StringRef(str.data, i - str.data));
            }
        }
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
    UInt64 max_single_part_upload_size_,
    UInt64 max_connections_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    const String & compression_method_,
    bool distributed_processing_)
    : IStorage(table_id_)
    , client_auth{uri_, access_key_id_, secret_access_key_, max_connections_, {}, {}} /// Client and settings will be updated later
    , format_name(format_name_)
    , max_single_read_retries(max_single_read_retries_)
    , min_upload_part_size(min_upload_part_size_)
    , max_single_part_upload_size(max_single_part_upload_size_)
    , compression_method(compression_method_)
    , name(uri_.storage_name)
    , distributed_processing(distributed_processing_)
    , format_settings(format_settings_)
{
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(uri_.uri);
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
    updateClientAndAuthSettings(context_, client_auth);
}


Pipe StorageS3::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
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

    std::shared_ptr<StorageS3Source::IteratorWrapper> iterator_wrapper{nullptr};
    if (distributed_processing)
    {
        iterator_wrapper = std::make_shared<StorageS3Source::IteratorWrapper>(
            [callback = local_context->getReadTaskCallback()]() -> String {
                return callback();
        });
    }
    else
    {
        /// Iterate through disclosed globs and make a source for each file
        auto glob_iterator = std::make_shared<StorageS3Source::DisclosedGlobIterator>(*client_auth.client, client_auth.uri);
        iterator_wrapper = std::make_shared<StorageS3Source::IteratorWrapper>([glob_iterator]()
        {
            return glob_iterator->next();
        });
    }

    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageS3Source>(
            need_path_column,
            need_file_column,
            format_name,
            getName(),
            metadata_snapshot->getSampleBlock(),
            local_context,
            format_settings,
            metadata_snapshot->getColumns(),
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
    auto chosen_compression_method = chooseCompressionMethod(client_auth.uri.key, compression_method);
    bool has_wildcards = client_auth.uri.bucket.find(PARTITION_ID_WILDCARD) != String::npos || client_auth.uri.key.find(PARTITION_ID_WILDCARD) != String::npos;
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query);

    bool is_partitioned_implementation = insert_query && insert_query->partition_by && has_wildcards;

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedStorageS3Sink>(
            insert_query->partition_by,
            format_name,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            client_auth.client,
            client_auth.uri.bucket,
            client_auth.uri.key,
            min_upload_part_size,
            max_single_part_upload_size);
    }
    else
    {
        return std::make_shared<StorageS3Sink>(
            format_name,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            client_auth.client,
            client_auth.uri.bucket,
            client_auth.uri.key,
            min_upload_part_size,
            max_single_part_upload_size);
    }
}


void StorageS3::truncate(const ASTPtr & /* query */, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    updateClientAndAuthSettings(local_context, client_auth);

    Aws::S3::Model::ObjectIdentifier obj;
    obj.SetKey(client_auth.uri.key);

    Aws::S3::Model::Delete delkeys;
    delkeys.AddObjects(std::move(obj));

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

void registerStorageS3Impl(const String & name, StorageFactory & factory)
{
    factory.registerStorage(name, [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 2 || engine_args.size() > 5)
            throw Exception(
                "Storage S3 requires 2 to 5 arguments: url, [access_key_id, secret_access_key], name of used format and [compression_method].",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

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
                {
                    user_format_settings.set(change.name, change.value);
                }
            }

            // Apply changes from SETTINGS clause, with validation.
            user_format_settings.applyChanges(args.storage_def->settings->changes);
            format_settings = getFormatSettings(args.getContext(), user_format_settings);
        }
        else
        {
            format_settings = getFormatSettings(args.getContext());
        }

        String url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        Poco::URI uri (url);
        S3::URI s3_uri (uri);

        String access_key_id;
        String secret_access_key;
        if (engine_args.size() >= 4)
        {
            access_key_id = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
            secret_access_key = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        }

        UInt64 max_single_read_retries = args.getLocalContext()->getSettingsRef().s3_max_single_read_retries;
        UInt64 min_upload_part_size = args.getLocalContext()->getSettingsRef().s3_min_upload_part_size;
        UInt64 max_single_part_upload_size = args.getLocalContext()->getSettingsRef().s3_max_single_part_upload_size;
        UInt64 max_connections = args.getLocalContext()->getSettingsRef().s3_max_connections;

        String compression_method;
        String format_name;
        if (engine_args.size() == 3 || engine_args.size() == 5)
        {
            compression_method = engine_args.back()->as<ASTLiteral &>().value.safeGet<String>();
            format_name = engine_args[engine_args.size() - 2]->as<ASTLiteral &>().value.safeGet<String>();
        }
        else
        {
            compression_method = "auto";
            format_name = engine_args.back()->as<ASTLiteral &>().value.safeGet<String>();
        }

        return StorageS3::create(
            s3_uri,
            access_key_id,
            secret_access_key,
            args.table_id,
            format_name,
            max_single_read_retries,
            min_upload_part_size,
            max_single_part_upload_size,
            max_connections,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
            format_settings,
            compression_method);
    },
    {
        .supports_settings = true,
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
        {"_path", std::make_shared<DataTypeString>()},
        {"_file", std::make_shared<DataTypeString>()}
    };
}

bool StorageS3::supportsPartitionBy() const
{
    return true;
}

}

#endif
