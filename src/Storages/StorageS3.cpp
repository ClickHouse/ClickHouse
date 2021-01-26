#include <Common/config.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageS3Settings.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadBufferFanIn.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/narrowBlockInputStreams.h>

#include <DataTypes/DataTypeString.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include <Common/parseGlobs.h>
#include <Common/quoteString.h>
#include <re2/re2.h>

#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/Pipe.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_EXPRESSION;
    extern const int S3_ERROR;
}


namespace
{
    class StorageS3Source : public SourceWithProgress
    {
    public:

        static Block getHeader(Block sample_block, bool with_path_column, bool with_file_column)
        {
            if (with_path_column)
                sample_block.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_path"});
            if (with_file_column)
                sample_block.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_file"});

            return sample_block;
        }

        StorageS3Source(
            bool need_path,
            bool need_file,
            const String & format,
            String name_,
            const Block & sample_block,
            const Context & context,
            const ColumnsDescription & columns,
            UInt64 max_block_size,
            const CompressionMethod compression_method,
            const std::shared_ptr<Aws::S3::S3Client> & client,
            const String & bucket,
            const String & key,
            size_t max_read_threads)
            : SourceWithProgress(getHeader(sample_block, need_path, need_file))
            , name(std::move(name_))
            , with_file_column(need_file)
            , with_path_column(need_path)
            , file_path(bucket + "/" + key)
        {
            size_t object_size = DB::S3::getObjectSize(client, bucket, key);


            std::unique_ptr<ReadBuffer> s3_read_buf;
            if (max_read_threads <= 1 || object_size <= 4 * DBMS_DEFAULT_BUFFER_SIZE)
            {
                LOG_TRACE(&Poco::Logger::get("StorageS3Source"), "Downloading from S3 in single thread");
                s3_read_buf = std::make_unique<ReadBufferFromS3>(client, bucket, key);
            }
            else
            {
                /// Download two segments per each worker.
                /// Not so much, but better than one, because some worker can steal segment if other one is slow
                size_t number_of_ranges = max_read_threads * 2;
                size_t split_size = (object_size + number_of_ranges) / number_of_ranges;
                LOG_TRACE(&Poco::Logger::get("StorageS3Source"),
                          "Downloading from S3 in {} threads. Object size: {}, Range size: {}",
                          max_read_threads, object_size, split_size);
                auto mul_s3_read_buf = std::make_unique<ReadBufferFanIn>(max_read_threads);
                for (size_t from_range = 0; from_range < object_size; from_range += split_size + 1)
                {
                    auto subreader = std::make_shared<ReadBufferFromS3>(client, bucket, key);
                    size_t to_range = std::min(object_size - 1, from_range + split_size);
                    subreader->setRange(from_range, to_range);
                    mul_s3_read_buf->addReader(subreader);
                }
                mul_s3_read_buf->start();
                s3_read_buf = std::move(mul_s3_read_buf);
            }

            read_buf = wrapReadBufferWithCompressionMethod(std::move(s3_read_buf), compression_method);

            auto input_format = FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size);
            reader = std::make_shared<InputStreamFromInputFormat>(input_format);

            if (columns.hasDefaults())
                reader = std::make_shared<AddingDefaultsBlockInputStream>(reader, columns, context);
        }

        String getName() const override
        {
            return name;
        }

        Chunk generate() override
        {
            if (!reader)
                return {};

            if (!initialized)
            {
                reader->readSuffix();
                initialized = true;
            }

            if (auto block = reader->read())
            {
                auto columns = block.getColumns();
                UInt64 num_rows = block.rows();

                if (with_path_column)
                    columns.push_back(DataTypeString().createColumnConst(num_rows, file_path)->convertToFullColumnIfConst());
                if (with_file_column)
                {
                    size_t last_slash_pos = file_path.find_last_of('/');
                    columns.push_back(DataTypeString().createColumnConst(num_rows, file_path.substr(
                            last_slash_pos + 1))->convertToFullColumnIfConst());
                }

                return Chunk(std::move(columns), num_rows);
            }

            reader.reset();

            return {};
        }

    private:
        String name;
        std::unique_ptr<ReadBuffer> read_buf;
        BlockInputStreamPtr reader;
        bool initialized = false;
        bool with_file_column = false;
        bool with_path_column = false;
        String file_path;
    };

    class StorageS3BlockOutputStream : public IBlockOutputStream
    {
    public:
        StorageS3BlockOutputStream(
            const String & format,
            const Block & sample_block_,
            const Context & context,
            const CompressionMethod compression_method,
            const std::shared_ptr<Aws::S3::S3Client> & client,
            const String & bucket,
            const String & key,
            size_t min_upload_part_size,
            size_t max_single_part_upload_size)
            : sample_block(sample_block_)
        {
            write_buf = wrapWriteBufferWithCompressionMethod(
                std::make_unique<WriteBufferFromS3>(client, bucket, key, min_upload_part_size, max_single_part_upload_size), compression_method, 3);
            writer = FormatFactory::instance().getOutputStream(format, *write_buf, sample_block, context);
        }

        Block getHeader() const override
        {
            return sample_block;
        }

        void write(const Block & block) override
        {
            writer->write(block);
        }

        void writePrefix() override
        {
            writer->writePrefix();
        }

        void writeSuffix() override
        {
            writer->writeSuffix();
            writer->flush();
            write_buf->finalize();
        }

    private:
        Block sample_block;
        std::unique_ptr<WriteBuffer> write_buf;
        BlockOutputStreamPtr writer;
    };
}


StorageS3::StorageS3(
    const S3::URI & uri_,
    const String & access_key_id_,
    const String & secret_access_key_,
    const StorageID & table_id_,
    const String & format_name_,
    UInt64 min_upload_part_size_,
    UInt64 max_single_part_upload_size_,
    UInt64 max_connections_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_,
    const String & compression_method_)
    : IStorage(table_id_)
    , uri(uri_)
    , global_context(context_.getGlobalContext())
    , format_name(format_name_)
    , min_upload_part_size(min_upload_part_size_)
    , max_single_part_upload_size(max_single_part_upload_size_)
    , compression_method(compression_method_)
    , name(uri_.storage_name)
    , max_read_threads(context_.getSettings().max_threads)
{
    global_context.getRemoteHostFilter().checkURL(uri_.uri);
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    auto settings = context_.getStorageS3Settings().getSettings(uri.endpoint);
    Aws::Auth::AWSCredentials credentials(access_key_id_, secret_access_key_);
    if (access_key_id_.empty())
        credentials = Aws::Auth::AWSCredentials(std::move(settings.access_key_id), std::move(settings.secret_access_key));

    Aws::Client::ClientConfiguration client_configuration;
    client_configuration.endpointOverride = uri_.endpoint;
    client_configuration.maxConnections = max_connections_;

    client = S3::ClientFactory::instance().create(
        client_configuration,
        uri_.is_virtual_hosted_style,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        std::move(settings.headers),
        settings.use_environment_credentials.value_or(global_context.getConfigRef().getBool("s3.use_environment_credentials", false)),
        context_.getRemoteHostFilter(),
        context_.getGlobalContext().getSettingsRef().s3_max_redirects);
}


namespace
{
    /* "Recursive" directory listing with matched paths as a result.
 * Have the same method in StorageFile.
 */
Strings listFilesWithRegexpMatching(Aws::S3::S3Client & client, const S3::URI & globbed_uri)
{
    if (globbed_uri.bucket.find_first_of("*?{") != globbed_uri.bucket.npos)
    {
        throw Exception("Expression can not have wildcards inside bucket name", ErrorCodes::UNEXPECTED_EXPRESSION);
    }

    const String key_prefix = globbed_uri.key.substr(0, globbed_uri.key.find_first_of("*?{"));
    if (key_prefix.size() == globbed_uri.key.size())
    {
        return {globbed_uri.key};
    }

    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(globbed_uri.bucket);
    request.SetPrefix(key_prefix);

    re2::RE2 matcher(makeRegexpPatternFromGlobs(globbed_uri.key));
    Strings result;
    Aws::S3::Model::ListObjectsV2Outcome outcome;
    int page = 0;
    do
    {
        ++page;
        outcome = client.ListObjectsV2(request);
        if (!outcome.IsSuccess())
        {
            if (page > 1)
                throw Exception(ErrorCodes::S3_ERROR, "Could not list objects in bucket {} with prefix {}, page {}, S3 exception: {}, message: {}",
                            quoteString(request.GetBucket()), quoteString(request.GetPrefix()), page,
                            backQuote(outcome.GetError().GetExceptionName()), quoteString(outcome.GetError().GetMessage()));

            throw Exception(ErrorCodes::S3_ERROR, "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
                            quoteString(request.GetBucket()), quoteString(request.GetPrefix()),
                            backQuote(outcome.GetError().GetExceptionName()), quoteString(outcome.GetError().GetMessage()));
        }

        for (const auto & row : outcome.GetResult().GetContents())
        {
            String key = row.GetKey();
            if (re2::RE2::FullMatch(key, matcher))
                result.emplace_back(std::move(key));
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    }
    while (outcome.GetResult().GetIsTruncated());

    return result;
}

}


Pipe StorageS3::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
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

    for (const String & key : listFilesWithRegexpMatching(*client, uri))
        pipes.emplace_back(std::make_shared<StorageS3Source>(
            need_path_column,
            need_file_column,
            format_name,
            getName(),
            metadata_snapshot->getSampleBlock(),
            context,
            metadata_snapshot->getColumns(),
            max_block_size,
            chooseCompressionMethod(uri.endpoint, compression_method),
            client,
            uri.bucket,
            key,
            max_read_threads));

    auto pipe = Pipe::unitePipes(std::move(pipes));
    // It's possible to have many buckets read from s3, resize(num_streams) might open too many handles at the same time.
    // Using narrowPipe instead.
    narrowPipe(pipe, num_streams);
    return pipe;
}

BlockOutputStreamPtr StorageS3::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<StorageS3BlockOutputStream>(
        format_name,
        metadata_snapshot->getSampleBlock(),
        global_context,
        chooseCompressionMethod(uri.endpoint, compression_method),
        client,
        uri.bucket,
        uri.key,
        min_upload_part_size,
        max_single_part_upload_size);
}

void registerStorageS3Impl(const String & name, StorageFactory & factory)
{
    factory.registerStorage(name, [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 2 || engine_args.size() > 5)
            throw Exception(
                "Storage S3 requires 2 to 5 arguments: url, [access_key_id, secret_access_key], name of used format and [compression_method].", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

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

        UInt64 min_upload_part_size = args.local_context.getSettingsRef().s3_min_upload_part_size;
        UInt64 max_single_part_upload_size = args.local_context.getSettingsRef().s3_max_single_part_upload_size;
        UInt64 max_connections = args.local_context.getSettingsRef().s3_max_connections;

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
            min_upload_part_size,
            max_single_part_upload_size,
            max_connections,
            args.columns,
            args.constraints,
            args.context,
            compression_method
        );
    },
    {
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

}

#endif
