#include <Common/config.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>

#include <aws/s3/S3Client.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{
    class StorageS3BlockInputStream : public IBlockInputStream
    {
    public:
        StorageS3BlockInputStream(
            const String & format,
            const String & name_,
            const Block & sample_block,
            const Context & context,
            UInt64 max_block_size,
            const CompressionMethod compression_method,
            const std::shared_ptr<Aws::S3::S3Client> & client,
            const String & bucket,
            const String & key)
            : name(name_)
        {
            read_buf = getReadBuffer<ReadBufferFromS3>(compression_method, client, bucket, key);
            reader = FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size);
        }

        String getName() const override
        {
            return name;
        }

        Block readImpl() override
        {
            return reader->read();
        }

        Block getHeader() const override
        {
            return reader->getHeader();
        }

        void readPrefixImpl() override
        {
            reader->readPrefix();
        }

        void readSuffixImpl() override
        {
            reader->readSuffix();
        }

    private:
        String name;
        std::unique_ptr<ReadBuffer> read_buf;
        BlockInputStreamPtr reader;
    };

    class StorageS3BlockOutputStream : public IBlockOutputStream
    {
    public:
        StorageS3BlockOutputStream(
            const String & format,
            UInt64 min_upload_part_size,
            const Block & sample_block_,
            const Context & context,
            const CompressionMethod compression_method,
            const std::shared_ptr<Aws::S3::S3Client> & client,
            const String & bucket,
            const String & key)
            : sample_block(sample_block_)
        {
            write_buf = getWriteBuffer<WriteBufferFromS3>(compression_method, client, bucket, key, min_upload_part_size);
            writer = FormatFactory::instance().getOutput(format, *write_buf, sample_block, context);
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


StorageS3::StorageS3(const S3::URI & uri_,
    const String & access_key_id_,
    const String & secret_access_key_,
    const std::string & database_name_,
    const std::string & table_name_,
    const String & format_name_,
    UInt64 min_upload_part_size_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    Context & context_,
    const String & compression_method_ = "")
    : IStorage(columns_)
    , uri(uri_)
    , context_global(context_)
    , format_name(format_name_)
    , database_name(database_name_)
    , table_name(table_name_)
    , min_upload_part_size(min_upload_part_size_)
    , compression_method(compression_method_)
    , client(S3::ClientFactory::instance().create(uri_.endpoint, access_key_id_, secret_access_key_))
{
    context_global.getRemoteHostFilter().checkURL(uri_.uri);
    setColumns(columns_);
    setConstraints(constraints_);
}


BlockInputStreams StorageS3::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    BlockInputStreamPtr block_input = std::make_shared<StorageS3BlockInputStream>(
        format_name,
        getName(),
        getHeaderBlock(column_names),
        context,
        max_block_size,
        IStorage::chooseCompressionMethod(uri.endpoint, compression_method),
        client,
        uri.bucket,
        uri.key);

    auto column_defaults = getColumns().getDefaults();
    if (column_defaults.empty())
        return {block_input};
    return {std::make_shared<AddingDefaultsBlockInputStream>(block_input, column_defaults, context)};
}

void StorageS3::rename(const String & /*new_path_to_db*/, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &)
{
    table_name = new_table_name;
    database_name = new_database_name;
}

BlockOutputStreamPtr StorageS3::write(const ASTPtr & /*query*/, const Context & /*context*/)
{
    return std::make_shared<StorageS3BlockOutputStream>(
        format_name, min_upload_part_size, getSampleBlock(), context_global,
        IStorage::chooseCompressionMethod(uri.endpoint, compression_method),
        client, uri.bucket, uri.key);
}

void registerStorageS3(StorageFactory & factory)
{
    factory.registerStorage("S3", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 2 || engine_args.size() > 5)
            throw Exception(
                "Storage S3 requires 2 to 5 arguments: url, [access_key_id, secret_access_key], name of used format and [compression_method].", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < engine_args.size(); ++i)
            engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.local_context);

        String url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        Poco::URI uri (url);
        S3::URI s3_uri (uri);

        String format_name = engine_args[engine_args.size() - 1]->as<ASTLiteral &>().value.safeGet<String>();

        String access_key_id;
        String secret_access_key;
        if (engine_args.size() >= 4)
        {
            access_key_id = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
            secret_access_key = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        }

        UInt64 min_upload_part_size = args.local_context.getSettingsRef().s3_min_upload_part_size;

        String compression_method;
        if (engine_args.size() == 3 || engine_args.size() == 5)
            compression_method = engine_args.back()->as<ASTLiteral &>().value.safeGet<String>();
        else
            compression_method = "auto";

        return StorageS3::create(s3_uri, access_key_id, secret_access_key, args.database_name, args.table_name, format_name, min_upload_part_size, args.columns, args.constraints, args.context);
    });
}

}

#endif
