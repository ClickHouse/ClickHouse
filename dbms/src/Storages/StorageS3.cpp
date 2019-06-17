#include <Storages/StorageFactory.h>
#include <Storages/StorageS3.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>

#include <Formats/FormatFactory.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>

#include <Poco/Net/HTTPRequest.h>


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
        StorageS3BlockInputStream(const Poco::URI & uri,
            const String & format,
            const String & name_,
            const Block & sample_block,
            const Context & context,
            UInt64 max_block_size,
            const ConnectionTimeouts & timeouts)
            : name(name_)
        {
            read_buf = std::make_unique<ReadBufferFromS3>(uri, timeouts);

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
        std::unique_ptr<ReadBufferFromS3> read_buf;
        BlockInputStreamPtr reader;
    };

    class StorageS3BlockOutputStream : public IBlockOutputStream
    {
    public:
        StorageS3BlockOutputStream(const Poco::URI & uri,
            const String & format,
            const Block & sample_block_,
            const Context & context,
            const ConnectionTimeouts & timeouts)
            : sample_block(sample_block_)
        {
            write_buf = std::make_unique<WriteBufferFromS3>(uri, timeouts);
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
        std::unique_ptr<WriteBufferFromS3> write_buf;
        BlockOutputStreamPtr writer;
    };
}


BlockInputStreams StorageS3::read(const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    BlockInputStreamPtr block_input = std::make_shared<StorageS3BlockInputStream>(uri,
        format_name,
        getName(),
        getHeaderBlock(column_names),
        context,
        max_block_size,
        ConnectionTimeouts::getHTTPTimeouts(context));

    auto column_defaults = getColumns().getDefaults();
    if (column_defaults.empty())
        return {block_input};
    return {std::make_shared<AddingDefaultsBlockInputStream>(block_input, column_defaults, context)};
}

void StorageS3::rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & /*new_table_name*/) {}

BlockOutputStreamPtr StorageS3::write(const ASTPtr & /*query*/, const Context & /*context*/)
{
    return std::make_shared<StorageS3BlockOutputStream>(
        uri, format_name, getSampleBlock(), context_global, ConnectionTimeouts::getHTTPTimeouts(context_global));
}

void registerStorageS3(StorageFactory & factory)
{
    factory.registerStorage("S3", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (!(engine_args.size() == 1 || engine_args.size() == 2))
            throw Exception(
                "Storage S3 requires exactly 2 arguments: url and name of used format.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);

        String url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        Poco::URI uri(url);

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);

        String format_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        return StorageS3::create(uri, args.table_name, format_name, args.columns, args.context);
    });
}
}
