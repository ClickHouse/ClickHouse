#include <Storages/StorageFactory.h>
#include <Storages/StorageURL.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromOStream.h>

#include <Formats/FormatFactory.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>

#include <Poco/Net/HTTPRequest.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
};

StorageURL::StorageURL(const Poco::URI & uri_,
    const std::string & table_name_,
    const String & format_name_,
    const ColumnsDescription & columns_,
    Context & context_)
    : IStorage(columns_), uri(uri_), format_name(format_name_), table_name(table_name_), context_global(context_)
{
}

namespace
{
    class StorageURLBlockInputStream : public IProfilingBlockInputStream
    {
    public:
        StorageURLBlockInputStream(const Poco::URI & uri,
            const String & format,
            const String & name_,
            const Block & sample_block,
            const Context & context,
            size_t max_block_size,
            const ConnectionTimeouts & timeouts)
            : name(name_)
        {
            read_buf = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_GET, nullptr, timeouts);

            reader = FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size);
        }

        ~StorageURLBlockInputStream() override {}

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
        std::unique_ptr<ReadWriteBufferFromHTTP> read_buf;
        BlockInputStreamPtr reader;
    };

    class StorageURLBlockOutputStream : public IBlockOutputStream
    {
    public:
        StorageURLBlockOutputStream(const Poco::URI & uri_,
            const String & format_,
            const Block & sample_block_,
            Context & context_,
            const ConnectionTimeouts & timeouts_)
            : global_context(context_), uri(uri_), format(format_), sample_block(sample_block_), timeouts(timeouts_)
        {
        }

        ~StorageURLBlockOutputStream() {}

        Block getHeader() const override
        {
            return sample_block;
        }

        void write(const Block & block) override
        {
            ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & ostr) {
                WriteBufferFromOStream out_buffer(ostr);
                auto writer = FormatFactory::instance().getOutput(format, out_buffer, sample_block, global_context);
                writer->writePrefix();
                writer->write(block);
                writer->writeSuffix();
                writer->flush();
            };
            ReadWriteBufferFromHTTP(uri, Poco::Net::HTTPRequest::HTTP_POST, out_stream_callback, timeouts); // just for request
        }

    private:
        Context & global_context;
        Poco::URI uri;
        String format;
        Block sample_block;
        ConnectionTimeouts timeouts;
    };
}
BlockInputStreams StorageURL::read(const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    return {std::make_shared<StorageURLBlockInputStream>(uri,
        format_name,
        getName(),
        getSampleBlock(),
        context,
        max_block_size,
        ConnectionTimeouts::getHTTPTimeouts(context.getSettingsRef()))};
}

void StorageURL::rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & /*new_table_name*/) {}

BlockOutputStreamPtr StorageURL::write(const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    return std::make_shared<StorageURLBlockOutputStream>(
        uri, format_name, getSampleBlock(), context_global, ConnectionTimeouts::getHTTPTimeouts(context_global.getSettingsRef()));
}
void registerStorageURL(StorageFactory & factory)
{
    factory.registerStorage("URL", [](const StorageFactory::Arguments & args) {
        ASTs & engine_args = args.engine_args;

        if (!(engine_args.size() == 1 || engine_args.size() == 2))
            throw Exception(
                "Storage URL requires exactly 2 arguments: url and name of used format.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);

        String url = static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>();
        Poco::URI uri(url);

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);

        String format_name = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();

        return StorageURL::create(uri, args.table_name, format_name, args.columns, args.context);
    });
}
}
