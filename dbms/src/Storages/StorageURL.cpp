#include <Storages/StorageFactory.h>
#include <Storages/StorageURL.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromHTTP.h>

#include <Formats/FormatFactory.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>

#include <Poco/Net/HTTPRequest.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

IStorageURLBase::IStorageURLBase(const Poco::URI & uri_,
    const Context & context_,
    const std::string & table_name_,
    const String & format_name_,
    const ColumnsDescription & columns_)
    : IStorage(columns_), uri(uri_), context_global(context_), format_name(format_name_), table_name(table_name_)
{
}

namespace
{
    class StorageURLBlockInputStream : public IProfilingBlockInputStream
    {
    public:
        StorageURLBlockInputStream(const Poco::URI & uri,
            const std::string & method,
            std::function<void(std::ostream &)> callback,
            const String & format,
            const String & name_,
            const Block & sample_block,
            const Context & context,
            size_t max_block_size,
            const ConnectionTimeouts & timeouts)
            : name(name_)
        {
            read_buf = std::make_unique<ReadWriteBufferFromHTTP>(uri, method, callback, timeouts);

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
        std::unique_ptr<ReadWriteBufferFromHTTP> read_buf;
        BlockInputStreamPtr reader;
    };

    class StorageURLBlockOutputStream : public IBlockOutputStream
    {
    public:
        StorageURLBlockOutputStream(const Poco::URI & uri,
            const String & format,
            const Block & sample_block_,
            const Context & context,
            const ConnectionTimeouts & timeouts)
            : sample_block(sample_block_)
        {
            write_buf = std::make_unique<WriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, timeouts);
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
        std::unique_ptr<WriteBufferFromHTTP> write_buf;
        BlockOutputStreamPtr writer;
    };
}


std::string IStorageURLBase::getReadMethod() const
{
    return Poco::Net::HTTPRequest::HTTP_GET;
}

std::vector<std::pair<std::string, std::string>> IStorageURLBase::getReadURIParams(const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    return {};
}

std::function<void(std::ostream &)> IStorageURLBase::getReadPOSTDataCallback(const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    return nullptr;
}


BlockInputStreams IStorageURLBase::read(const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    auto request_uri = uri;
    auto params = getReadURIParams(column_names, query_info, context, processed_stage, max_block_size);
    for (const auto & [param, value] : params)
        request_uri.addQueryParameter(param, value);

    return {std::make_shared<StorageURLBlockInputStream>(request_uri,
        getReadMethod(),
        getReadPOSTDataCallback(column_names, query_info, context, processed_stage, max_block_size),
        format_name,
        getName(),
        getSampleBlock(),
        context,
        max_block_size,
        ConnectionTimeouts::getHTTPTimeouts(context.getSettingsRef()))};
}

void IStorageURLBase::rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & /*new_table_name*/) {}

BlockOutputStreamPtr IStorageURLBase::write(const ASTPtr & /*query*/, const Settings & /*settings*/)
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
