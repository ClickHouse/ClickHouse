#include <Storages/StorageS3.h>
#include <Storages/StorageFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromHTTP.h>

#include <Formats/FormatFactory.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

#include <Poco/Path.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPRequest.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int INCORRECT_FILE_NAME;
    extern const int FILE_DOESNT_EXIST;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}


StorageS3::StorageS3(
        const std::string & table_uri_,
        const std::string & table_name_,
        const std::string & format_name_,
        const ColumnsDescription & columns_,
        Context & context_)
    : IStorage(columns_)
    , table_name(table_name_)
    , format_name(format_name_)
    , context_global(context_)
    , uri(table_uri_)
{
}


class StorageS3BlockInputStream : public IBlockInputStream
{
public:
    StorageS3BlockInputStream(const Poco::URI & uri,
        const std::string & method,
        std::function<void(std::ostream &)> callback,
        const String & format,
        const String & name_,
        const Block & sample_block,
        const Context & context,
        UInt64 max_block_size,
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


BlockInputStreams StorageS3::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    auto request_uri = uri;

    BlockInputStreamPtr block_input = std::make_shared<StorageS3BlockInputStream>(request_uri,
        Poco::Net::HTTPRequest::HTTP_GET,
        nullptr,
        //getReadPOSTDataCallback(column_names, query_info, context, processed_stage, max_block_size),
        format_name,
        getName(),
        getSampleBlockForColumns(column_names),
        context,
        max_block_size,
        ConnectionTimeouts::getHTTPTimeouts(context));


    auto column_defaults = getColumns().getDefaults();
    if (column_defaults.empty())
        return {block_input};
    return {std::make_shared<AddingDefaultsBlockInputStream>(block_input, column_defaults, context)};
}


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

BlockOutputStreamPtr StorageS3::write(
    const ASTPtr & /*query*/,
    const Context & /*context*/)
{
    return std::make_shared<StorageS3BlockOutputStream>(
        uri, format_name, getSampleBlock(), context_global, ConnectionTimeouts::getHTTPTimeouts(context_global));
}


void StorageS3::drop()
{
    /// Extra actions are not required.
}


void StorageS3::rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & /*new_table_name*/) {}


void registerStorageS3(StorageFactory & factory)
{
    factory.registerStorage("S3", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (!(engine_args.size() == 2))
            throw Exception(
                "Storage S3 requires 2 arguments: name of used format and source.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);
        String format_name = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        String source_path;
        if (const auto * literal = engine_args[1]->as<ASTLiteral>())
        {
            auto type = literal->value.getType();
            if (type == Field::Types::String)
            {
                source_path = literal->value.get<String>();
                return StorageS3::create(
                    source_path,
                    args.table_name, format_name, args.columns,
                    args.context);
            }
        }

        throw Exception("Unknown entity in first arg of S3 storage constructor, String expected.",
            ErrorCodes::UNKNOWN_IDENTIFIER);
    });
}

}
