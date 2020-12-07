#include <Storages/StorageFactory.h>
#include <Storages/StorageURL.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>

#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromHTTP.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>

#include <Poco/Net/HTTPRequest.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

IStorageURLBase::IStorageURLBase(
    const Poco::URI & uri_,
    const Context & context_,
    const StorageID & table_id_,
    const String & format_name_,
    const std::optional<FormatSettings> & format_settings_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & compression_method_)
    : IStorage(table_id_)
    , uri(uri_)
    , context_global(context_)
    , compression_method(compression_method_)
    , format_name(format_name_)
    , format_settings(format_settings_)
{
    context_global.getRemoteHostFilter().checkURL(uri);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}

namespace
{
    class StorageURLSource : public SourceWithProgress
    {
    public:
        StorageURLSource(const Poco::URI & uri,
            const std::string & method,
            std::function<void(std::ostream &)> callback,
            const String & format,
            const std::optional<FormatSettings> & format_settings,
            String name_,
            const Block & sample_block,
            const Context & context,
            const ColumnsDescription & columns,
            UInt64 max_block_size,
            const ConnectionTimeouts & timeouts,
            const CompressionMethod compression_method)
            : SourceWithProgress(sample_block), name(std::move(name_))
        {
            ReadWriteBufferFromHTTP::HTTPHeaderEntries header;

            // Propagate OpenTelemetry trace context, if any, downstream.
            if (CurrentThread::isInitialized())
            {
                const auto & thread_trace_context = CurrentThread::get().thread_trace_context;
                if (thread_trace_context.trace_id)
                {
                    header.emplace_back("traceparent",
                        thread_trace_context.composeTraceparentHeader());

                    if (!thread_trace_context.tracestate.empty())
                    {
                        header.emplace_back("tracestate",
                            thread_trace_context.tracestate);
                    }
                }
            }

            read_buf = wrapReadBufferWithCompressionMethod(
                std::make_unique<ReadWriteBufferFromHTTP>(
                    uri,
                    method,
                    std::move(callback),
                    timeouts,
                    context.getSettingsRef().max_http_get_redirects,
                    Poco::Net::HTTPBasicCredentials{},
                    DBMS_DEFAULT_BUFFER_SIZE,
                    header,
                    context.getRemoteHostFilter()),
                compression_method);

            reader = FormatFactory::instance().getInput(format, *read_buf,
                sample_block, context, max_block_size, format_settings);
            reader = std::make_shared<AddingDefaultsBlockInputStream>(reader,
                columns, context);
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
                reader->readPrefix();

            initialized = true;

            if (auto block = reader->read())
                return Chunk(block.getColumns(), block.rows());

            reader->readSuffix();
            reader.reset();

            return {};
        }

    private:
        String name;
        std::unique_ptr<ReadBuffer> read_buf;
        BlockInputStreamPtr reader;
        bool initialized = false;
    };
}

StorageURLBlockOutputStream::StorageURLBlockOutputStream(const Poco::URI & uri,
        const String & format,
        const std::optional<FormatSettings> & format_settings,
        const Block & sample_block_,
        const Context & context,
        const ConnectionTimeouts & timeouts,
        const CompressionMethod compression_method)
        : sample_block(sample_block_)
{
    write_buf = wrapWriteBufferWithCompressionMethod(
            std::make_unique<WriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, timeouts),
            compression_method, 3);
    writer = FormatFactory::instance().getOutput(format, *write_buf, sample_block,
        context, {} /* write callback */, format_settings);
}


void StorageURLBlockOutputStream::write(const Block & block)
{
    writer->write(block);
}

void StorageURLBlockOutputStream::writePrefix()
{
    writer->writePrefix();
}

void StorageURLBlockOutputStream::writeSuffix()
{
    writer->writeSuffix();
    writer->flush();
    write_buf->finalize();
}


std::string IStorageURLBase::getReadMethod() const
{
    return Poco::Net::HTTPRequest::HTTP_GET;
}

std::vector<std::pair<std::string, std::string>> IStorageURLBase::getReadURIParams(
    const Names & /*column_names*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    return {};
}

std::function<void(std::ostream &)> IStorageURLBase::getReadPOSTDataCallback(
    const Names & /*column_names*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    return nullptr;
}


Pipe IStorageURLBase::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    auto request_uri = uri;
    auto params = getReadURIParams(column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size);
    for (const auto & [param, value] : params)
        request_uri.addQueryParameter(param, value);

    return Pipe(std::make_shared<StorageURLSource>(
        request_uri,
        getReadMethod(),
        getReadPOSTDataCallback(
            column_names, metadata_snapshot, query_info,
            context, processed_stage, max_block_size),
        format_name,
        format_settings,
        getName(),
        getHeaderBlock(column_names, metadata_snapshot),
        context,
        metadata_snapshot->getColumns(),
        max_block_size,
        ConnectionTimeouts::getHTTPTimeouts(context),
        chooseCompressionMethod(request_uri.getPath(), compression_method)));
}

BlockOutputStreamPtr IStorageURLBase::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<StorageURLBlockOutputStream>(uri, format_name,
        format_settings, metadata_snapshot->getSampleBlock(), context_global,
        ConnectionTimeouts::getHTTPTimeouts(context_global),
        chooseCompressionMethod(uri.toString(), compression_method));
}

void registerStorageURL(StorageFactory & factory)
{
    factory.registerStorage("URL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2 && engine_args.size() != 3)
            throw Exception(
                "Storage URL requires 2 or 3 arguments: url, name of used format and optional compression method.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);

        String url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        Poco::URI uri(url);

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);

        String format_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        String compression_method;
        if (engine_args.size() == 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);
            compression_method = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        }
        else
        {
            compression_method = "auto";
        }

        // Use format settings from global server context + settings from
        // the SETTINGS clause of the create query. Settings from current
        // session and user are ignored.
        FormatSettings format_settings;
        if (args.storage_def->settings)
        {
            FormatFactorySettings user_format_settings;

            // Apply changed settings from global context, but ignore the
            // unknown ones, because we only have the format settings here.
            const auto & changes = args.context.getSettingsRef().changes();
            for (const auto & change : changes)
            {
                if (user_format_settings.has(change.name))
                {
                    user_format_settings.set(change.name, change.value);
                }
            }

            // Apply changes from SETTINGS clause, with validation.
            user_format_settings.applyChanges(args.storage_def->settings->changes);

            format_settings = getFormatSettings(args.context,
                user_format_settings);
        }
        else
        {
            format_settings = getFormatSettings(args.context);
        }

        return StorageURL::create(
            uri,
            args.table_id,
            format_name,
            format_settings,
            args.columns, args.constraints, args.context,
            compression_method);
    },
    {
        .supports_settings = true,
        .source_access_type = AccessType::URL,
    });
}
}
