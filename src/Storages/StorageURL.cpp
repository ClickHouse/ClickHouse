#include <Storages/StorageURL.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTInsertQuery.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromHTTP.h>
#include <IO/WriteHelpers.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ConnectionTimeoutsContext.h>

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>

#include <Common/parseRemoteDescription.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Storages/PartitionedSink.h>

#include <Poco/Net/HTTPRequest.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <base/logger_useful.h>
#include <algorithm>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NETWORK_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}


static bool urlWithGlobs(const String & uri)
{
    return (uri.find('{') != std::string::npos && uri.find('}') != std::string::npos)
        || uri.find('|') != std::string::npos;
}


IStorageURLBase::IStorageURLBase(
    const String & uri_,
    ContextPtr context_,
    const StorageID & table_id_,
    const String & format_name_,
    const std::optional<FormatSettings> & format_settings_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    const String & compression_method_,
    const ReadWriteBufferFromHTTP::HTTPHeaderEntries & headers_,
    const String & http_method_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , uri(uri_)
    , compression_method(compression_method_)
    , format_name(format_name_)
    , format_settings(format_settings_)
    , headers(headers_)
    , http_method(http_method_)
    , partition_by(partition_by_)
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(format_name, uri, compression_method, headers, format_settings, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


namespace
{
    ReadWriteBufferFromHTTP::HTTPHeaderEntries getHeaders(
        const ReadWriteBufferFromHTTP::HTTPHeaderEntries & headers_)
    {
        ReadWriteBufferFromHTTP::HTTPHeaderEntries headers(headers_.begin(), headers_.end());
        // Propagate OpenTelemetry trace context, if any, downstream.
        if (CurrentThread::isInitialized())
        {
            const auto & thread_trace_context = CurrentThread::get().thread_trace_context;
            if (thread_trace_context.trace_id != UUID())
            {
                headers.emplace_back("traceparent",
                    thread_trace_context.composeTraceparentHeader());

                if (!thread_trace_context.tracestate.empty())
                {
                    headers.emplace_back("tracestate",
                        thread_trace_context.tracestate);
                }
            }
        }
        return headers;
    }


    class StorageURLSource : public SourceWithProgress
    {

    using URIParams = std::vector<std::pair<String, String>>;

    public:
        struct URIInfo
        {
            using FailoverOptions = std::vector<String>;
            std::vector<FailoverOptions> uri_list_to_read;
            std::atomic<size_t> next_uri_to_read = 0;
        };
        using URIInfoPtr = std::shared_ptr<URIInfo>;

        void onCancel() override
        {
            std::lock_guard lock(reader_mutex);
            if (reader)
                reader->cancel();
        }

        static void setCredentials(Poco::Net::HTTPBasicCredentials & credentials, const Poco::URI & request_uri)
        {
            const auto & user_info = request_uri.getUserInfo();
            if (!user_info.empty())
            {
                std::size_t n = user_info.find(':');
                if (n != std::string::npos)
                {
                    credentials.setUsername(user_info.substr(0, n));
                    credentials.setPassword(user_info.substr(n + 1));
                }
            }
        }

        StorageURLSource(
            URIInfoPtr uri_info_,
            const std::string & http_method,
            std::function<void(std::ostream &)> callback,
            const String & format,
            const std::optional<FormatSettings> & format_settings,
            String name_,
            const Block & sample_block,
            ContextPtr context,
            const ColumnsDescription & columns,
            UInt64 max_block_size,
            const ConnectionTimeouts & timeouts,
            const String & compression_method,
            const ReadWriteBufferFromHTTP::HTTPHeaderEntries & headers_ = {},
            const URIParams & params = {},
            bool glob_url = false)
            : SourceWithProgress(sample_block), name(std::move(name_))
            , uri_info(uri_info_)
        {
            auto headers = getHeaders(headers_);

            /// Lazy initialization. We should not perform requests in constructor, because we need to do it in query pipeline.
            initialize = [=, this](const URIInfo::FailoverOptions & uri_options)
            {
                if (uri_options.empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty url list");

                auto first_option = uri_options.begin();
                read_buf = getFirstAvailableURLReadBuffer(
                    first_option, uri_options.end(), context, params, http_method,
                    callback, timeouts, compression_method, credentials, headers, glob_url, uri_options.size() == 1);

                auto input_format = FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size, format_settings);
                QueryPipelineBuilder builder;
                builder.init(Pipe(input_format));

                builder.addSimpleTransform([&](const Block & cur_header)
                {
                    return std::make_shared<AddingDefaultsTransform>(cur_header, columns, *input_format, context);
                });

                pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
                reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
            };
        }

        String getName() const override
        {
            return name;
        }

        Chunk generate() override
        {
            while (true)
            {

                if (!reader)
                {
                    auto current_uri_pos = uri_info->next_uri_to_read.fetch_add(1);
                    if (current_uri_pos >= uri_info->uri_list_to_read.size())
                        return {};

                    auto current_uri = uri_info->uri_list_to_read[current_uri_pos];

                    std::lock_guard lock(reader_mutex);
                    initialize(current_uri);
                }

                Chunk chunk;
                if (reader->pull(chunk))
                    return chunk;

                {
                    std::lock_guard lock(reader_mutex);
                    pipeline->reset();
                    reader.reset();
                }
            }
        }

        static std::unique_ptr<ReadBuffer> getFirstAvailableURLReadBuffer(
            std::vector<String>::const_iterator & option,
            const std::vector<String>::const_iterator & end,
            ContextPtr context,
            const URIParams & params,
            const String & http_method,
            std::function<void(std::ostream &)> callback,
            const ConnectionTimeouts & timeouts,
            const String & compression_method,
            Poco::Net::HTTPBasicCredentials & credentials,
            const ReadWriteBufferFromHTTP::HTTPHeaderEntries & headers,
            bool glob_url,
            bool delay_initialization)
        {
            String first_exception_message;
            ReadSettings read_settings = context->getReadSettings();

            size_t options = std::distance(option, end);
            for (; option != end; ++option)
            {
                bool skip_url_not_found_error = glob_url && read_settings.http_skip_not_found_url_for_globs && option == std::prev(end);
                auto request_uri = Poco::URI(*option);

                for (const auto & [param, value] : params)
                    request_uri.addQueryParameter(param, value);

                setCredentials(credentials, request_uri);

                try
                {
                    return wrapReadBufferWithCompressionMethod(
                        std::make_unique<ReadWriteBufferFromHTTP>(
                            request_uri,
                            http_method,
                            callback,
                            timeouts,
                            credentials,
                            context->getSettingsRef().max_http_get_redirects,
                            DBMS_DEFAULT_BUFFER_SIZE,
                            read_settings,
                            headers,
                            ReadWriteBufferFromHTTP::Range{},
                            &context->getRemoteHostFilter(),
                            delay_initialization,
                            /* use_external_buffer */false,
                            /* skip_url_not_found_error */skip_url_not_found_error),
                        chooseCompressionMethod(request_uri.getPath(), compression_method));
                }
                catch (...)
                {
                    if (first_exception_message.empty())
                        first_exception_message = getCurrentExceptionMessage(false);

                    if (options == 1)
                        throw;

                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }

            throw Exception(ErrorCodes::NETWORK_ERROR, "All uri ({}) options are unreachable: {}", options, first_exception_message);
        }

    private:
        using InitializeFunc = std::function<void(const URIInfo::FailoverOptions &)>;
        InitializeFunc initialize;

        String name;
        URIInfoPtr uri_info;

        std::unique_ptr<ReadBuffer> read_buf;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
        /// onCancell and generate can be called concurrently and both of them
        /// have R/W access to reader pointer.
        std::mutex reader_mutex;

        Poco::Net::HTTPBasicCredentials credentials;
    };
}

StorageURLSink::StorageURLSink(
    const String & uri,
    const String & format,
    const std::optional<FormatSettings> & format_settings,
    const Block & sample_block,
    ContextPtr context,
    const ConnectionTimeouts & timeouts,
    const CompressionMethod compression_method,
    const String & http_method)
    : SinkToStorage(sample_block)
{
    std::string content_type = FormatFactory::instance().getContentType(format, context, format_settings);
    std::string content_encoding = toContentEncodingName(compression_method);

    write_buf = wrapWriteBufferWithCompressionMethod(
            std::make_unique<WriteBufferFromHTTP>(Poco::URI(uri), http_method, content_type, content_encoding, timeouts),
            compression_method, 3);
    writer = FormatFactory::instance().getOutputFormat(format, *write_buf, sample_block,
        context, {} /* write callback */, format_settings);
}


void StorageURLSink::consume(Chunk chunk)
{
    writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}

void StorageURLSink::onFinish()
{
    writer->finalize();
    writer->flush();
    write_buf->finalize();
}

class PartitionedStorageURLSink : public PartitionedSink
{
public:
    PartitionedStorageURLSink(
        const ASTPtr & partition_by,
        const String & uri_,
        const String & format_,
        const std::optional<FormatSettings> & format_settings_,
        const Block & sample_block_,
        ContextPtr context_,
        const ConnectionTimeouts & timeouts_,
        const CompressionMethod compression_method_,
        const String & http_method_)
            : PartitionedSink(partition_by, context_, sample_block_)
            , uri(uri_)
            , format(format_)
            , format_settings(format_settings_)
            , sample_block(sample_block_)
            , context(context_)
            , timeouts(timeouts_)
            , compression_method(compression_method_)
            , http_method(http_method_)
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto partition_path = PartitionedSink::replaceWildcards(uri, partition_id);
        context->getRemoteHostFilter().checkURL(Poco::URI(partition_path));
        return std::make_shared<StorageURLSink>(partition_path, format,
            format_settings, sample_block, context, timeouts, compression_method, http_method);
    }

private:
    const String uri;
    const String format;
    const std::optional<FormatSettings> format_settings;
    const Block sample_block;
    ContextPtr context;
    const ConnectionTimeouts timeouts;

    const CompressionMethod compression_method;
    const String http_method;
};

std::string IStorageURLBase::getReadMethod() const
{
    return Poco::Net::HTTPRequest::HTTP_GET;
}

std::vector<std::pair<std::string, std::string>> IStorageURLBase::getReadURIParams(
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    const SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    return {};
}

std::function<void(std::ostream &)> IStorageURLBase::getReadPOSTDataCallback(
    const Names & /*column_names*/,
    const ColumnsDescription & /* columns_description */,
    const SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    return nullptr;
}


ColumnsDescription IStorageURLBase::getTableStructureFromData(
    const String & format,
    const String & uri,
    const String & compression_method,
    const ReadWriteBufferFromHTTP::HTTPHeaderEntries & headers,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context)
{
    Poco::Net::HTTPBasicCredentials credentials;

    std::vector<String> urls_to_check;
    if (urlWithGlobs(uri))
    {
        size_t max_addresses = context->getSettingsRef().glob_expansion_max_elements;
        auto uri_descriptions = parseRemoteDescription(uri, 0, uri.size(), ',', max_addresses);
        for (const auto & description : uri_descriptions)
        {
            auto options = parseRemoteDescription(description, 0, description.size(), '|', max_addresses);
            urls_to_check.insert(urls_to_check.end(), options.begin(), options.end());
        }
    }
    else
    {
        urls_to_check = {uri};
    }

    String exception_messages;
    bool read_buffer_creator_was_used = false;

    std::vector<String>::const_iterator option = urls_to_check.begin();
    do
    {
        auto read_buffer_creator = [&]()
        {
            read_buffer_creator_was_used = true;
            return StorageURLSource::getFirstAvailableURLReadBuffer(
                option,
                urls_to_check.end(),
                context,
                {},
                Poco::Net::HTTPRequest::HTTP_GET,
                {},
                ConnectionTimeouts::getHTTPTimeouts(context),
                compression_method,
                credentials,
                headers,
                false,
                false);
        };

        try
        {
            return readSchemaFromFormat(format, format_settings, read_buffer_creator, context);
        }
        catch (...)
        {
            if (urls_to_check.size() == 1 || !read_buffer_creator_was_used)
                throw;

            exception_messages += getCurrentExceptionMessage(false) + "\n";
        }

    } while (++option < urls_to_check.end());

    throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "All attempts to extract table structure from urls failed. Errors:\n{}", exception_messages);
}

bool IStorageURLBase::isColumnOriented() const
{
    return FormatFactory::instance().checkIfFormatIsColumnOriented(format_name);
}

Pipe IStorageURLBase::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    auto params = getReadURIParams(column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size);

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

    if (urlWithGlobs(uri))
    {
        size_t max_addresses = local_context->getSettingsRef().glob_expansion_max_elements;
        auto uri_descriptions = parseRemoteDescription(uri, 0, uri.size(), ',', max_addresses);

        if (num_streams > uri_descriptions.size())
            num_streams = uri_descriptions.size();

        /// For each uri (which acts like shard) check if it has failover options
        auto uri_info = std::make_shared<StorageURLSource::URIInfo>();
        for (const auto & description : uri_descriptions)
            uri_info->uri_list_to_read.emplace_back(parseRemoteDescription(description, 0, description.size(), '|', max_addresses));

        Pipes pipes;
        pipes.reserve(num_streams);

        for (size_t i = 0; i < num_streams; ++i)
        {
            pipes.emplace_back(std::make_shared<StorageURLSource>(
                uri_info,
                getReadMethod(),
                getReadPOSTDataCallback(
                    column_names, columns_description, query_info,
                    local_context, processed_stage, max_block_size),
                format_name,
                format_settings,
                getName(),
                block_for_format,
                local_context,
                columns_description,
                max_block_size,
                ConnectionTimeouts::getHTTPTimeouts(local_context),
                compression_method, headers, params, /* glob_url */true));
        }
        return Pipe::unitePipes(std::move(pipes));
    }
    else
    {
        auto uri_info = std::make_shared<StorageURLSource::URIInfo>();
        uri_info->uri_list_to_read.emplace_back(std::vector<String>{uri});
        return Pipe(std::make_shared<StorageURLSource>(
            uri_info,
            getReadMethod(),
            getReadPOSTDataCallback(
                column_names, columns_description, query_info,
                local_context, processed_stage, max_block_size),
            format_name,
            format_settings,
            getName(),
            block_for_format,
            local_context,
            columns_description,
            max_block_size,
            ConnectionTimeouts::getHTTPTimeouts(local_context),
            compression_method, headers, params));
    }
}


Pipe StorageURLWithFailover::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
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

    auto params = getReadURIParams(column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size);

    auto uri_info = std::make_shared<StorageURLSource::URIInfo>();
    uri_info->uri_list_to_read.emplace_back(uri_options);
    auto pipe =  Pipe(std::make_shared<StorageURLSource>(
        uri_info,
        getReadMethod(),
        getReadPOSTDataCallback(
            column_names, columns_description, query_info,
            local_context, processed_stage, max_block_size),
        format_name,
        format_settings,
        getName(),
        block_for_format,
        local_context,
        columns_description,
        max_block_size,
        ConnectionTimeouts::getHTTPTimeouts(local_context),
        compression_method, headers, params));
    std::shuffle(uri_options.begin(), uri_options.end(), thread_local_rng);
    return pipe;
}


SinkToStoragePtr IStorageURLBase::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    if (http_method.empty())
        http_method = Poco::Net::HTTPRequest::HTTP_POST;

    bool has_wildcards = uri.find(PartitionedSink::PARTITION_ID_WILDCARD) != String::npos;
    const auto * insert_query = dynamic_cast<const ASTInsertQuery *>(query.get());
    auto partition_by_ast = insert_query ? (insert_query->partition_by ? insert_query->partition_by : partition_by) : nullptr;
    bool is_partitioned_implementation = partition_by_ast && has_wildcards;

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedStorageURLSink>(
            partition_by_ast,
            uri, format_name,
            format_settings, metadata_snapshot->getSampleBlock(), context,
            ConnectionTimeouts::getHTTPTimeouts(context),
            chooseCompressionMethod(uri, compression_method), http_method);
    }
    else
    {
        return std::make_shared<StorageURLSink>(uri, format_name,
            format_settings, metadata_snapshot->getSampleBlock(), context,
            ConnectionTimeouts::getHTTPTimeouts(context),
            chooseCompressionMethod(uri, compression_method), http_method);
    }
}

StorageURL::StorageURL(
    const String & uri_,
    const StorageID & table_id_,
    const String & format_name_,
    const std::optional<FormatSettings> & format_settings_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    const String & compression_method_,
    const ReadWriteBufferFromHTTP::HTTPHeaderEntries & headers_,
    const String & http_method_,
    ASTPtr partition_by_)
    : IStorageURLBase(uri_, context_, table_id_, format_name_, format_settings_,
        columns_, constraints_, comment, compression_method_, headers_, http_method_, partition_by_)
{
    context_->getRemoteHostFilter().checkURL(Poco::URI(uri));
}


StorageURLWithFailover::StorageURLWithFailover(
    const std::vector<String> & uri_options_,
    const StorageID & table_id_,
    const String & format_name_,
    const std::optional<FormatSettings> & format_settings_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_,
    const String & compression_method_)
    : StorageURL("", table_id_, format_name_, format_settings_, columns_, constraints_, String{}, context_, compression_method_)
{
    for (const auto & uri_option : uri_options_)
    {
        Poco::URI poco_uri(uri_option);
        context_->getRemoteHostFilter().checkURL(poco_uri);
        LOG_DEBUG(&Poco::Logger::get("StorageURLDistributed"), "Adding URL option: {}", uri_option);
        uri_options.emplace_back(uri_option);
    }
}


FormatSettings StorageURL::getFormatSettingsFromArgs(const StorageFactory::Arguments & args)
{
    // Use format settings from global server context + settings from
    // the SETTINGS clause of the create query. Settings from current
    // session and user are ignored.
    FormatSettings format_settings;
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

        format_settings = getFormatSettings(args.getContext(),
            user_format_settings);
    }
    else
    {
        format_settings = getFormatSettings(args.getContext());
    }

    return format_settings;
}

URLBasedDataSourceConfiguration StorageURL::getConfiguration(ASTs & args, ContextPtr local_context)
{
    URLBasedDataSourceConfiguration configuration;

    if (auto named_collection = getURLBasedDataSourceConfiguration(args, local_context))
    {
        auto [common_configuration, storage_specific_args] = named_collection.value();
        configuration.set(common_configuration);

        if (!configuration.http_method.empty()
            && configuration.http_method != Poco::Net::HTTPRequest::HTTP_POST
            && configuration.http_method != Poco::Net::HTTPRequest::HTTP_PUT)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Http method can be POST or PUT (current: {}). For insert default is POST, for select GET",
                            configuration.http_method);

        if (!storage_specific_args.empty())
        {
            String illegal_args;
            for (const auto & arg : storage_specific_args)
            {
                if (!illegal_args.empty())
                    illegal_args += ", ";
                illegal_args += arg.first;
            }
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown argument `{}` for storage URL", illegal_args);
        }
    }
    else
    {
        if (args.empty() || args.size() > 3)
            throw Exception(
                "Storage URL requires 1, 2 or 3 arguments: url, name of used format (taken from file extension by default) and optional compression method.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, local_context);

        configuration.url = args[0]->as<ASTLiteral &>().value.safeGet<String>();
        if (args.size() > 1)
            configuration.format = args[1]->as<ASTLiteral &>().value.safeGet<String>();
        if (args.size() == 3)
            configuration.compression_method = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    }

    if (configuration.format == "auto")
        configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.url, true);

    return configuration;
}


void registerStorageURL(StorageFactory & factory)
{
    factory.registerStorage("URL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        auto configuration = StorageURL::getConfiguration(engine_args, args.getLocalContext());
        auto format_settings = StorageURL::getFormatSettingsFromArgs(args);

        ReadWriteBufferFromHTTP::HTTPHeaderEntries headers;
        for (const auto & [header, value] : configuration.headers)
        {
            auto value_literal = value.safeGet<String>();
            if (header == "Range")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Range headers are not allowed");
            headers.emplace_back(std::make_pair(header, value_literal));
        }

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            partition_by = args.storage_def->partition_by->clone();

        return StorageURL::create(
            configuration.url,
            args.table_id,
            configuration.format,
            format_settings,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
            configuration.compression_method,
            headers,
            configuration.http_method,
            partition_by);
    },
    {
        .supports_settings = true,
        .supports_schema_inference = true,
        .source_access_type = AccessType::URL,
    });
}
}
