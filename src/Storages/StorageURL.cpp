#include <Storages/StorageURL.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Storages/PartitionedSink.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ReadFromStorageProgress.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/WriteBufferFromHTTP.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>

#include <Common/ThreadStatus.h>
#include <Common/parseRemoteDescription.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>

#include <algorithm>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/logger_useful.h>
#include <Poco/Net/HTTPRequest.h>
#include <regex>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NETWORK_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

static constexpr auto bad_arguments_error_message = "Storage URL requires 1-4 arguments: "
                                                    "url, name of used format (taken from file extension by default), "
                                                    "optional compression method, optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

static const std::unordered_set<std::string_view> required_configuration_keys = {
    "url",
};

static const std::unordered_set<std::string_view> optional_configuration_keys = {
    "format",
    "compression",
    "compression_method",
    "structure",
    "filename",
    "method",
    "http_method",
    "description",
    "headers.header.name",
    "headers.header.value",
};

/// Headers in config file will have structure "headers.header.name" and "headers.header.value".
/// But Poco::AbstractConfiguration converts them into "header", "header[1]", "header[2]".
static const std::vector<std::regex> optional_regex_keys = {
    std::regex(R"(headers.header\[[\d]*\].name)"),
    std::regex(R"(headers.header\[[\d]*\].value)"),
};

static bool urlWithGlobs(const String & uri)
{
    return (uri.find('{') != std::string::npos && uri.find('}') != std::string::npos) || uri.find('|') != std::string::npos;
}

static ConnectionTimeouts getHTTPTimeouts(ContextPtr context)
{
    return ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), {context->getConfigRef().getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT), 0});
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
    const HTTPHeaderEntries & headers_,
    const String & http_method_,
    ASTPtr partition_by_,
    bool distributed_processing_)
    : IStorage(table_id_)
    , uri(uri_)
    , compression_method(chooseCompressionMethod(Poco::URI(uri_).getPath(), compression_method_))
    , format_name(format_name_)
    , format_settings(format_settings_)
    , headers(headers_)
    , http_method(http_method_)
    , partition_by(partition_by_)
    , distributed_processing(distributed_processing_)
{
    FormatFactory::instance().checkFormatName(format_name);
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
    HTTPHeaderEntries getHeaders(const HTTPHeaderEntries & headers_)
    {
        HTTPHeaderEntries headers(headers_.begin(), headers_.end());

        // Propagate OpenTelemetry trace context, if any, downstream.
        const auto & current_trace_context = OpenTelemetry::CurrentContext();
        if (current_trace_context.isTraceEnabled())
        {
            headers.emplace_back("traceparent", current_trace_context.composeTraceparentHeader());

            if (!current_trace_context.tracestate.empty())
            {
                headers.emplace_back("tracestate", current_trace_context.tracestate);
            }
        }

        return headers;
    }

    StorageURLSource::FailoverOptions getFailoverOptions(const String & uri, size_t max_addresses)
    {
        return parseRemoteDescription(uri, 0, uri.size(), '|', max_addresses);
    }
}

class StorageURLSource::DisclosedGlobIterator::Impl
{
public:
    Impl(const String & uri, size_t max_addresses)
    {
        uris = parseRemoteDescription(uri, 0, uri.size(), ',', max_addresses);
    }

    String next()
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= uris.size())
            return {};

        return uris[current_index];
    }

    size_t size()
    {
        return uris.size();
    }

private:
    Strings uris;
    std::atomic_size_t index = 0;
};

StorageURLSource::DisclosedGlobIterator::DisclosedGlobIterator(const String & uri, size_t max_addresses)
    : pimpl(std::make_shared<StorageURLSource::DisclosedGlobIterator::Impl>(uri, max_addresses)) {}

String StorageURLSource::DisclosedGlobIterator::next()
{
    return pimpl->next();
}

size_t StorageURLSource::DisclosedGlobIterator::size()
{
    return pimpl->size();
}

void StorageURLSource::setCredentials(Poco::Net::HTTPBasicCredentials & credentials, const Poco::URI & request_uri)
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

Block StorageURLSource::getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns)
{
    for (const auto & virtual_column : requested_virtual_columns)
        sample_block.insert({virtual_column.type->createColumn(), virtual_column.type, virtual_column.name});

    return sample_block;
}

StorageURLSource::StorageURLSource(
    const std::vector<NameAndTypePair> & requested_virtual_columns_,
    std::shared_ptr<IteratorWrapper> uri_iterator_,
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
    CompressionMethod compression_method,
    size_t download_threads,
    const HTTPHeaderEntries & headers_,
    const URIParams & params,
    bool glob_url)
    : ISource(getHeader(sample_block, requested_virtual_columns_)), name(std::move(name_)), requested_virtual_columns(requested_virtual_columns_), uri_iterator(uri_iterator_)
{
    auto headers = getHeaders(headers_);

    /// Lazy initialization. We should not perform requests in constructor, because we need to do it in query pipeline.
    initialize = [=, this]()
    {
        std::vector<String> current_uri_options;
        std::pair<Poco::URI, std::unique_ptr<ReadWriteBufferFromHTTP>> uri_and_buf;
        do
        {
            current_uri_options = (*uri_iterator)();
            if (current_uri_options.empty())
                return false;

            auto first_option = current_uri_options.cbegin();
            uri_and_buf = getFirstAvailableURIAndReadBuffer(
                first_option,
                current_uri_options.end(),
                context,
                params,
                http_method,
                callback,
                timeouts,
                credentials,
                headers,
                glob_url,
                current_uri_options.size() == 1);

            /// If file is empty and engine_url_skip_empty_files=1, skip it and go to the next file.
        }
        while (context->getSettingsRef().engine_url_skip_empty_files && uri_and_buf.second->eof());

        curr_uri = uri_and_buf.first;
        read_buf = std::move(uri_and_buf.second);

        size_t file_size = 0;
        try
        {
            file_size = getFileSizeFromReadBuffer(*read_buf);
        }
        catch (...)
        {
            // we simply continue without updating total_size
        }

        if (file_size)
        {
            /// Adjust total_rows_approx_accumulated with new total size.
            if (total_size)
                total_rows_approx_accumulated = static_cast<size_t>(std::ceil(static_cast<double>(total_size + file_size) / total_size * total_rows_approx_accumulated));
            total_size += file_size;
        }

        // TODO: Pass max_parsing_threads and max_download_threads adjusted for num_streams.
        input_format = FormatFactory::instance().getInput(
            format,
            *read_buf,
            sample_block,
            context,
            max_block_size,
            format_settings,
            download_threads,
            /*max_download_threads*/ std::nullopt,
            /* is_remote_fs */ true,
            compression_method);

        QueryPipelineBuilder builder;
        builder.init(Pipe(input_format));

        builder.addSimpleTransform([&](const Block & cur_header)
                                   { return std::make_shared<AddingDefaultsTransform>(cur_header, columns, *input_format, context); });

        pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
        reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
        return true;
    };
}

Chunk StorageURLSource::generate()
{
    while (true)
    {
        if (isCancelled())
        {
            if (reader)
                reader->cancel();
            break;
        }

        if (!reader && !initialize())
            return {};

        Chunk chunk;
        if (reader->pull(chunk))
        {
            UInt64 num_rows = chunk.getNumRows();
            if (num_rows && total_size)
            {
                size_t chunk_size = input_format->getApproxBytesReadForChunk();
                if (!chunk_size)
                    chunk_size = chunk.bytes();
                updateRowsProgressApprox(
                    *this, num_rows, chunk_size, total_size, total_rows_approx_accumulated, total_rows_count_times, total_rows_approx_max);
            }

            const String & path{curr_uri.getPath()};

            for (const auto & virtual_column : requested_virtual_columns)
            {
                if (virtual_column.name == "_path")
                {
                    chunk.addColumn(virtual_column.type->createColumnConst(num_rows, path)->convertToFullColumnIfConst());
                }
                else if (virtual_column.name == "_file")
                {
                    size_t last_slash_pos = path.find_last_of('/');
                    auto column = virtual_column.type->createColumnConst(num_rows, path.substr(last_slash_pos + 1));
                    chunk.addColumn(column->convertToFullColumnIfConst());
                }
            }

            return chunk;
        }

        pipeline->reset();
        reader.reset();
        input_format.reset();
        read_buf.reset();
    }
    return {};
}

std::pair<Poco::URI, std::unique_ptr<ReadWriteBufferFromHTTP>> StorageURLSource::getFirstAvailableURIAndReadBuffer(
    std::vector<String>::const_iterator & option,
    const std::vector<String>::const_iterator & end,
    ContextPtr context,
    const URIParams & params,
    const String & http_method,
    std::function<void(std::ostream &)> callback,
    const ConnectionTimeouts & timeouts,
    Poco::Net::HTTPBasicCredentials & credentials,
    const HTTPHeaderEntries & headers,
    bool glob_url,
    bool delay_initialization)
{
    String first_exception_message;
    ReadSettings read_settings = context->getReadSettings();

    size_t options = std::distance(option, end);
    std::pair<Poco::URI, std::unique_ptr<ReadWriteBufferFromHTTP>> last_skipped_empty_res;
    for (; option != end; ++option)
    {
        bool skip_url_not_found_error = glob_url && read_settings.http_skip_not_found_url_for_globs && option == std::prev(end);
        auto request_uri = Poco::URI(*option);

        for (const auto & [param, value] : params)
            request_uri.addQueryParameter(param, value);

        setCredentials(credentials, request_uri);

        const auto settings = context->getSettings();

        try
        {
            auto res = std::make_unique<ReadWriteBufferFromHTTP>(
                request_uri,
                http_method,
                callback,
                timeouts,
                credentials,
                settings.max_http_get_redirects,
                settings.max_read_buffer_size,
                read_settings,
                headers,
                &context->getRemoteHostFilter(),
                delay_initialization,
                /* use_external_buffer */ false,
                /* skip_url_not_found_error */ skip_url_not_found_error);

            if (context->getSettingsRef().engine_url_skip_empty_files && res->eof() && option != std::prev(end))
            {
                last_skipped_empty_res = {request_uri, std::move(res)};
                continue;
            }

            return std::make_tuple(request_uri, std::move(res));
        }
        catch (...)
        {
            if (options == 1)
                throw;

            if (first_exception_message.empty())
                first_exception_message = getCurrentExceptionMessage(false);

            tryLogCurrentException(__PRETTY_FUNCTION__);

            continue;
        }
    }

    /// If all options are unreachable except empty ones that we skipped,
    /// return last empty result. It will be skipped later.
    if (last_skipped_empty_res.second)
        return last_skipped_empty_res;

    throw Exception(ErrorCodes::NETWORK_ERROR, "All uri ({}) options are unreachable: {}", options, first_exception_message);
}

StorageURLSink::StorageURLSink(
    const String & uri,
    const String & format,
    const std::optional<FormatSettings> & format_settings,
    const Block & sample_block,
    ContextPtr context,
    const ConnectionTimeouts & timeouts,
    const CompressionMethod compression_method,
    const HTTPHeaderEntries & headers,
    const String & http_method)
    : SinkToStorage(sample_block)
{
    std::string content_type = FormatFactory::instance().getContentType(format, context, format_settings);
    std::string content_encoding = toContentEncodingName(compression_method);

    write_buf = wrapWriteBufferWithCompressionMethod(
        std::make_unique<WriteBufferFromHTTP>(Poco::URI(uri), http_method, content_type, content_encoding, headers, timeouts),
        compression_method,
        3);
    writer = FormatFactory::instance().getOutputFormat(format, *write_buf, sample_block, context, format_settings);
}


void StorageURLSink::consume(Chunk chunk)
{
    std::lock_guard lock(cancel_mutex);
    if (cancelled)
        return;
    writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}

void StorageURLSink::onCancel()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
    cancelled = true;
}

void StorageURLSink::onException(std::exception_ptr exception)
{
    std::lock_guard lock(cancel_mutex);
    try
    {
        std::rethrow_exception(exception);
    }
    catch (...)
    {
        /// An exception context is needed to proper delete write buffers without finalization
        release();
    }
}

void StorageURLSink::onFinish()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
}

void StorageURLSink::finalize()
{
    if (!writer)
        return;

    try
    {
        writer->finalize();
        writer->flush();
        write_buf->finalize();
    }
    catch (...)
    {
        /// Stop ParallelFormattingOutputFormat correctly.
        release();
        throw;
    }
}

void StorageURLSink::release()
{
    writer.reset();
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
        const HTTPHeaderEntries & headers_,
        const String & http_method_)
        : PartitionedSink(partition_by, context_, sample_block_)
        , uri(uri_)
        , format(format_)
        , format_settings(format_settings_)
        , sample_block(sample_block_)
        , context(context_)
        , timeouts(timeouts_)
        , compression_method(compression_method_)
        , headers(headers_)
        , http_method(http_method_)
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto partition_path = PartitionedSink::replaceWildcards(uri, partition_id);
        context->getRemoteHostFilter().checkURL(Poco::URI(partition_path));
        return std::make_shared<StorageURLSink>(
            partition_path, format, format_settings, sample_block, context, timeouts, compression_method, headers, http_method);
    }

private:
    const String uri;
    const String format;
    const std::optional<FormatSettings> format_settings;
    const Block sample_block;
    ContextPtr context;
    const ConnectionTimeouts timeouts;

    const CompressionMethod compression_method;
    const HTTPHeaderEntries headers;
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
    CompressionMethod compression_method,
    const HTTPHeaderEntries & headers,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context)
{
    context->getRemoteHostFilter().checkURL(Poco::URI(uri));

    Poco::Net::HTTPBasicCredentials credentials;

    std::vector<String> urls_to_check;
    if (urlWithGlobs(uri))
    {
        size_t max_addresses = context->getSettingsRef().glob_expansion_max_elements;
        auto uri_descriptions = parseRemoteDescription(uri, 0, uri.size(), ',', max_addresses, "url");
        for (const auto & description : uri_descriptions)
        {
            auto options = parseRemoteDescription(description, 0, description.size(), '|', max_addresses, "url");
            urls_to_check.insert(urls_to_check.end(), options.begin(), options.end());
        }
    }
    else
    {
        urls_to_check = {uri};
    }

    std::optional<ColumnsDescription> columns_from_cache;
    if (context->getSettingsRef().schema_inference_use_cache_for_url)
        columns_from_cache = tryGetColumnsFromCache(urls_to_check, headers, credentials, format, format_settings, context);

    ReadBufferIterator read_buffer_iterator = [&, it = urls_to_check.cbegin(), first = true](ColumnsDescription &) mutable -> std::unique_ptr<ReadBuffer>
    {
        std::pair<Poco::URI, std::unique_ptr<ReadWriteBufferFromHTTP>> uri_and_buf;
        do
        {
            if (it == urls_to_check.cend())
            {
                if (first)
                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                        "Cannot extract table structure from {} format file, because all files are empty. "
                        "You must specify table structure manually",
                        format);
                return nullptr;
            }

            uri_and_buf = StorageURLSource::getFirstAvailableURIAndReadBuffer(
                it,
                urls_to_check.cend(),
                context,
                {},
                Poco::Net::HTTPRequest::HTTP_GET,
                {},
                getHTTPTimeouts(context),
                credentials,
                headers,
                false,
                false);

            ++it;
        } while (context->getSettingsRef().engine_url_skip_empty_files && uri_and_buf.second->eof());

        first = false;
        return wrapReadBufferWithCompressionMethod(
            std::move(uri_and_buf.second),
            compression_method,
            static_cast<int>(context->getSettingsRef().zstd_window_log_max));
    };

    ColumnsDescription columns;
    if (columns_from_cache)
        columns = *columns_from_cache;
    else
        columns = readSchemaFromFormat(format, format_settings, read_buffer_iterator, urls_to_check.size() > 1, context);

    if (context->getSettingsRef().schema_inference_use_cache_for_url)
        addColumnsToCache(urls_to_check, columns, format, format_settings, context);

    return columns;
}

bool IStorageURLBase::supportsSubsetOfColumns() const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name);
}

bool IStorageURLBase::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(format_name);
}

bool IStorageURLBase::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(format_name, context);
}

Pipe IStorageURLBase::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto params = getReadURIParams(column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size);

    ColumnsDescription columns_description;
    Block block_for_format;
    if (supportsSubsetOfColumns())
    {
        columns_description = storage_snapshot->getDescriptionForColumns(column_names);
        block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    }
    else
    {
        columns_description = storage_snapshot->metadata->getColumns();
        block_for_format = storage_snapshot->metadata->getSampleBlock();
    }

    std::unordered_set<String> column_names_set(column_names.begin(), column_names.end());
    std::vector<NameAndTypePair> requested_virtual_columns;
    for (const auto & virtual_column : getVirtuals())
    {
        if (column_names_set.contains(virtual_column.name))
            requested_virtual_columns.push_back(virtual_column);
    }

    size_t max_download_threads = local_context->getSettingsRef().max_download_threads;

    std::shared_ptr<StorageURLSource::IteratorWrapper> iterator_wrapper{nullptr};
    bool is_url_with_globs = urlWithGlobs(uri);
    size_t max_addresses = local_context->getSettingsRef().glob_expansion_max_elements;
    if (distributed_processing)
    {
        iterator_wrapper = std::make_shared<StorageURLSource::IteratorWrapper>(
            [callback = local_context->getReadTaskCallback(), max_addresses]()
            {
                String next_uri = callback();
                if (next_uri.empty())
                    return StorageURLSource::FailoverOptions{};
                return getFailoverOptions(next_uri, max_addresses);
            });
    }
    else if (is_url_with_globs)
    {
        /// Iterate through disclosed globs and make a source for each file
        auto glob_iterator = std::make_shared<StorageURLSource::DisclosedGlobIterator>(uri, max_addresses);
        iterator_wrapper = std::make_shared<StorageURLSource::IteratorWrapper>([glob_iterator, max_addresses]()
        {
            String next_uri = glob_iterator->next();
            if (next_uri.empty())
                return StorageURLSource::FailoverOptions{};
            return getFailoverOptions(next_uri, max_addresses);
        });

        if (num_streams > glob_iterator->size())
            num_streams = glob_iterator->size();
    }
    else
    {
        iterator_wrapper = std::make_shared<StorageURLSource::IteratorWrapper>([&, max_addresses, done = false]() mutable
        {
            if (done)
                return StorageURLSource::FailoverOptions{};
            done = true;
            return getFailoverOptions(uri, max_addresses);
        });
        num_streams = 1;
    }

    Pipes pipes;
    pipes.reserve(num_streams);

    size_t download_threads = num_streams >= max_download_threads ? 1 : (max_download_threads / num_streams);
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageURLSource>(
            requested_virtual_columns,
            iterator_wrapper,
            getReadMethod(),
            getReadPOSTDataCallback(column_names, columns_description, query_info, local_context, processed_stage, max_block_size),
            format_name,
            format_settings,
            getName(),
            block_for_format,
            local_context,
            columns_description,
            max_block_size,
            getHTTPTimeouts(local_context),
            compression_method,
            download_threads,
            headers,
            params,
            is_url_with_globs));
    }

    return Pipe::unitePipes(std::move(pipes));
}


Pipe StorageURLWithFailover::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    ColumnsDescription columns_description;
    Block block_for_format;
    if (supportsSubsetOfColumns())
    {
        columns_description = storage_snapshot->getDescriptionForColumns(column_names);
        block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    }
    else
    {
        columns_description = storage_snapshot->metadata->getColumns();
        block_for_format = storage_snapshot->metadata->getSampleBlock();
    }

    auto params = getReadURIParams(column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size);

    auto iterator_wrapper = std::make_shared<StorageURLSource::IteratorWrapper>([&, done = false]() mutable
    {
        if (done)
            return StorageURLSource::FailoverOptions{};
        done = true;
        return uri_options;
    });

    auto pipe = Pipe(std::make_shared<StorageURLSource>(
        std::vector<NameAndTypePair>{},
        iterator_wrapper,
        getReadMethod(),
        getReadPOSTDataCallback(column_names, columns_description, query_info, local_context, processed_stage, max_block_size),
        format_name,
        format_settings,
        getName(),
        block_for_format,
        local_context,
        columns_description,
        max_block_size,
        getHTTPTimeouts(local_context),
        compression_method,
        local_context->getSettingsRef().max_download_threads,
        headers,
        params));
    std::shuffle(uri_options.begin(), uri_options.end(), thread_local_rng);
    return pipe;
}


SinkToStoragePtr IStorageURLBase::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool /*async_insert*/)
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
            uri,
            format_name,
            format_settings,
            metadata_snapshot->getSampleBlock(),
            context,
            getHTTPTimeouts(context),
            compression_method,
            headers,
            http_method);
    }
    else
    {
        return std::make_shared<StorageURLSink>(
            uri,
            format_name,
            format_settings,
            metadata_snapshot->getSampleBlock(),
            context,
            getHTTPTimeouts(context),
            compression_method,
            headers,
            http_method);
    }
}

NamesAndTypesList IStorageURLBase::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};
}

SchemaCache & IStorageURLBase::getSchemaCache(const ContextPtr & context)
{
    static SchemaCache schema_cache(context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_url", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

std::optional<ColumnsDescription> IStorageURLBase::tryGetColumnsFromCache(
    const Strings & urls,
    const HTTPHeaderEntries & headers,
    const Poco::Net::HTTPBasicCredentials & credentials,
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context)
{
    auto & schema_cache = getSchemaCache(context);
    for (const auto & url : urls)
    {
        auto get_last_mod_time = [&]() -> std::optional<time_t>
        {
            auto last_mod_time = getLastModificationTime(url, headers, credentials, context);
            /// Some URLs could not have Last-Modified header, in this case we cannot be sure that
            /// data wasn't changed after adding it's schema to cache. Use schema from cache only if
            /// special setting for this case is enabled.
            if (!last_mod_time && !context->getSettingsRef().schema_inference_cache_require_modification_time_for_url)
                return 0;
            return last_mod_time;
        };

        auto cache_key = getKeyForSchemaCache(url, format_name, format_settings, context);
        auto columns = schema_cache.tryGet(cache_key, get_last_mod_time);
        if (columns)
            return columns;
    }

    return std::nullopt;
}

void IStorageURLBase::addColumnsToCache(
    const Strings & urls,
    const ColumnsDescription & columns,
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context)
{
    auto & schema_cache = getSchemaCache(context);
    auto cache_keys = getKeysForSchemaCache(urls, format_name, format_settings, context);
    schema_cache.addMany(cache_keys, columns);
}

std::optional<time_t> IStorageURLBase::getLastModificationTime(
    const String & url,
    const HTTPHeaderEntries & headers,
    const Poco::Net::HTTPBasicCredentials & credentials,
    const ContextPtr & context)
{
    auto settings = context->getSettingsRef();

    try
    {
        ReadWriteBufferFromHTTP buf(
            Poco::URI(url),
            Poco::Net::HTTPRequest::HTTP_GET,
            {},
            getHTTPTimeouts(context),
            credentials,
            settings.max_http_get_redirects,
            settings.max_read_buffer_size,
            context->getReadSettings(),
            headers,
            &context->getRemoteHostFilter(),
            true,
            false,
            false);

        return buf.getLastModificationTime();
    }
    catch (...)
    {
        return std::nullopt;
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
    const HTTPHeaderEntries & headers_,
    const String & http_method_,
    ASTPtr partition_by_,
    bool distributed_processing_)
    : IStorageURLBase(
        uri_,
        context_,
        table_id_,
        format_name_,
        format_settings_,
        columns_,
        constraints_,
        comment,
        compression_method_,
        headers_,
        http_method_,
        partition_by_,
        distributed_processing_)
{
    context_->getRemoteHostFilter().checkURL(Poco::URI(uri));
    context_->getHTTPHeaderFilter().checkHeaders(headers);
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

        format_settings = getFormatSettings(args.getContext(), user_format_settings);
    }
    else
    {
        format_settings = getFormatSettings(args.getContext());
    }

    return format_settings;
}

ASTs::iterator StorageURL::collectHeaders(
    ASTs & url_function_args, HTTPHeaderEntries & header_entries, ContextPtr context)
{
    ASTs::iterator headers_it = url_function_args.end();

    for (auto * arg_it = url_function_args.begin(); arg_it != url_function_args.end(); ++arg_it)
    {
        const auto * headers_ast_function = (*arg_it)->as<ASTFunction>();
        if (headers_ast_function && headers_ast_function->name == "headers")
        {
            if (headers_it != url_function_args.end())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "URL table function can have only one key-value argument: headers=(). {}",
                    bad_arguments_error_message);

            const auto * headers_function_args_expr = assert_cast<const ASTExpressionList *>(headers_ast_function->arguments.get());
            auto headers_function_args = headers_function_args_expr->children;

            for (auto & header_arg : headers_function_args)
            {
                const auto * header_ast = header_arg->as<ASTFunction>();
                if (!header_ast || header_ast->name != "equals")
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Headers argument is incorrect. {}", bad_arguments_error_message);

                const auto * header_args_expr = assert_cast<const ASTExpressionList *>(header_ast->arguments.get());
                auto header_args = header_args_expr->children;
                if (header_args.size() != 2)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Headers argument is incorrect: expected 2 arguments, got {}",
                        header_args.size());

                auto ast_literal = evaluateConstantExpressionOrIdentifierAsLiteral(header_args[0], context);
                auto arg_name_value = ast_literal->as<ASTLiteral>()->value;
                if (arg_name_value.getType() != Field::Types::Which::String)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as header name");
                auto arg_name = arg_name_value.safeGet<String>();

                ast_literal = evaluateConstantExpressionOrIdentifierAsLiteral(header_args[1], context);
                auto arg_value = ast_literal->as<ASTLiteral>()->value;
                if (arg_value.getType() != Field::Types::Which::String)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as header value");

                header_entries.emplace_back(arg_name, arg_value.safeGet<String>());
            }

            headers_it = arg_it;

            continue;
        }

        if (headers_ast_function && headers_ast_function->name == "equals")
            continue;

        (*arg_it) = evaluateConstantExpressionOrIdentifierAsLiteral((*arg_it), context);
    }

    return headers_it;
}

void StorageURL::processNamedCollectionResult(Configuration & configuration, const NamedCollection & collection)
{
    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys, optional_regex_keys);

    configuration.url = collection.get<String>("url");
    configuration.headers = getHeadersFromNamedCollection(collection);

    configuration.http_method = collection.getOrDefault<String>("http_method", collection.getOrDefault<String>("method", ""));
    if (!configuration.http_method.empty() && configuration.http_method != Poco::Net::HTTPRequest::HTTP_POST
        && configuration.http_method != Poco::Net::HTTPRequest::HTTP_PUT)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Http method can be POST or PUT (current: {}). For insert default is POST, for select GET",
            configuration.http_method);

    configuration.format = collection.getOrDefault<String>("format", "auto");
    configuration.compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    configuration.structure = collection.getOrDefault<String>("structure", "auto");
}

StorageURL::Configuration StorageURL::getConfiguration(ASTs & args, ContextPtr local_context)
{
    StorageURL::Configuration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, local_context))
    {
        StorageURL::processNamedCollectionResult(configuration, *named_collection);
        collectHeaders(args, configuration.headers, local_context);
    }
    else
    {
        if (args.empty() || args.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, bad_arguments_error_message);

        auto * header_it = collectHeaders(args, configuration.headers, local_context);
        if (header_it != args.end())
            args.erase(header_it);

        configuration.url = checkAndGetLiteralArgument<String>(args[0], "url");
        if (args.size() > 1)
            configuration.format = checkAndGetLiteralArgument<String>(args[1], "format");
        if (args.size() == 3)
            configuration.compression_method = checkAndGetLiteralArgument<String>(args[2], "compression_method");
    }

    if (configuration.format == "auto")
        configuration.format = FormatFactory::instance().getFormatFromFileName(Poco::URI(configuration.url).getPath(), true);

    for (const auto & [header, value] : configuration.headers)
    {
        if (header == "Range")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Range headers are not allowed");
    }

    return configuration;
}


void registerStorageURL(StorageFactory & factory)
{
    factory.registerStorage(
        "URL",
        [](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;
            auto configuration = StorageURL::getConfiguration(engine_args, args.getLocalContext());
            auto format_settings = StorageURL::getFormatSettingsFromArgs(args);

            ASTPtr partition_by;
            if (args.storage_def->partition_by)
                partition_by = args.storage_def->partition_by->clone();

            return std::make_shared<StorageURL>(
                configuration.url,
                args.table_id,
                configuration.format,
                format_settings,
                args.columns,
                args.constraints,
                args.comment,
                args.getContext(),
                configuration.compression_method,
                configuration.headers,
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
