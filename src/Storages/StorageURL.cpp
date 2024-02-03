#include <Storages/StorageURL.h>
#include <Storages/PartitionedSink.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/VirtualColumnUtils.h>

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
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

#include <Common/ThreadStatus.h>
#include <Common/parseRemoteDescription.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Common/ProfileEvents.h>
#include <Common/thread_local_rng.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>

#include <algorithm>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Poco/Net/HTTPRequest.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace ProfileEvents
{
    extern const Event EngineFileLikeReadFiles;
}

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
static const std::vector<std::shared_ptr<re2::RE2>> optional_regex_keys = {
    std::make_shared<re2::RE2>(R"(headers.header\[[0-9]*\].name)"),
    std::make_shared<re2::RE2>(R"(headers.header\[[0-9]*\].value)"),
};

static bool urlWithGlobs(const String & uri)
{
    return (uri.find('{') != std::string::npos && uri.find('}') != std::string::npos) || uri.find('|') != std::string::npos;
}

static ConnectionTimeouts getHTTPTimeouts(ContextPtr context)
{
    return ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings().keep_alive_timeout);
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
    {
        /// We don't allow special columns in URL storage.
        if (!columns_.hasOnlyOrdinary())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table engine URL doesn't support special columns like MATERIALIZED, ALIAS or EPHEMERAL");
        storage_metadata.setColumns(columns_);
    }

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    virtual_columns = VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage(storage_metadata.getSampleBlock().getNamesAndTypesList());
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

    auto getProxyConfiguration(const std::string & protocol_string)
    {
        auto protocol = protocol_string == "https" ? ProxyConfigurationResolver::Protocol::HTTPS
                                             : ProxyConfigurationResolver::Protocol::HTTP;
        return ProxyConfigurationResolverProvider::get(protocol, Context::getGlobalContextInstance()->getConfigRef())->resolve();
    }
}

class StorageURLSource::DisclosedGlobIterator::Impl
{
public:
    Impl(const String & uri_, size_t max_addresses, const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns, const ContextPtr & context)
    {
        uris = parseRemoteDescription(uri_, 0, uri_.size(), ',', max_addresses);

        ActionsDAGPtr filter_dag;
        if (!uris.empty())
            filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);

        if (filter_dag)
        {
            std::vector<String> paths;
            paths.reserve(uris.size());
            for (const auto & uri : uris)
                paths.push_back(Poco::URI(uri).getPath());

            VirtualColumnUtils::filterByPathOrFile(uris, paths, filter_dag, virtual_columns, context);
        }
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

StorageURLSource::DisclosedGlobIterator::DisclosedGlobIterator(const String & uri, size_t max_addresses, const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns, const ContextPtr & context)
    : pimpl(std::make_shared<StorageURLSource::DisclosedGlobIterator::Impl>(uri, max_addresses, predicate, virtual_columns, context)) {}

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

StorageURLSource::StorageURLSource(
    const ReadFromFormatInfo & info,
    std::shared_ptr<IteratorWrapper> uri_iterator_,
    const std::string & http_method,
    std::function<void(std::ostream &)> callback,
    const String & format_,
    const std::optional<FormatSettings> & format_settings_,
    String name_,
    ContextPtr context_,
    UInt64 max_block_size,
    const ConnectionTimeouts & timeouts,
    CompressionMethod compression_method,
    size_t max_parsing_threads,
    const HTTPHeaderEntries & headers_,
    const URIParams & params,
    bool glob_url,
    bool need_only_count_)
    : SourceWithKeyCondition(info.source_header, false), WithContext(context_)
    , name(std::move(name_))
    , columns_description(info.columns_description)
    , requested_columns(info.requested_columns)
    , requested_virtual_columns(info.requested_virtual_columns)
    , block_for_format(info.format_header)
    , uri_iterator(uri_iterator_)
    , format(format_)
    , format_settings(format_settings_)
    , headers(getHeaders(headers_))
    , need_only_count(need_only_count_)
{
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
                getContext(),
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
        while (getContext()->getSettingsRef().engine_url_skip_empty_files && uri_and_buf.second->eof());

        curr_uri = uri_and_buf.first;
        auto last_mod_time = uri_and_buf.second->tryGetLastModificationTime();
        read_buf = std::move(uri_and_buf.second);
        current_file_size = tryGetFileSizeFromReadBuffer(*read_buf);

        if (auto file_progress_callback = getContext()->getFileProgressCallback())
            file_progress_callback(FileProgress(0, current_file_size.value_or(0)));

        QueryPipelineBuilder builder;
        std::optional<size_t> num_rows_from_cache = std::nullopt;
        if (need_only_count && getContext()->getSettingsRef().use_cache_for_count_from_files)
            num_rows_from_cache = tryGetNumRowsFromCache(curr_uri.toString(), last_mod_time);

        if (num_rows_from_cache)
        {
            /// We should not return single chunk with all number of rows,
            /// because there is a chance that this chunk will be materialized later
            /// (it can cause memory problems even with default values in columns or when virtual columns are requested).
            /// Instead, we use special ConstChunkGenerator that will generate chunks
            /// with max_block_size rows until total number of rows is reached.
            auto source = std::make_shared<ConstChunkGenerator>(block_for_format, *num_rows_from_cache, max_block_size);
            builder.init(Pipe(source));
        }
        else
        {
            // TODO: Pass max_parsing_threads and max_download_threads adjusted for num_streams.
            input_format = FormatFactory::instance().getInput(
                format,
                *read_buf,
                block_for_format,
                getContext(),
                max_block_size,
                format_settings,
                max_parsing_threads,
                /*max_download_threads*/ std::nullopt,
                /* is_remote_ fs */ true,
                compression_method,
                need_only_count);

            if (key_condition)
                input_format->setKeyCondition(key_condition);

            if (need_only_count)
                input_format->needOnlyCount();

          builder.init(Pipe(input_format));

            if (columns_description.hasDefaults())
            {
                builder.addSimpleTransform([&](const Block & cur_header)
                {
                    return std::make_shared<AddingDefaultsTransform>(cur_header, columns_description, *input_format, getContext());
                });
            }
        }

        /// Add ExtractColumnsTransform to extract requested columns/subcolumns
        /// from chunk read by IInputFormat.
        builder.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExtractColumnsTransform>(header, requested_columns);
        });

        pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
        reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

        ProfileEvents::increment(ProfileEvents::EngineFileLikeReadFiles);
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
            total_rows_in_file += num_rows;
            size_t chunk_size = 0;
            if (input_format)
                chunk_size = input_format->getApproxBytesReadForChunk();
            progress(num_rows, chunk_size ? chunk_size : chunk.bytes());
            VirtualColumnUtils::addRequestedPathFileAndSizeVirtualsToChunk(chunk, requested_virtual_columns, curr_uri.getPath(), current_file_size);
            return chunk;
        }

        if (input_format && getContext()->getSettingsRef().use_cache_for_count_from_files)
            addNumRowsToCache(curr_uri.toString(), total_rows_in_file);

        pipeline->reset();
        reader.reset();
        input_format.reset();
        read_buf.reset();
        total_rows_in_file = 0;
    }
    return {};
}

std::pair<Poco::URI, std::unique_ptr<ReadWriteBufferFromHTTP>> StorageURLSource::getFirstAvailableURIAndReadBuffer(
    std::vector<String>::const_iterator & option,
    const std::vector<String>::const_iterator & end,
    ContextPtr context_,
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
    ReadSettings read_settings = context_->getReadSettings();

    size_t options = std::distance(option, end);
    std::pair<Poco::URI, std::unique_ptr<ReadWriteBufferFromHTTP>> last_skipped_empty_res;
    for (; option != end; ++option)
    {
        bool skip_url_not_found_error = glob_url && read_settings.http_skip_not_found_url_for_globs && option == std::prev(end);
        auto request_uri = Poco::URI(*option, context_->getSettingsRef().enable_url_encoding);

        for (const auto & [param, value] : params)
            request_uri.addQueryParameter(param, value);

        setCredentials(credentials, request_uri);

        const auto settings = context_->getSettings();

        auto proxy_config = getProxyConfiguration(http_method);

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
                &context_->getRemoteHostFilter(),
                delay_initialization,
                /* use_external_buffer */ false,
                /* skip_url_not_found_error */ skip_url_not_found_error,
                /* file_info */ std::nullopt,
                proxy_config);

            if (context_->getSettingsRef().engine_url_skip_empty_files && res->eof() && option != std::prev(end))
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

void StorageURLSource::addNumRowsToCache(const String & uri, size_t num_rows)
{
    auto cache_key = getKeyForSchemaCache(uri, format, format_settings, getContext());
    StorageURL::getSchemaCache(getContext()).addNumRows(cache_key, num_rows);
}

std::optional<size_t> StorageURLSource::tryGetNumRowsFromCache(const String & uri, std::optional<time_t> last_mod_time)
{
    auto cache_key = getKeyForSchemaCache(uri, format, format_settings, getContext());
    auto get_last_mod_time = [&]() -> std::optional<time_t>
    {
        /// Some URLs could not have Last-Modified header, in this case we cannot be sure that
        /// data wasn't changed after adding it's schema to cache. Use schema from cache only if
        /// special setting for this case is enabled.
        if (!last_mod_time && !getContext()->getSettingsRef().schema_inference_cache_require_modification_time_for_url)
            return 0;
        return last_mod_time;
    };

    return StorageURL::getSchemaCache(getContext()).tryGetNumRows(cache_key, get_last_mod_time);
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

    auto proxy_config = getProxyConfiguration(http_method);

    auto write_buffer = std::make_unique<WriteBufferFromHTTP>(
        Poco::URI(uri), http_method, content_type, content_encoding, headers, timeouts, DBMS_DEFAULT_BUFFER_SIZE, proxy_config
    );

    const auto & settings = context->getSettingsRef();
    write_buf = wrapWriteBufferWithCompressionMethod(
        std::move(write_buffer),
        compression_method,
        static_cast<int>(settings.output_format_compression_level),
        static_cast<int>(settings.output_format_compression_zstd_window_log));
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

namespace
{
    class ReadBufferIterator : public IReadBufferIterator, WithContext
    {
    public:
        ReadBufferIterator(
            const std::vector<String> & urls_to_check_,
            const String & format_,
            const CompressionMethod & compression_method_,
            const HTTPHeaderEntries & headers_,
            const std::optional<FormatSettings> & format_settings_,
            const ContextPtr & context_)
            : WithContext(context_), format(format_), compression_method(compression_method_), headers(headers_), format_settings(format_settings_)
        {
            url_options_to_check.reserve(urls_to_check_.size());
            for (const auto & url : urls_to_check_)
                url_options_to_check.push_back(getFailoverOptions(url, getContext()->getSettingsRef().glob_expansion_max_elements));
        }

        std::pair<std::unique_ptr<ReadBuffer>, std::optional<ColumnsDescription>> next() override
        {
            bool is_first = (current_index == 0);
            /// For default mode check cached columns for all urls on first iteration.
            if (is_first && getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT)
            {
                for (const auto & options : url_options_to_check)
                {
                    if (auto cached_columns = tryGetColumnsFromCache(options))
                        return {nullptr, cached_columns};
                }
            }

            std::pair<Poco::URI, std::unique_ptr<ReadWriteBufferFromHTTP>> uri_and_buf;
            do
            {
                if (current_index == url_options_to_check.size())
                {
                    if (is_first)
                        throw Exception(
                            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                            "Cannot extract table structure from {} format file, because all files are empty. "
                            "You must specify table structure manually",
                            format);
                    return {nullptr, std::nullopt};
                }

                if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::UNION)
                {
                    if (auto cached_columns = tryGetColumnsFromCache(url_options_to_check[current_index]))
                    {
                        ++current_index;
                        return {nullptr, cached_columns};
                    }
                }

                auto first_option = url_options_to_check[current_index].cbegin();
                uri_and_buf = StorageURLSource::getFirstAvailableURIAndReadBuffer(
                    first_option,
                    url_options_to_check[current_index].cend(),
                    getContext(),
                    {},
                    Poco::Net::HTTPRequest::HTTP_GET,
                    {},
                    getHTTPTimeouts(getContext()),
                    credentials,
                    headers,
                    false,
                    false);

                ++current_index;
            } while (getContext()->getSettingsRef().engine_url_skip_empty_files && uri_and_buf.second->eof());

            current_url_option = uri_and_buf.first.toString();
            return {wrapReadBufferWithCompressionMethod(
                std::move(uri_and_buf.second),
                compression_method,
                static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max)), std::nullopt};
        }

        void setNumRowsToLastFile(size_t num_rows) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_url)
                return;

            auto key = getKeyForSchemaCache(current_url_option, format, format_settings, getContext());
            StorageURL::getSchemaCache(getContext()).addNumRows(key, num_rows);
        }

        void setSchemaToLastFile(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_url
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::UNION)
                return;

            auto key = getKeyForSchemaCache(current_url_option, format, format_settings, getContext());
            StorageURL::getSchemaCache(getContext()).addColumns(key, columns);
        }

        void setResultingSchema(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_url
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::DEFAULT)
                return;

            for (const auto & options : url_options_to_check)
            {
                auto keys = getKeysForSchemaCache(options, format, format_settings, getContext());
                StorageURL::getSchemaCache(getContext()).addManyColumns(keys, columns);
            }
        }

        String getLastFileName() const override { return current_url_option; }

    private:
        std::optional<ColumnsDescription> tryGetColumnsFromCache(const Strings & urls)
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_url)
                return std::nullopt;

            auto & schema_cache = StorageURL::getSchemaCache(getContext());
            for (const auto & url : urls)
            {
                auto get_last_mod_time = [&]() -> std::optional<time_t>
                {
                    auto last_mod_time = StorageURL::tryGetLastModificationTime(url, headers, credentials, getContext());
                    /// Some URLs could not have Last-Modified header, in this case we cannot be sure that
                    /// data wasn't changed after adding it's schema to cache. Use schema from cache only if
                    /// special setting for this case is enabled.
                    if (!last_mod_time && !getContext()->getSettingsRef().schema_inference_cache_require_modification_time_for_url)
                        return 0;
                    return last_mod_time;
                };

                auto cache_key = getKeyForSchemaCache(url, format, format_settings, getContext());
                auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time);
                if (columns)
                    return columns;
            }

            return std::nullopt;
        }

        std::vector<std::vector<String>> url_options_to_check;
        size_t current_index = 0;
        String current_url_option;
        const String & format;
        const CompressionMethod & compression_method;
        const HTTPHeaderEntries & headers;
        Poco::Net::HTTPBasicCredentials credentials;
        const std::optional<FormatSettings> & format_settings;
    };
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
        urls_to_check = parseRemoteDescription(uri, 0, uri.size(), ',', context->getSettingsRef().glob_expansion_max_elements, "url");
    else
        urls_to_check = {uri};

    ReadBufferIterator read_buffer_iterator(urls_to_check, format, compression_method, headers, format_settings, context);
    return readSchemaFromFormat(format, format_settings, read_buffer_iterator, urls_to_check.size() > 1, context);
}

bool IStorageURLBase::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name, context, format_settings);
}

bool IStorageURLBase::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(format_name);
}

bool IStorageURLBase::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(format_name, context);
}

class ReadFromURL : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromURL"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters() override;

    ReadFromURL(
        Block sample_block,
        std::shared_ptr<IStorageURLBase> storage_,
        std::vector<String> * uri_options_,
        ReadFromFormatInfo info_,
        const bool need_only_count_,
        std::vector<std::pair<std::string, std::string>> read_uri_params_,
        std::function<void(std::ostream &)> read_post_data_callback_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(DataStream{.header = std::move(sample_block)})
        , storage(std::move(storage_))
        , uri_options(uri_options_)
        , info(std::move(info_))
        , need_only_count(need_only_count_)
        , read_uri_params(std::move(read_uri_params_))
        , read_post_data_callback(std::move(read_post_data_callback_))
        , context(std::move(context_))
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {
    }

private:
    std::shared_ptr<IStorageURLBase> storage;
    std::vector<String> * uri_options;

    ReadFromFormatInfo info;
    const bool need_only_count;
    std::vector<std::pair<std::string, std::string>> read_uri_params;
    std::function<void(std::ostream &)> read_post_data_callback;

    ContextPtr context;

    size_t max_block_size;
    size_t num_streams;

    std::shared_ptr<StorageURLSource::IteratorWrapper> iterator_wrapper;
    bool is_url_with_globs = false;
    bool is_empty_glob = false;

    void createIterator(const ActionsDAG::Node * predicate);
};

void ReadFromURL::applyFilters()
{
    auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes);
    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    createIterator(predicate);
}

void IStorageURLBase::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto params = getReadURIParams(column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size);
    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(local_context), getVirtuals());

    bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && local_context->getSettingsRef().optimize_count_from_files;

    auto read_post_data_callback = getReadPOSTDataCallback(
        read_from_format_info.columns_description.getNamesOfPhysical(),
        read_from_format_info.columns_description,
        query_info,
        local_context,
        processed_stage,
        max_block_size);

    auto this_ptr = std::static_pointer_cast<IStorageURLBase>(shared_from_this());

    auto reading = std::make_unique<ReadFromURL>(
        read_from_format_info.source_header,
        std::move(this_ptr),
        nullptr,
        std::move(read_from_format_info),
        need_only_count,
        std::move(params),
        std::move(read_post_data_callback),
        local_context,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(reading));
}

void ReadFromURL::createIterator(const ActionsDAG::Node * predicate)
{
    if (iterator_wrapper || is_empty_glob)
        return;

    if (uri_options)
    {
        iterator_wrapper = std::make_shared<StorageURLSource::IteratorWrapper>([&, done = false]() mutable
        {
            if (done)
                return StorageURLSource::FailoverOptions{};
            done = true;
            return *uri_options;
        });

        return;
    }

    size_t max_addresses = context->getSettingsRef().glob_expansion_max_elements;
    is_url_with_globs = urlWithGlobs(storage->uri);

    if (storage->distributed_processing)
    {
        iterator_wrapper = std::make_shared<StorageURLSource::IteratorWrapper>(
            [callback = context->getReadTaskCallback(), max_addresses]()
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
        auto glob_iterator = std::make_shared<StorageURLSource::DisclosedGlobIterator>(storage->uri, max_addresses, predicate, storage->virtual_columns, context);

        /// check if we filtered out all the paths
        if (glob_iterator->size() == 0)
        {
            is_empty_glob = true;
            return;
        }

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
        iterator_wrapper = std::make_shared<StorageURLSource::IteratorWrapper>([max_addresses, done = false, &uri = storage->uri]() mutable
        {
            if (done)
                return StorageURLSource::FailoverOptions{};
            done = true;
            return getFailoverOptions(uri, max_addresses);
        });
        num_streams = 1;
    }
}

void ReadFromURL::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    createIterator(nullptr);

    if (is_empty_glob)
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(info.source_header)));
        return;
    }

    Pipes pipes;
    pipes.reserve(num_streams);

    const size_t max_threads = context->getSettingsRef().max_threads;
    const size_t max_parsing_threads = num_streams >= max_threads ? 1 : (max_threads / num_streams);

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<StorageURLSource>(
            info,
            iterator_wrapper,
            storage->getReadMethod(),
            read_post_data_callback,
            storage->format_name,
            storage->format_settings,
            storage->getName(),
            context,
            max_block_size,
            getHTTPTimeouts(context),
            storage->compression_method,
            max_parsing_threads,
            storage->headers,
            read_uri_params,
            is_url_with_globs,
            need_only_count);

        source->setKeyCondition(filter_nodes.nodes, context);
        pipes.emplace_back(std::move(source));
    }

    if (uri_options)
        std::shuffle(uri_options->begin(), uri_options->end(), thread_local_rng);

    auto pipe = Pipe::unitePipes(std::move(pipes));
    size_t output_ports = pipe.numOutputPorts();
    const bool parallelize_output = context->getSettingsRef().parallelize_output_from_storages;
    if (parallelize_output && storage->parallelizeOutputAfterReading(context) && output_ports > 0 && output_ports < num_streams)
        pipe.resize(num_streams);

    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(info.source_header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}


void StorageURLWithFailover::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto params = getReadURIParams(column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size);
    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(local_context), getVirtuals());

    bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && local_context->getSettingsRef().optimize_count_from_files;

    auto read_post_data_callback = getReadPOSTDataCallback(
        read_from_format_info.columns_description.getNamesOfPhysical(),
        read_from_format_info.columns_description,
        query_info,
        local_context,
        processed_stage,
        max_block_size);

    auto this_ptr = std::static_pointer_cast<StorageURL>(shared_from_this());

    auto reading = std::make_unique<ReadFromURL>(
        read_from_format_info.source_header,
        std::move(this_ptr),
        &uri_options,
        std::move(read_from_format_info),
        need_only_count,
        std::move(params),
        std::move(read_post_data_callback),
        local_context,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(reading));
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
    return virtual_columns;
}

Names IStorageURLBase::getVirtualColumnNames()
{
    return VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage({}).getNames();
}

SchemaCache & IStorageURLBase::getSchemaCache(const ContextPtr & context)
{
    static SchemaCache schema_cache(context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_url", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

std::optional<time_t> IStorageURLBase::tryGetLastModificationTime(
    const String & url,
    const HTTPHeaderEntries & headers,
    const Poco::Net::HTTPBasicCredentials & credentials,
    const ContextPtr & context)
{
    auto settings = context->getSettingsRef();

    auto uri = Poco::URI(url);

    auto proxy_config = getProxyConfiguration(uri.getScheme());

    ReadWriteBufferFromHTTP buf(
        uri,
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
        false,
        std::nullopt,
        proxy_config);

    return buf.tryGetLastModificationTime();
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
        LOG_DEBUG(getLogger("StorageURLDistributed"), "Adding URL option: {}", uri_option);
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

size_t StorageURL::evalArgsAndCollectHeaders(
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

    if (headers_it == url_function_args.end())
        return url_function_args.size();

    std::rotate(headers_it, std::next(headers_it), url_function_args.end());
    return url_function_args.size() - 1;
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
        evalArgsAndCollectHeaders(args, configuration.headers, local_context);
    }
    else
    {
        size_t count = evalArgsAndCollectHeaders(args, configuration.headers, local_context);

        if (count == 0 || count > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, bad_arguments_error_message);

        configuration.url = checkAndGetLiteralArgument<String>(args[0], "url");
        if (count > 1)
            configuration.format = checkAndGetLiteralArgument<String>(args[1], "format");
        if (count == 3)
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
