#include "HTTPQueryRequestHandler.h"

#include <chrono>
#include <iomanip>
#include <Poco/File.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerRequestImpl.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/NetException.h>
#include <ext/scope_guard.h>
#include <Core/ExternalTable.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/config.h>
#include <Common/SettingsChanges.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <IO/BrotliReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/executeQuery.h>
#include <DataStreams/HTTPInputStreams.h>
#include <Common/typeid_cast.h>
#include <Poco/Net/HTTPStream.h>

#include "ExtractorClientInfo.h"
#include "ExtractorContextChange.h"
#include "HTTPQueryRequestHandlerMatcherAndCreator.h"
#include "HTTPSessionContextHolder.h"
#include "HTTPExceptionHandler.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int HTTP_LENGTH_REQUIRED;
}

template <typename QueryParamExtractor>
HTTPQueryRequestHandler<QueryParamExtractor>::HTTPQueryRequestHandler(const IServer & server_, const QueryParamExtractor & extractor_)
    : server(server_), log(&Logger::get("HTTPQueryRequestHandler")), extractor(extractor_)
{
    server_display_name = server.config().getString("display_name", getFQDNOrHostName());
}

template <typename QueryParamExtractor>
void HTTPQueryRequestHandler<QueryParamExtractor>::processQuery(
    Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params,
    Poco::Net::HTTPServerResponse & response, HTTPResponseBufferPtr & response_out)
{
    ExtractorClientInfo{context.getClientInfo()}.extract(request);
    ExtractorContextChange{context, extractor.loadSettingsFromPost()}.extract(request, params);

    HTTPInputStreams input_streams{context, request, params};
    HTTPOutputStreams output_streams = HTTPOutputStreams(response_out, context, request, params);

    const auto & queries = extractor.extract(context, request, params);

    for (const auto & [execute_query, not_touch_post] : queries)
    {
        ReadBufferPtr temp_query_buf;
        ReadBufferPtr execute_query_buf = std::make_shared<ReadBufferFromString>(execute_query);

        if (not_touch_post && !startsWith(request.getContentType().data(), "multipart/form-data"))
        {
            temp_query_buf = execute_query_buf; /// we create a temporary reference for not to be destroyed
            execute_query_buf = std::make_unique<ConcatReadBuffer>(*temp_query_buf, *input_streams.in_maybe_internal_compressed);
        }

        executeQuery(
            *execute_query_buf, *output_streams.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false,
            context, [&response] (const String & content_type) { response.setContentType(content_type); },
            [&response] (const String & current_query_id) { response.add("X-ClickHouse-Query-Id", current_query_id); }
        );
    }

    /// Send HTTP headers with code 200 if no exception happened and the data is still not sent to the client.
    output_streams.finalize();
}

template <typename QueryParamExtractor>
void HTTPQueryRequestHandler<QueryParamExtractor>::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    setThreadName("HTTPHandler");
    ThreadStatus thread_status;

    /// In case of exception, send stack trace to client.
    HTTPResponseBufferPtr response_out;
    bool with_stacktrace = false, internal_compression = false;

    try
    {
        response_out = createResponseOut(request, response);
        response.set("Content-Type", "text/plain; charset=UTF-8");
        response.set("X-ClickHouse-Server-Display-Name", server_display_name);

        /// For keep-alive to work.
        if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        HTMLForm params(request);
        with_stacktrace = params.getParsed<bool>("stacktrace", false);
        internal_compression = params.getParsed<bool>("compress", false);

        /// Workaround. Poco does not detect 411 Length Required case.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && !request.getChunkedTransferEncoding() && !request.hasContentLength())
            throw Exception("There is neither Transfer-Encoding header nor Content-Length header", ErrorCodes::HTTP_LENGTH_REQUIRED);

        {
            Context query_context = server.context();
            CurrentThread::QueryScope query_scope(query_context);

            HTTPSessionContextHolder holder{query_context, request, params};
            processQuery(holder.query_context, request, params, response, response_out);
            LOG_INFO(log, "Done processing query");
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */
        int exception_code = getCurrentExceptionCode();
        std::string exception_message = getCurrentExceptionMessage(with_stacktrace, true);
        HTTPExceptionHandler::handle(exception_message, exception_code, request, response, response_out, internal_compression, log);
    }
}

template <typename QueryParamExtractor>
HTTPResponseBufferPtr HTTPQueryRequestHandler<QueryParamExtractor>::createResponseOut(HTTPServerRequest & request, HTTPServerResponse & response)
{
    size_t keep_alive = server.config().getUInt("keep_alive_timeout", 10);
    /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
    String http_response_compression_methods = request.get("Accept-Encoding", "");

    if (!http_response_compression_methods.empty())
    {
        /// Both gzip and deflate are supported. If the client supports both, gzip is preferred.
        /// NOTE parsing of the list of methods is slightly incorrect.
        if (std::string::npos != http_response_compression_methods.find("gzip"))
            return std::make_shared<WriteBufferFromHTTPServerResponse>(
                request, response, keep_alive, true, CompressionMethod::Gzip, DBMS_DEFAULT_BUFFER_SIZE);
        else if (std::string::npos != http_response_compression_methods.find("deflate"))
            return std::make_shared<WriteBufferFromHTTPServerResponse>(
                request, response, keep_alive, true, CompressionMethod::Zlib, DBMS_DEFAULT_BUFFER_SIZE);
#if USE_BROTLI
        else if (http_response_compression_methods == "br")
            return std::make_shared<WriteBufferFromHTTPServerResponse>(
                request, response, keep_alive, true, CompressionMethod::Brotli, DBMS_DEFAULT_BUFFER_SIZE);
#endif
    }

    return std::make_shared<WriteBufferFromHTTPServerResponse>(request, response, keep_alive, false, CompressionMethod{}, DBMS_DEFAULT_BUFFER_SIZE);
}


template class HTTPQueryRequestHandler<ExtractorDynamicQueryParameters>;
template class HTTPQueryRequestHandler<ExtractorPredefinedQueryParameters>;

}
