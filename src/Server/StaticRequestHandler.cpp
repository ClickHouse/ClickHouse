#include "StaticRequestHandler.h"

#include "HTTPHandlerFactory.h"
#include "HTTPHandlerRequestFilter.h"

#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>

#include <Common/Exception.h>

#include <Poco/Path.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_FILE_NAME;
    extern const int HTTP_LENGTH_REQUIRED;
    extern const int INVALID_CONFIG_PARAMETER;
}

static inline WriteBufferPtr responseWriteBuffer(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, unsigned int keep_alive_timeout)
{
    /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
    String http_response_compression_methods = request.get("Accept-Encoding", "");
    CompressionMethod http_response_compression_method = CompressionMethod::None;

    if (!http_response_compression_methods.empty())
    {
        /// If client supports brotli - it's preferred.
        /// Both gzip and deflate are supported. If the client supports both, gzip is preferred.
        /// NOTE parsing of the list of methods is slightly incorrect.

        if (std::string::npos != http_response_compression_methods.find("br"))
            http_response_compression_method = CompressionMethod::Brotli;
        else if (std::string::npos != http_response_compression_methods.find("gzip"))
            http_response_compression_method = CompressionMethod::Gzip;
        else if (std::string::npos != http_response_compression_methods.find("deflate"))
            http_response_compression_method = CompressionMethod::Zlib;
    }

    bool client_supports_http_compression = http_response_compression_method != CompressionMethod::None;

    return std::make_shared<WriteBufferFromHTTPServerResponse>(
        request, response, keep_alive_timeout, client_supports_http_compression, http_response_compression_method);
}

static inline void trySendExceptionToClient(
    const std::string & s, int exception_code,
    Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response , WriteBuffer & out)
{
    try
    {
        response.set("X-ClickHouse-Exception-Code", toString<int>(exception_code));

        /// If HTTP method is POST and Keep-Alive is turned on, we should read the whole request body
        /// to avoid reading part of the current request body in the next request.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
            && response.getKeepAlive() && !request.stream().eof() && exception_code != ErrorCodes::HTTP_LENGTH_REQUIRED)
            request.stream().ignore(std::numeric_limits<std::streamsize>::max());

        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

        if (!response.sent())
            response.send() << s << std::endl;
        else
        {
            if (out.count() != out.offset())
                out.position() = out.buffer().begin();

            writeString(s, out);
            writeChar('\n', out);

            out.next();
            out.finalize();
        }
    }
    catch (...)
    {
        tryLogCurrentException("StaticRequestHandler", "Cannot send exception to client");
    }
}

void StaticRequestHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    auto keep_alive_timeout = server.config().getUInt("keep_alive_timeout", 10);
    const auto & out = responseWriteBuffer(request, response, keep_alive_timeout);

    try
    {
        response.setContentType(content_type);

        if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        /// Workaround. Poco does not detect 411 Length Required case.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && !request.getChunkedTransferEncoding() && !request.hasContentLength())
            throw Exception("The Transfer-Encoding is not chunked and there is no Content-Length header for POST request", ErrorCodes::HTTP_LENGTH_REQUIRED);

        setResponseDefaultHeaders(response, keep_alive_timeout);
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTPStatus(status));
        writeResponse(*out);
    }
    catch (...)
    {
        tryLogCurrentException("StaticRequestHandler");

        int exception_code = getCurrentExceptionCode();
        std::string exception_message = getCurrentExceptionMessage(false, true);
        trySendExceptionToClient(exception_message, exception_code, request, response, *out);
    }
}

void StaticRequestHandler::writeResponse(WriteBuffer & out)
{
    static const String file_prefix = "file://";
    static const String config_prefix = "config://";

    if (startsWith(response_expression, file_prefix))
    {
        const auto & user_files_absolute_path = Poco::Path(server.context().getUserFilesPath()).makeAbsolute().makeDirectory().toString();
        const auto & file_name = response_expression.substr(file_prefix.size(), response_expression.size() - file_prefix.size());

        const auto & file_path = Poco::Path(user_files_absolute_path, file_name).makeAbsolute().toString();
        if (!Poco::File(file_path).exists())
            throw Exception("Invalid file name " + file_path + " for static HTTPHandler. ", ErrorCodes::INCORRECT_FILE_NAME);

        ReadBufferFromFile in(file_path);
        copyData(in, out);
    }
    else if (startsWith(response_expression, config_prefix))
    {
        if (response_expression.size() <= config_prefix.size())
            throw Exception( "Static handling rule handler must contain a complete configuration path, for example: config://config_key",
                ErrorCodes::INVALID_CONFIG_PARAMETER);

        const auto & config_path = response_expression.substr(config_prefix.size(), response_expression.size() - config_prefix.size());
        writeString(server.config().getRawString(config_path, "Ok.\n"), out);
    }
    else
        writeString(response_expression, out);
}

StaticRequestHandler::StaticRequestHandler(IServer & server_, const String & expression, int status_, const String & content_type_)
    : server(server_), status(status_), content_type(content_type_), response_expression(expression)
{
}

Poco::Net::HTTPRequestHandlerFactory * createStaticHandlerFactory(IServer & server, const std::string & config_prefix)
{
    int status = server.config().getInt(config_prefix + ".handler.status", 200);
    std::string response_content = server.config().getRawString(config_prefix + ".handler.response_content", "Ok.\n");
    std::string response_content_type = server.config().getString(config_prefix + ".handler.content_type", "text/plain; charset=UTF-8");

    return addFiltersFromConfig(new HandlingRuleHTTPHandlerFactory<StaticRequestHandler>(
        server, std::move(response_content), std::move(status), std::move(response_content_type)), server.config(), config_prefix);
}

}
