#include <Server/StaticRequestHandler.h>
#include <Server/IServer.h>

#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTPResponseHeaderWriter.h>

#include <Core/ServerSettings.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

#include <Common/Exception.h>

#include <memory>
#include <unordered_map>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_FILE_NAME;
    extern const int HTTP_LENGTH_REQUIRED;
    extern const int INVALID_CONFIG_PARAMETER;
}

struct ResponseOutput
{
    std::unique_ptr<WriteBufferFromHTTPServerResponse> response_holder;
    std::unique_ptr<WriteBuffer> compression_holder;

    explicit ResponseOutput(std::unique_ptr<WriteBufferFromHTTPServerResponse> && buf)
        : response_holder(std::move(buf))
    {
    }

    void setCompressedOut(std::unique_ptr<WriteBuffer> && buf)
    {
        chassert(response_holder);
        chassert(!compression_holder);
        compression_holder = std::move(buf);
    }

    WriteBuffer * get() const
    {
        if (compression_holder)
            return compression_holder.get();
        return response_holder.get();
    }
};

static inline ResponseOutput responseWriteBuffer(HTTPServerRequest & request, HTTPServerResponse & response)
{
    auto result = ResponseOutput(std::make_unique<WriteBufferFromHTTPServerResponse>(response, request.getMethod() == HTTPRequest::HTTP_HEAD));

    /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
    String http_response_compression_methods = request.get("Accept-Encoding", "");
    CompressionMethod http_response_compression_method = CompressionMethod::None;

    if (!http_response_compression_methods.empty())
        http_response_compression_method = chooseHTTPCompressionMethod(http_response_compression_methods);

    if (http_response_compression_method == CompressionMethod::None)
        return result;

    response.set("Content-Encoding", toContentEncodingName(http_response_compression_method));
    result.setCompressedOut(wrapWriteBufferWithCompressionMethod(result.get(), http_response_compression_method, 1, 0));

    return result;
}

void StaticRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
{
    applyHTTPResponseHeaders(response, http_response_headers_override);

    if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    auto response_output = responseWriteBuffer(request, response);

    try
    {
        /// Workaround. Poco does not detect 411 Length Required case.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && !request.getChunkedTransferEncoding() && !request.hasContentLength())
            throw Exception(ErrorCodes::HTTP_LENGTH_REQUIRED,
                            "The Transfer-Encoding is not chunked and there "
                            "is no Content-Length header for POST request");

        setResponseDefaultHeaders(response);
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTPStatus(status));
        writeResponse(*response_output.get());
        response_output.get()->finalize();
    }
    catch (...)
    {
        tryLogCurrentException("StaticRequestHandler");
        response_output.response_holder->cancelWithException(
            request, getCurrentExceptionCode(), getCurrentExceptionMessage(false, true), response_output.compression_holder.get());
    }
}

void StaticRequestHandler::writeResponse(WriteBuffer & out)
{
    static const String file_prefix = "file://";
    static const String config_prefix = "config://";

    if (startsWith(response_expression, file_prefix))
    {
        auto file_name = response_expression.substr(file_prefix.size(), response_expression.size() - file_prefix.size());
        if (file_name.starts_with('/'))
            file_name = file_name.substr(1);

        fs::path user_files_absolute_path = fs::canonical(fs::path(server.context()->getUserFilesPath()));
        String file_path = fs::weakly_canonical(user_files_absolute_path / file_name);

        if (!fs::exists(file_path))
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Invalid file name {} for static HTTPHandler. ", file_path);

        ReadBufferFromFile in(file_path);
        copyData(in, out);
    }
    else if (startsWith(response_expression, config_prefix))
    {
        if (response_expression.size() <= config_prefix.size())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                            "Static handling rule handler must contain a complete configuration path, for example: "
                            "config://config_key");

        const auto & config_path = response_expression.substr(config_prefix.size(), response_expression.size() - config_prefix.size());
        writeString(server.config().getRawString(config_path, "Ok.\n"), out);
    }
    else
        writeString(response_expression, out);
}

StaticRequestHandler::StaticRequestHandler(
    IServer & server_, const String & expression, const std::unordered_map<String, String> & http_response_headers_override_, int status_)
    : server(server_), status(status_), http_response_headers_override(http_response_headers_override_), response_expression(expression)
{
}

HTTPRequestHandlerFactoryPtr createStaticHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    std::unordered_map<String, String> & common_headers)
{
    int status = config.getInt(config_prefix + ".handler.status", 200);
    std::string response_content = config.getRawString(config_prefix + ".handler.response_content", "Ok.\n");

    std::unordered_map<String, String> http_response_headers_override
        = parseHTTPResponseHeadersWithCommons(config, config_prefix, "text/plain; charset=UTF-8", common_headers);

    auto creator = [&server, http_response_headers_override, response_content, status]() -> std::unique_ptr<StaticRequestHandler>
    { return std::make_unique<StaticRequestHandler>(server, response_content, http_response_headers_override, status); };

    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<StaticRequestHandler>>(std::move(creator));

    factory->addFiltersFromConfig(config, config_prefix);

    return factory;
}

}
