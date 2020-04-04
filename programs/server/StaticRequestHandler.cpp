#include "StaticRequestHandler.h"

#include "HTTPHandlerFactory.h"
#include "HTTPHandlerRequestFilter.h"

#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>

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
    extern const int INVALID_CONFIG_PARAMETER;
}

void StaticRequestHandler::handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response)
{
    try
    {
        setResponseDefaultHeaders(response, server.config().getUInt("keep_alive_timeout", 10));

        response.setContentType(content_type);
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTPStatus(status));

        response.sendBuffer(response_content.data(), response_content.size());
    }
    catch (...)
    {
        tryLogCurrentException("StaticRequestHandler");
    }
}

StaticRequestHandler::StaticRequestHandler(IServer & server_, const String & expression, int status_, const String & content_type_)
    : server(server_), status(status_), content_type(content_type_)
{
    static const String file_prefix = "file://";
    static const String config_prefix = "config://";

    if (startsWith(expression, file_prefix))
    {
        std::string config_dir = Poco::Path(server.context().getPath()).parent().toString();
        const std::string & file_path = config_dir + expression.substr(file_prefix.size(), expression.size() - file_prefix.size());

        if (!Poco::File(file_path).exists())
            throw Exception("Invalid file name for static HTTPHandler." + file_path, ErrorCodes::INCORRECT_FILE_NAME);

        WriteBufferFromOwnString out;
        ReadBufferFromFile in(file_path);
        copyData(in, out);
        response_content = out.str();
    }
    else if (startsWith(expression, config_prefix))
    {
        if (expression.size() <= config_prefix.size())
            throw Exception("Static routing rule handler must contain a complete configuration path, for example: config://config_key",
                ErrorCodes::INVALID_CONFIG_PARAMETER);

        response_content = server.config().getString(expression.substr(config_prefix.size(), expression.size() - config_prefix.size()), "Ok.\n");
    }
    else
        response_content = expression;
}

Poco::Net::HTTPRequestHandlerFactory * createStaticHandlerFactory(IServer & server, const std::string & config_prefix)
{
    const auto & status = server.config().getInt(config_prefix + ".handler.status", 200);
    const auto & response_content = server.config().getRawString(config_prefix + ".handler.response_content", "Ok.\n");
    const auto & response_content_type = server.config().getString(config_prefix + ".handler.content_type", "text/plain; charset=UTF-8");

    return addFiltersFromConfig(new RoutingRuleHTTPHandlerFactory<StaticRequestHandler>(
        server, response_content, status, response_content_type), server.config(), config_prefix);
}

}
