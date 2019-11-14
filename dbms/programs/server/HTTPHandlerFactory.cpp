#include "HTTPHandlerFactory.h"

#include "NotFoundHandler.h"
#include "HTTPRequestHandler/HTTPRootRequestHandler.h"
#include "HTTPRequestHandler/HTTPPingRequestHandler.h"
#include "HTTPRequestHandler/HTTPReplicasStatusRequestHandler.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int UNKNOW_HTTP_HANDLER_TYPE;
    extern const int EMPTY_HTTP_HANDLER_IN_CONFIG;
}

InterserverIOHTTPHandlerFactory::InterserverIOHTTPHandlerFactory(IServer & server_, const std::string & name_)
    : server(server_), log(&Logger::get(name_)), name(name_)
{
}

Poco::Net::HTTPRequestHandler * InterserverIOHTTPHandlerFactory::createRequestHandler(const Poco::Net::HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP Request for " << name << ". "
       << "Method: " << request.getMethod()
       << ", Address: " << request.clientAddress().toString()
       << ", User-Agent: " << (request.has("User-Agent") ? request.get("User-Agent") : "none")
       << (request.hasContentLength() ? (", Length: " + std::to_string(request.getContentLength())) : (""))
       << ", Content Type: " << request.getContentType()
       << ", Transfer Encoding: " << request.getTransferEncoding());

    const auto & uri = request.getURI();

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD)
    {
        if (uri == "/")
            return new HTTPRootRequestHandler(server);
        if (uri == "/ping")
            return new HTTPPingRequestHandler(server);
        else if (startsWith(uri, "/replicas_status"))
            return new HTTPReplicasStatusRequestHandler(server.context());
    }

    if (uri.find('?') != std::string::npos || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        return new InterserverIOHTTPHandler(server);
    }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD
        || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        return new NotFoundHandler(
            "Use / or /ping for health checks.\n"
            "Or /replicas_status for more sophisticated health checks.\n"
            "Send queries from your program with POST method or GET /?query=...\n\n"
            "   Use clickhouse-client:\n\n"
            "   For interactive data analysis:\n"
            "       clickhouse-client\n\n"
            "   For batch query processing:\n"
            "       clickhouse-client --query='SELECT 1' > result\n"
            "       clickhouse-client < query > result"
        );
    }

    return nullptr;
}

HTTPHandlerFactory::HTTPHandlerFactory(IServer & server_, const std::string & name_)
    : server(server_), log(&Logger::get(name_)), name(name_)
{
    updateHTTPHandlersCreator(server.config());

    if (handlers_creator.empty())
        throw Exception("The HTTPHandlers does not exist in the config.xml", ErrorCodes::EMPTY_HTTP_HANDLER_IN_CONFIG);
}

Poco::Net::HTTPRequestHandler * HTTPHandlerFactory::createRequestHandler(const Poco::Net::HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP Request for " << name << ". "
       << "Method: " << request.getMethod()
       << ", Address: " << request.clientAddress().toString()
       << ", User-Agent: " << (request.has("User-Agent") ? request.get("User-Agent") : "none")
       << (request.hasContentLength() ? (", Length: " + std::to_string(request.getContentLength())) : (""))
       << ", Content Type: " << request.getContentType()
       << ", Transfer Encoding: " << request.getTransferEncoding());

    for (const auto & [matcher, creator] : handlers_creator)
    {
        if (matcher(request))
            return creator();
    }

    return new NotFoundHandler(no_handler_description);
}

HTTPHandlerMatcher createRootHandlerMatcher(IServer &, const String &);
HTTPHandlerMatcher createPingHandlerMatcher(IServer &, const String &);
HTTPHandlerMatcher createDynamicQueryHandlerMatcher(IServer &, const String &);
HTTPHandlerMatcher createReplicasStatusHandlerMatcher(IServer &, const String &);
HTTPHandlerMatcher createPredefineQueryHandlerMatcher(IServer &, const String &);

HTTPHandlerCreator createRootHandlerCreator(IServer &, const String &);
HTTPHandlerCreator createPingHandlerCreator(IServer &, const String &);
HTTPHandlerCreator createDynamicQueryHandlerCreator(IServer &, const String &);
HTTPHandlerCreator createReplicasStatusHandlerCreator(IServer &, const String &);
HTTPHandlerCreator createPredefineQueryHandlerCreator(IServer &, const String &);

void HTTPHandlerFactory::updateHTTPHandlersCreator(Poco::Util::AbstractConfiguration & configuration, const String & key)
{
    Poco::Util::AbstractConfiguration::Keys http_handlers_item_key;
    configuration.keys(key, http_handlers_item_key);

    handlers_creator.reserve(http_handlers_item_key.size());
    for (const auto & http_handler_type_name : http_handlers_item_key)
    {
        if (http_handler_type_name.find('.') != String::npos)
            throw Exception("HTTPHandler type name with dots are not supported: '" + http_handler_type_name + "'", ErrorCodes::SYNTAX_ERROR);

        const auto & handler_key = key + "." + http_handler_type_name;

        if (startsWith(http_handler_type_name, "root_handler"))
            handlers_creator.push_back({createRootHandlerMatcher(server, handler_key), createRootHandlerCreator(server, handler_key)});
        else if (startsWith(http_handler_type_name, "ping_handler"))
            handlers_creator.push_back({createPingHandlerMatcher(server, handler_key), createPingHandlerCreator(server, handler_key)});
        else if (startsWith(http_handler_type_name, "dynamic_query_handler"))
            handlers_creator.push_back({createDynamicQueryHandlerMatcher(server, handler_key), createDynamicQueryHandlerCreator(server, handler_key)});
        else if (startsWith(http_handler_type_name, "predefine_query_handler"))
            handlers_creator.push_back({createPredefineQueryHandlerMatcher(server, handler_key), createPredefineQueryHandlerCreator(server, handler_key)});
        else if (startsWith(http_handler_type_name, "replicas_status_handler"))
            handlers_creator.push_back({createReplicasStatusHandlerMatcher(server, handler_key), createReplicasStatusHandlerCreator(server, handler_key)});
        else if (http_handler_type_name == "no_handler_description")
            no_handler_description = configuration.getString(key + ".no_handler_description");
        else
            throw Exception("Unknown HTTPHandler type name: " + http_handler_type_name, ErrorCodes::UNKNOW_HTTP_HANDLER_TYPE);
    }
}

}
