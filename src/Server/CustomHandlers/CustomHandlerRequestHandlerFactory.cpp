#include <Server/CustomHandlers/CustomHandlerRequestHandlerFactory.h>
#include <Server/CustomHandlers/CustomHandlersFactory.h>
#include <Server/HTTPHandler.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/IServer.h>
#include <Common/re2.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/String.h>

#include <algorithm>


namespace DB
{

namespace
{

/// Extracts the path portion of the URI (before '?')
std::string getRequestPath(const std::string & uri)
{
    auto pos = uri.find('?');
    if (pos != std::string::npos)
        return uri.substr(0, pos);
    return uri;
}

bool matchesURL(const CustomHandlerDefinition & def, const std::string & request_uri)
{
    std::string request_path = getRequestPath(request_uri);

    if (def.url_type == "exact")
        return request_path == def.url;

    if (def.url_type == "prefix")
        return startsWith(request_path, def.url);

    if (def.url_type == "regexp")
    {
        if (!def.compiled_regex || !def.compiled_regex->ok())
            return false;
        return re2::RE2::FullMatch(request_path, *def.compiled_regex);
    }

    return false;
}

bool matchesMethod(const CustomHandlerDefinition & def, const std::string & method)
{
    if (def.methods.empty())
        return method == Poco::Net::HTTPRequest::HTTP_GET;

    return std::any_of(def.methods.begin(), def.methods.end(),
        [&method](const std::string & m)
        {
            return Poco::icompare(m, method) == 0;
        });
}

class CustomHandlerRequestHandlerFactory : public HTTPRequestHandlerFactory
{
public:
    explicit CustomHandlerRequestHandlerFactory(IServer & server_) : server(server_) {}

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override
    {
        auto handlers = CustomHandlersFactory::instance().getAll();

        /// No SQL-defined handlers exist — skip matching entirely.
        if (handlers.empty())
            return nullptr;

        /// Sort by name for deterministic matching order
        std::sort(handlers.begin(), handlers.end(),
            [](const CustomHandlerDefinition & a, const CustomHandlerDefinition & b)
            {
                return a.name < b.name;
            });

        for (const auto & handler : handlers)
        {
            if (!matchesURL(handler, request.getURI()))
                continue;

            if (!matchesMethod(handler, request.getMethod()))
                continue;

            /// Create a PredefinedQueryHandler for this match
            NameSet receive_params;
            CompiledRegexPtr url_regex = handler.compiled_regex;
            std::unordered_map<String, CompiledRegexPtr> header_name_with_regex;

            return std::make_unique<PredefinedQueryHandler>(
                server,
                HTTPHandlerConnectionConfig{},
                receive_params,
                handler.query,
                url_regex,
                header_name_with_regex);
        }

        return nullptr;
    }

private:
    IServer & server;
};

}

HTTPRequestHandlerFactoryPtr createCustomHandlerRequestHandlerFactory(IServer & server)
{
    return std::make_shared<CustomHandlerRequestHandlerFactory>(server);
}

}
