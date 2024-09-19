#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTPHandlerRequestFilter.h>
#include <Server/HTTPRequestHandlerFactoryMain.h>
#include <Common/StringUtils.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

class IServer;
class AsynchronousMetrics;

template <typename TEndpoint>
class HandlingRuleHTTPHandlerFactory : public HTTPRequestHandlerFactory
{
public:
    using Filter = std::function<bool(const HTTPServerRequest &)>;

    using Creator = std::function<std::unique_ptr<TEndpoint>()>;
    explicit HandlingRuleHTTPHandlerFactory(Creator && creator_)
        : creator(std::move(creator_))
    {}

    explicit HandlingRuleHTTPHandlerFactory(IServer & server)
    {
        creator = [&server]() -> std::unique_ptr<TEndpoint> { return std::make_unique<TEndpoint>(server); };
    }

    void addFilter(Filter cur_filter)
    {
        Filter prev_filter = filter;
        filter = [prev_filter, cur_filter](const auto & request)
        {
            return prev_filter ? prev_filter(request) && cur_filter(request) : cur_filter(request);
        };
    }

    void addFiltersFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & prefix)
    {
        Poco::Util::AbstractConfiguration::Keys filters_type;
        config.keys(prefix, filters_type);

        for (const auto & filter_type : filters_type)
        {
            if (filter_type == "handler")
                continue;
            else if (filter_type == "url")
                addFilter(urlFilter(config, prefix + ".url"));
            else if (filter_type == "empty_query_string")
                addFilter(emptyQueryStringFilter());
            else if (filter_type == "headers")
                addFilter(headersFilter(config, prefix + ".headers"));
            else if (filter_type == "methods")
                addFilter(methodsFilter(config, prefix + ".methods"));
            else
                throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: {}.{}", prefix, filter_type);
        }
    }

    void attachStrictPath(const String & strict_path)
    {
        addFilter([strict_path](const auto & request) { return request.getURI() == strict_path; });
    }

    void attachNonStrictPath(const String & non_strict_path)
    {
        addFilter([non_strict_path](const auto & request) { return startsWith(request.getURI(), non_strict_path); });
    }

    /// Handle GET or HEAD endpoint on specified path
    void allowGetAndHeadRequest()
    {
        addFilter([](const auto & request)
        {
            return request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
                || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD;
        });
    }

    /// Handle Post request or (Get or Head) with params or OPTIONS requests
    void allowPostAndGetParamsAndOptionsRequest()
    {
        addFilter([](const auto & request)
        {
            return (request.getURI().find('?') != std::string::npos
                && (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
                || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD))
                || request.getMethod() == Poco::Net::HTTPRequest::HTTP_OPTIONS
                || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST;
        });
    }

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override
    {
        return filter(request) ? creator() : nullptr;
    }

private:
    Filter filter;
    std::function<std::unique_ptr<HTTPRequestHandler> ()> creator;
};

HTTPRequestHandlerFactoryPtr createStaticHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix);

HTTPRequestHandlerFactoryPtr createDynamicHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix);

HTTPRequestHandlerFactoryPtr createPredefinedHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix);

HTTPRequestHandlerFactoryPtr createReplicasStatusHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix);

/// @param server - used in handlers to check IServer::isCancelled()
/// @param config - not the same as server.config(), since it can be newer
/// @param async_metrics - used for prometheus (in case of prometheus.asynchronous_metrics=true)
HTTPRequestHandlerFactoryPtr createHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    AsynchronousMetrics & async_metrics,
    const std::string & name);

}
