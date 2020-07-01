#pragma once

#include "IServer.h"
#include <common/logger_useful.h>
#include <Common/HTMLForm.h>
#include <Common/StringUtils/StringUtils.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Interpreters/AsynchronousMetrics.h>

namespace DB
{

/// Handle request using child handlers
class HTTPRequestHandlerFactoryMain : public Poco::Net::HTTPRequestHandlerFactory, boost::noncopyable
{
private:
    using TThis = HTTPRequestHandlerFactoryMain;

    Poco::Logger * log;
    std::string name;

    std::vector<Poco::Net::HTTPRequestHandlerFactory *> child_factories;
public:

    ~HTTPRequestHandlerFactoryMain() override;

    HTTPRequestHandlerFactoryMain(const std::string & name_);

    TThis * addHandler(Poco::Net::HTTPRequestHandlerFactory * child_factory);

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override;
};

template <typename TEndpoint>
class HandlingRuleHTTPHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    using TThis = HandlingRuleHTTPHandlerFactory<TEndpoint>;
    using Filter = std::function<bool(const Poco::Net::HTTPServerRequest &)>;

    template <typename... TArgs>
    HandlingRuleHTTPHandlerFactory(TArgs &&... args)
    {
        creator = [args = std::tuple<TArgs...>(std::forward<TArgs>(args) ...)]()
        {
            return std::apply([&](auto && ... endpoint_args)
            {
                return new TEndpoint(std::forward<decltype(endpoint_args)>(endpoint_args)...);
            }, std::move(args));
        };
    }

    TThis * addFilter(Filter cur_filter)
    {
        Filter prev_filter = filter;
        filter = [prev_filter, cur_filter](const auto & request)
        {
            return prev_filter ? prev_filter(request) && cur_filter(request) : cur_filter(request);
        };

        return this;
    }

    TThis * attachStrictPath(const String & strict_path)
    {
        return addFilter([strict_path](const auto & request) { return request.getURI() == strict_path; });
    }

    TThis * attachNonStrictPath(const String & non_strict_path)
    {
        return addFilter([non_strict_path](const auto & request) { return startsWith(request.getURI(), non_strict_path); });
    }

    /// Handle GET or HEAD endpoint on specified path
    TThis * allowGetAndHeadRequest()
    {
        return addFilter([](const auto & request)
        {
            return request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
                || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD;
        });
    }

    /// Handle POST or GET with params
    TThis * allowPostAndGetParamsRequest()
    {
        return addFilter([](const auto & request)
        {
            return request.getURI().find('?') != std::string::npos
                || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST;
        });
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override
    {
        return filter(request) ? creator() : nullptr;
    }

private:
    Filter filter;
    std::function<Poco::Net::HTTPRequestHandler * ()> creator;
};

Poco::Net::HTTPRequestHandlerFactory * createStaticHandlerFactory(IServer & server, const std::string & config_prefix);

Poco::Net::HTTPRequestHandlerFactory * createDynamicHandlerFactory(IServer & server, const std::string & config_prefix);

Poco::Net::HTTPRequestHandlerFactory * createPredefinedHandlerFactory(IServer & server, const std::string & config_prefix);

Poco::Net::HTTPRequestHandlerFactory * createReplicasStatusHandlerFactory(IServer & server, const std::string & config_prefix);

Poco::Net::HTTPRequestHandlerFactory * createPrometheusHandlerFactory(IServer & server, AsynchronousMetrics & async_metrics, const std::string & config_prefix);

Poco::Net::HTTPRequestHandlerFactory * createHandlerFactory(IServer & server, AsynchronousMetrics & async_metrics, const std::string & name);

}
