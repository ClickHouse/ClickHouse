#pragma once

#include <shared_mutex>

#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <Core/Settings.h>
#include <Common/HTMLForm.h>
#include <Common/Exception.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

class Context;
class CustomExecutor;
struct HTTPInputStreams;
struct HTTPOutputStreams;

using HTTPRequest = Poco::Net::HTTPServerRequest;
using HTTPResponse = Poco::Net::HTTPServerResponse;
using CustomExecutorPtr = std::shared_ptr<CustomExecutor>;

class CustomExecutors
{
public:
    using Configuration = Poco::Util::AbstractConfiguration;
    CustomExecutors(const Configuration & config, const Settings & settings, const String & config_prefix = "CustomHTTP");

    CustomExecutors(const CustomExecutors &) = delete;
    CustomExecutors & operator=(const CustomExecutors &) = delete;

    void updateCustomExecutors(const Configuration & config, const Settings & settings, const String & config_prefix);

    std::pair<String, CustomExecutorPtr> getCustomExecutor(Poco::Net::HTTPServerRequest & request, HTMLForm & params) const;
private:
    mutable std::shared_mutex rwlock;
    std::unordered_map<String, CustomExecutorPtr> custom_executors;

    CustomExecutorPtr createCustomExecutor(const Configuration & config, const Settings & settings, const String & config_prefix);
};

class CustomExecutor
{
public:
    bool isQueryParam(const String & param_name) const;

    bool match(HTTPRequest & request, HTMLForm & params) const;

    bool canBeParseRequestBody(HTTPRequest & request, HTMLForm & params) const;

    void executeQuery(
        Context & context, HTTPRequest & request, HTTPResponse & response,
        HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams
    );

public:
    class CustomMatcher
    {
    public:
        virtual ~CustomMatcher() = default;

        virtual bool match(HTTPRequest & request, HTMLForm & params) const = 0;
    };

    class CustomQueryExecutor
    {
    public:
        virtual ~CustomQueryExecutor() = default;

        virtual bool isQueryParam(const String &) const = 0;
        virtual bool canBeParseRequestBody(HTTPRequest &, HTMLForm &) const = 0;

        virtual void executeQueryImpl(
            Context & context, HTTPRequest & request, HTTPResponse & response,
            HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams) const = 0;
    };

public:
    using CustomMatcherPtr = std::shared_ptr<CustomMatcher>;
    using CustomQueryExecutorPtr = std::shared_ptr<CustomQueryExecutor>;

    CustomExecutor(const std::vector<CustomMatcherPtr> & matchers_, const std::vector<CustomQueryExecutorPtr> & query_executors_);

private:
    std::vector<CustomMatcherPtr> matchers;
    std::vector<CustomQueryExecutorPtr> query_executors;
};

}
