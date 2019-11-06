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
#include <Interpreters/Context.h>
#include <Interpreters/CustomHTTP/HTTPInputStreams.h>
#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>
#include <Interpreters/CustomHTTP/CustomQueryExecutors.h>
#include <Interpreters/CustomHTTP/CustomExecutorMatchers.h>

namespace DB
{
class CustomExecutor;

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

    using QueryExecutorCreator = std::function<CustomQueryExecutorPtr(const Configuration &, const String &)>;
    void registerQueryExecutor(const String & query_executor_name, const QueryExecutorCreator & creator);

    using CustomMatcherCreator = const std::function<CustomExecutorMatcherPtr(const Configuration &, const String &)>;
    void registerCustomMatcher(const String & matcher_name, const CustomMatcherCreator & creator);

    void updateCustomExecutors(const Configuration & config, const Settings & settings, const String & config_prefix);

    std::pair<String, CustomExecutorPtr> getCustomExecutor(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params) const;
private:
    mutable std::shared_mutex rwlock;
    std::vector<std::pair<String, CustomExecutorPtr>> custom_executors;
    std::unordered_map<String, QueryExecutorCreator> query_executor_creators;
    std::unordered_map<String, CustomMatcherCreator> custom_matcher_creators;

    CustomExecutorPtr createCustomExecutor(const Configuration & config, const String & config_prefix);
};

class CustomExecutor
{
public:
    bool isQueryParam(const String & param_name) const;

    bool canBeParseRequestBody(HTTPRequest & request, HTMLForm & params) const;

    bool match(Context & context, HTTPRequest & request, HTMLForm & params) const;

    void executeQuery(
        Context & context, HTTPRequest & request, HTTPResponse & response,
        HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams
    );

    CustomExecutor(const std::vector<CustomExecutorMatcherPtr> & matchers_, const std::vector<CustomQueryExecutorPtr> & query_executors_);

private:
    std::vector<CustomExecutorMatcherPtr> matchers;
    std::vector<CustomQueryExecutorPtr> query_executors;
};

}
