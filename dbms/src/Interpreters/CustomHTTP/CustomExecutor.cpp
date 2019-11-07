#include <Interpreters/CustomHTTP/CustomExecutor.h>
#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>
#include "CustomExecutor.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int DUPLICATE_CUSTOM_EXECUTOR;
    extern const int TOO_MANY_INPUT_CUSTOM_EXECUTOR;
}

bool CustomExecutor::match(Context & context, HTTPRequest & request, HTMLForm & params) const
{
    for (const auto & matcher : matchers)
    {
        if (!matcher->match(context, request, params))
            return false;
    }

    return true;
}

bool CustomExecutor::isQueryParam(const String & param_name) const
{
    for (const auto & query_executor : query_executors)
    {
        if (!query_executor->isQueryParam(param_name))
            return false;
    }

    return true;
}

bool CustomExecutor::canBeParseRequestBody(HTTPRequest & request, HTMLForm & params) const
{
    for (const auto & query_executor : query_executors)
    {
        if (!query_executor->canBeParseRequestBody(request, params))
            return false;
    }

    return true;
}

void CustomExecutor::executeQuery(
    Context & context, HTTPRequest & request, HTTPResponse & response,
    HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams)
{
    for (const auto & query_executor : query_executors)
        query_executor->executeQueryImpl(context, request, response, params, input_streams, output_streams);

    /// Send HTTP headers with code 200 if no exception happened and the data is still not sent to the client.
    output_streams.finalize();
}

CustomExecutor::CustomExecutor(
    const std::vector<CustomExecutorMatcherPtr> & matchers_, const std::vector<CustomQueryExecutorPtr> & query_executors_)
    : matchers(matchers_), query_executors(query_executors_)
{
}

static CustomExecutorPtr createDefaultCustomExecutor()
{
    std::vector<CustomExecutorMatcherPtr> custom_matchers{std::make_shared<AlwaysMatchedCustomExecutorMatcher>()};
    std::vector<CustomQueryExecutorPtr> custom_query_executors{std::make_shared<ExtractQueryParamCustomQueryExecutor>()};

    return std::make_shared<CustomExecutor>(custom_matchers, custom_query_executors);
}

void CustomExecutors::updateCustomExecutors(const Configuration & config, const Settings & /*settings*/, const String & config_prefix)
{
    Configuration::Keys custom_executors_keys;
    config.keys(config_prefix, custom_executors_keys);

    std::vector<std::pair<String, CustomExecutorPtr>> new_custom_executors;

    for (const auto & custom_executor_key : custom_executors_keys)
    {
        if (custom_executor_key == "Default")
            throw Exception("CustomExecutor cannot be 'Default'.", ErrorCodes::SYNTAX_ERROR);
        else if (custom_executor_key.find('.') != String::npos)
            throw Exception("CustomExecutor names with dots are not supported: '" + custom_executor_key + "'", ErrorCodes::SYNTAX_ERROR);

        const auto & exists_executor = [&](auto & ele) { return ele.first == custom_executor_key; };
        if (std::count_if(new_custom_executors.begin(), new_custom_executors.end(), exists_executor))
            throw Exception("CustomExecutor name '" + custom_executor_key + "' already exists in system.",
                            ErrorCodes::DUPLICATE_CUSTOM_EXECUTOR);

        new_custom_executors.push_back(
            std::make_pair(custom_executor_key, createCustomExecutor(config, config_prefix + "." + custom_executor_key)));
    }

    new_custom_executors.push_back(std::make_pair("Default", createDefaultCustomExecutor()));

    std::unique_lock<std::shared_mutex> lock(rwlock);
    custom_executors = new_custom_executors;
}

void CustomExecutors::registerQueryExecutor(const String & query_executor_name, const CustomExecutors::QueryExecutorCreator & creator)
{
    const auto & matcher_creator_it = custom_matcher_creators.find(query_executor_name);
    const auto & query_executor_creator_it = query_executor_creators.find(query_executor_name);

    if (matcher_creator_it != custom_matcher_creators.end() && query_executor_creator_it != query_executor_creators.end())
        throw Exception("LOGICAL_ERROR CustomQueryExecutor name must be unique between the CustomQueryExecutor and CustomExecutorMatcher.",
                        ErrorCodes::LOGICAL_ERROR);

    query_executor_creators[query_executor_name] = creator;
}

void CustomExecutors::registerCustomMatcher(const String & matcher_name, const CustomExecutors::CustomMatcherCreator & creator)
{
    const auto & matcher_creator_it = custom_matcher_creators.find(matcher_name);
    const auto & query_executor_creator_it = query_executor_creators.find(matcher_name);

    if (matcher_creator_it != custom_matcher_creators.end() && query_executor_creator_it != query_executor_creators.end())
        throw Exception("LOGICAL_ERROR CustomExecutorMatcher name must be unique between the CustomQueryExecutor and CustomExecutorMatcher.",
                        ErrorCodes::LOGICAL_ERROR);

    custom_matcher_creators[matcher_name] = creator;
}

CustomExecutorPtr CustomExecutors::createCustomExecutor(const Configuration & config, const String & config_prefix)
{
    Configuration::Keys matchers_or_query_executors_type;
    config.keys(config_prefix, matchers_or_query_executors_type);

    std::vector<CustomQueryExecutorPtr> custom_query_executors;
    std::vector<CustomExecutorMatcherPtr> custom_executor_matchers;

    for (const auto & matcher_or_query_executor_type : matchers_or_query_executors_type)
    {
        if (matcher_or_query_executor_type.find('.') != String::npos)
            throw Exception(
                "CustomMatcher or CustomQueryExecutor names with dots are not supported: '" + matcher_or_query_executor_type + "'",
                ErrorCodes::SYNTAX_ERROR);

        const auto & matcher_creator_it = custom_matcher_creators.find(matcher_or_query_executor_type);
        const auto & query_executor_creator_it = query_executor_creators.find(matcher_or_query_executor_type);

        if (matcher_creator_it == custom_matcher_creators.end() && query_executor_creator_it == query_executor_creators.end())
            throw Exception("CustomMatcher or CustomQueryExecutor '" + matcher_or_query_executor_type + "' is not implemented.",
                            ErrorCodes::NOT_IMPLEMENTED);

        if (matcher_creator_it != custom_matcher_creators.end())
            custom_executor_matchers.push_back(matcher_creator_it->second(config, config_prefix + "." + matcher_or_query_executor_type));

        if (query_executor_creator_it != query_executor_creators.end())
            custom_query_executors.push_back(query_executor_creator_it->second(config, config_prefix + "." + matcher_or_query_executor_type));
    }

    checkCustomMatchersAndQueryExecutors(custom_executor_matchers, custom_query_executors);
    return std::make_shared<CustomExecutor>(custom_executor_matchers, custom_query_executors);
}

void CustomExecutors::checkCustomMatchersAndQueryExecutors(
    std::vector<CustomExecutorMatcherPtr> & matchers, std::vector<CustomQueryExecutorPtr> & query_executors)
{
    const auto & sum_func = [&](auto & ele) { return !ele->canBeParseRequestBody(); };
    const auto & need_post_data_count = std::count_if(query_executors.begin(), query_executors.end(), sum_func);

    if (need_post_data_count > 1)
        throw Exception("The CustomExecutor can only contain one insert query.", ErrorCodes::TOO_MANY_INPUT_CUSTOM_EXECUTOR);

    for (const auto & matcher : matchers)
        matcher->checkQueryExecutors(query_executors);
}

std::pair<String, CustomExecutorPtr> CustomExecutors::getCustomExecutor(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params) const
{
    std::shared_lock<std::shared_mutex> lock(rwlock);

    for (const auto & custom_executor : custom_executors)
        if (custom_executor.second->match(context, request, params))
            return custom_executor;

    throw Exception("LOGICAL_ERROR not found custom executor.", ErrorCodes::LOGICAL_ERROR);
}

CustomExecutors::CustomExecutors(const Configuration & config, const Settings & settings, const String & config_prefix)
{
    registerCustomMatcher("URL", [&](const auto & matcher_config, const auto & prefix)
        { return std::make_shared<HTTPURLCustomExecutorMatcher>(matcher_config, prefix); });

    registerCustomMatcher("method", [&](const auto & matcher_config, const auto & prefix)
        { return std::make_shared<HTTPMethodCustomExecutorMatcher>(matcher_config, prefix); });

    registerQueryExecutor("query", [&](const auto & matcher_config, const auto & prefix)
        { return std::make_shared<ConstQueryCustomQueryExecutor>(matcher_config, prefix); });

    updateCustomExecutors(config, settings, config_prefix);
}

}
