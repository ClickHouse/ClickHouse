#include <Interpreters/CustomHTTP/CustomExecutor.h>
#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>
#include <Interpreters/CustomHTTP/ConstQueryExecutor.h>
#include <Interpreters/CustomHTTP/DynamicQueryExecutor.h>
#include <Interpreters/CustomHTTP/URLQueryMatcher.h>
#include <Interpreters/CustomHTTP/MethodQueryMatcher.h>
#include <Interpreters/CustomHTTP/AlwaysQueryMatcher.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int UNKNOW_QUERY_EXECUTOR;
    extern const int TOO_MANY_INPUT_CUSTOM_EXECUTOR;
}

CustomExecutor::CustomExecutor(
    const std::vector<QueryMatcherPtr> & matchers_,
    const std::vector<QueryExecutorPtr> & query_executors_)
    : matchers(matchers_), query_executors(query_executors_)
{
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

bool CustomExecutor::canBeParseRequestBody() const
{
    for (const auto & query_executor : query_executors)
    {
        if (!query_executor->canBeParseRequestBody())
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

void CustomExecutors::updateCustomExecutors(const Configuration & config, const Settings & /*settings*/, const String & config_prefix)
{
    Configuration::Keys custom_executors_keys;
    config.keys(config_prefix, custom_executors_keys);

    std::vector<std::pair<String, CustomExecutorPtr>> new_custom_executors;

    for (const auto & custom_executor_key : custom_executors_keys)
    {
        if (custom_executor_key.find('.') != String::npos)
            throw Exception("CustomExecutor names with dots are not supported: '" + custom_executor_key + "'", ErrorCodes::SYNTAX_ERROR);

        new_custom_executors.push_back({custom_executor_key, createCustomExecutor(config, config_prefix, custom_executor_key)});
    }

    std::unique_lock<std::shared_mutex> lock(rwlock);
    custom_executors = new_custom_executors;
}

void CustomExecutors::registerCustomMatcher(const String & matcher_name, const CustomExecutors::CustomMatcherCreator & creator)
{
    const auto & matcher_creator_it = custom_matcher_creators.find(matcher_name);
    const auto & query_executor_creator_it = query_executor_creators.find(matcher_name);

    if (matcher_creator_it != custom_matcher_creators.end() && query_executor_creator_it != query_executor_creators.end())
        throw Exception("LOGICAL_ERROR QueryMatcher name must be unique between the QueryExecutor and QueryMatcher.",
            ErrorCodes::LOGICAL_ERROR);

    custom_matcher_creators[matcher_name] = creator;
}

void CustomExecutors::registerQueryExecutor(const String & query_executor_name, const CustomExecutors::QueryExecutorCreator & creator)
{
    const auto & matcher_creator_it = custom_matcher_creators.find(query_executor_name);
    const auto & query_executor_creator_it = query_executor_creators.find(query_executor_name);

    if (matcher_creator_it != custom_matcher_creators.end() && query_executor_creator_it != query_executor_creators.end())
        throw Exception("LOGICAL_ERROR QueryExecutor name must be unique between the QueryExecutor and QueryMatcher.",
            ErrorCodes::LOGICAL_ERROR);

    query_executor_creators[query_executor_name] = creator;
}

String fixMatcherOrExecutorTypeName(const String & matcher_or_executor_type_name)
{
    auto type_name_end_pos = matcher_or_executor_type_name.find('[');
    return type_name_end_pos == String::npos ? matcher_or_executor_type_name : matcher_or_executor_type_name.substr(0, type_name_end_pos);
}

CustomExecutorPtr CustomExecutors::createCustomExecutor(const Configuration & config, const String & config_prefix, const String & name)
{
    Configuration::Keys matchers_key;
    config.keys(config_prefix + "." + name, matchers_key);

    std::vector<QueryMatcherPtr> query_matchers;
    std::vector<QueryExecutorPtr> query_executors;

    for (const auto & matcher_key : matchers_key)
    {
        String matcher_or_query_executor_type = fixMatcherOrExecutorTypeName(matcher_key);

        if (matcher_or_query_executor_type.find('.') != String::npos)
            throw Exception("CustomMatcher or QueryExecutor names with dots are not supported: '" + matcher_or_query_executor_type + "'",
                ErrorCodes::SYNTAX_ERROR);

        const auto & matcher_creator_it = custom_matcher_creators.find(matcher_or_query_executor_type);
        const auto & query_executor_creator_it = query_executor_creators.find(matcher_or_query_executor_type);

        if (matcher_creator_it == custom_matcher_creators.end() && query_executor_creator_it == query_executor_creators.end())
            throw Exception("CustomMatcher or QueryExecutor '" + matcher_or_query_executor_type + "' is not implemented.",
                ErrorCodes::NOT_IMPLEMENTED);

        if (matcher_creator_it != custom_matcher_creators.end())
            query_matchers.push_back(matcher_creator_it->second(config, config_prefix + "." + name + "." + matcher_key));

        if (query_executor_creator_it != query_executor_creators.end())
            query_executors.push_back(query_executor_creator_it->second(config, config_prefix + "." + name + "." + matcher_key));
    }

    checkQueryMatchersAndExecutors(name, query_matchers, query_executors);
    return std::make_shared<CustomExecutor>(query_matchers, query_executors);
}

void CustomExecutors::checkQueryMatchersAndExecutors(
    const String & name, std::vector<QueryMatcherPtr> & matchers, std::vector<QueryExecutorPtr> & query_executors)
{
    if (matchers.empty() || query_executors.empty())
        throw Exception("The CustomExecutor '" + name + "' must contain a Matcher and a QueryExecutor.", ErrorCodes::SYNTAX_ERROR);

    const auto & sum_func = [&](auto & ele) -> bool { return !ele->canBeParseRequestBody(); };
    const auto & need_post_data_count = std::count_if(query_executors.begin(), query_executors.end(), sum_func);

    if (need_post_data_count > 1)
        throw Exception("The CustomExecutor '" + name + "' can only contain one insert query." + toString(need_post_data_count), ErrorCodes::TOO_MANY_INPUT_CUSTOM_EXECUTOR);

    for (const auto & matcher : matchers)
        matcher->checkQueryExecutors(query_executors);
}

std::pair<String, CustomExecutorPtr> CustomExecutors::getCustomExecutor(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params) const
{
    std::shared_lock<std::shared_mutex> lock(rwlock);

    for (const auto & custom_executor : custom_executors)
        if (custom_executor.second->match(context, request, params))
            return custom_executor;

    throw Exception("No CustomExecutor match " + request.getURI(), ErrorCodes::UNKNOW_QUERY_EXECUTOR);
}

CustomExecutors::CustomExecutors(const Configuration & config, const Settings & settings, const String & config_prefix)
{
    registerCustomMatcher("URL", [&](const auto & matcher_config, const auto & prefix)
        { return std::make_shared<URLQueryMatcher>(matcher_config, prefix); });

    registerCustomMatcher("method", [&](const auto & matcher_config, const auto & prefix)
        { return std::make_shared<MethodQueryMatcher>(matcher_config, prefix); });

    registerCustomMatcher("always_matched", [&](const auto & /*matcher_config*/, const auto & /*prefix*/)
        { return std::make_shared<AlwaysQueryMatcher>(); });

    registerQueryExecutor("query", [&](const auto & matcher_config, const auto & prefix)
        { return std::make_shared<QueryExecutorConst>(matcher_config, prefix); });

    registerQueryExecutor("dynamic_query", [&](const auto & matcher_config, const auto & prefix)
        { return std::make_shared<QueryExecutorDynamic>(matcher_config, prefix); });

    updateCustomExecutors(config, settings, config_prefix);
}

}
