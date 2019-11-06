#include <Interpreters/CustomHTTP/CustomExecutor.h>
#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>
#include <Interpreters/CustomHTTP/CustomExecutorDefault.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool CustomExecutor::match(HTTPRequest & request, HTMLForm & params) const
{
    for (const auto & matcher : matchers)
    {
        if (!matcher->match(request, params))
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
    const std::vector<CustomMatcherPtr> & matchers_, const std::vector<CustomQueryExecutorPtr> & query_executors_)
    : matchers(matchers_), query_executors(query_executors_)
{
}

CustomExecutors::CustomExecutors(const Configuration & config, const Settings & settings, const String & config_prefix)
{
    updateCustomExecutors(config, settings, config_prefix);
}

void CustomExecutors::updateCustomExecutors(const Configuration & config, const Settings & settings, const String & config_prefix)
{
    Configuration::Keys custom_executors_keys;
    config.keys(config_prefix, custom_executors_keys);

    std::unordered_map<String, CustomExecutorPtr> new_custom_executors;

    for (const auto & custom_executor_key : custom_executors_keys)
    {
        if (custom_executor_key == "Default")
            throw Exception("CustomExecutor cannot be 'Default'.", ErrorCodes::SYNTAX_ERROR);
        else if (custom_executor_key.find('.') != String::npos)
            throw Exception("CustomExecutor names with dots are not supported: '" + custom_executor_key + "'", ErrorCodes::SYNTAX_ERROR);

        new_custom_executors[custom_executor_key] = createCustomExecutor(config, settings, config_prefix + "." + custom_executor_key);
    }

    new_custom_executors["Default"] = CustomExecutorDefault::createDefaultCustomExecutor();

    std::unique_lock<std::shared_mutex> lock(rwlock);
    custom_executors = new_custom_executors;
}

CustomExecutorPtr CustomExecutors::createCustomExecutor(const CustomExecutors::Configuration & config, const Settings & /*settings*/, const String & config_prefix)
{
    Configuration::Keys matchers_or_query_executors_type;
    config.keys(config_prefix, matchers_or_query_executors_type);

    for (const auto & matcher_or_query_executor_type : matchers_or_query_executors_type)
    {
        if (matcher_or_query_executor_type.find('.') != String::npos)
            throw Exception(
                "CustomMatcher or CustomQueryExecutor names with dots are not supported: '" + matcher_or_query_executor_type + "'",
                ErrorCodes::SYNTAX_ERROR);

//        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
//        new_custom_executors[matcher_or_query_executor_type] = createCustomExecutor(config, settings, config_prefix + "." + matcher_or_query_executor_type);
    }
    return DB::CustomExecutorPtr();
}

std::pair<String, CustomExecutorPtr> CustomExecutors::getCustomExecutor(Poco::Net::HTTPServerRequest & request, HTMLForm & params) const
{
    std::shared_lock<std::shared_mutex> lock(rwlock);

    for (const auto & custom_executor : custom_executors)
        if (custom_executor.second->match(request, params))
            return custom_executor;

    throw Exception("LOGICAL_ERROR not found custom executor.", ErrorCodes::LOGICAL_ERROR);
}

}
