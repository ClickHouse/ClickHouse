#include <Interpreters/CustomHTTP/URLQueryMatcher.h>
#include "URLQueryMatcher.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int UNDEFINED_CUSTOM_EXECUTOR_PARAM;
}

URLQueryMatcher::URLQueryMatcher(const Poco::Util::AbstractConfiguration & configuration, const String & config_key)
{
    const auto & regex_str = configuration.getString(config_key);
    regex_matcher = std::make_unique<re2_st::RE2>(regex_str);

    if (!regex_matcher->ok())
        throw Exception("cannot compile re2: " + regex_str + ", error: " + regex_matcher->error() +
                        ". Look at https://github.com/google/re2/wiki/Syntax for reference.", ErrorCodes::CANNOT_COMPILE_REGEXP);
}

bool checkQueryOneQueryParam(const String & param_name, const std::vector<QueryExecutorPtr> & custom_query_executors)
{
    for (const auto & custom_query_executor : custom_query_executors)
        if (custom_query_executor->isQueryParam(param_name))
            return true;

    return false;
}

bool URLQueryMatcher::checkQueryExecutors(const std::vector<QueryExecutorPtr> & custom_query_executors) const
{
    for (const auto & named_capturing_group : regex_matcher->NamedCapturingGroups())
        if (!checkQueryOneQueryParam(named_capturing_group.first, custom_query_executors))
            throw Exception("The param name '" + named_capturing_group.first + "' is not defined in the QueryExecutor.",
                ErrorCodes::UNDEFINED_CUSTOM_EXECUTOR_PARAM);

    return true;
}

bool URLQueryMatcher::match(Context & context, Poco::Net::HTTPServerRequest &request, HTMLForm &) const
{
    const String request_uri = request.getURI();
    int num_captures = regex_matcher->NumberOfCapturingGroups() + 1;

    re2_st::StringPiece matches[num_captures];
    re2_st::StringPiece input(request_uri.data(), request_uri.size());
    if (regex_matcher->Match(input, 0, request_uri.size(), re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
    {
        const auto & full_match = matches[0];
        const char * url_end = request_uri.data() + request_uri.size();
        const char * not_matched_begin = request_uri.data() + full_match.size();

        if (not_matched_begin != url_end && *not_matched_begin == '/')
            ++not_matched_begin;

        if (not_matched_begin == url_end || *not_matched_begin == '?')
        {
            for (const auto & named_capturing_group : regex_matcher->NamedCapturingGroups())
            {
                const auto & capturing_value = matches[named_capturing_group.second];
                context.setQueryParameter(named_capturing_group.first, String(capturing_value.data(), capturing_value.size()));
            }

            return true;
        }
    }
    return false;
}

}
