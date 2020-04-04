#pragma once

#include "HTTPHandlerFactory.h"

#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Net/HTTPServerRequest.h>

#include <common/find_symbols.h>

#if USE_RE2_ST
#include <re2_st/re2.h>
#else
#define re2_st re2
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

static inline std::string uriPathGetter(const Poco::Net::HTTPServerRequest & request)
{
    const auto & uri = request.getURI();
    const auto & end = find_first_symbols<'?'>(uri.data(), uri.data() + uri.size());

    return std::string(uri.data(), end - uri.data());
}

static inline std::function<std::string(const Poco::Net::HTTPServerRequest &)> headerGetter(const std::string & header_name)
{
    return [header_name](const Poco::Net::HTTPServerRequest & request) { return request.get(header_name, ""); };
}

static inline auto methodsExpressionFilter(const std::string &methods_expression)
{
    Poco::StringTokenizer tokenizer(Poco::toUpper(Poco::trim(methods_expression)), ",");
    return [methods = std::vector<String>(tokenizer.begin(), tokenizer.end())](const Poco::Net::HTTPServerRequest & request)
    {
        return std::count(methods.begin(), methods.end(), request.getMethod());
    };
}

template <typename GetFunction>
static inline auto regularExpressionFilter(const std::string & regular_expression, const GetFunction & get)
{
    auto compiled_regex = std::make_shared<re2_st::RE2>(regular_expression);

    if (!compiled_regex->ok())
        throw Exception("cannot compile re2: " + regular_expression + " for routing_rule, error: " + compiled_regex->error() +
            ". Look at https://github.com/google/re2/wiki/Syntax for reference.", ErrorCodes::CANNOT_COMPILE_REGEXP);

    return std::make_pair(compiled_regex, [get = std::move(get), compiled_regex](const Poco::Net::HTTPServerRequest & request)
    {
        const auto & test_content = get(request);
        int num_captures = compiled_regex->NumberOfCapturingGroups() + 1;

        re2_st::StringPiece matches[num_captures];
        re2_st::StringPiece input(test_content.data(), test_content.size());
        return compiled_regex->Match(input, 0, test_content.size(), re2_st::RE2::Anchor::ANCHOR_BOTH, matches, num_captures);
    });
}

template <typename GetFunction>
static inline std::function<bool(const Poco::Net::HTTPServerRequest &)> expressionFilter(const std::string & expression, const GetFunction & get)
{
    if (startsWith(expression, "regex:"))
        return regularExpressionFilter(expression, get).second;

    return [expression, get = std::move(get)](const Poco::Net::HTTPServerRequest & request) { return get(request) == expression; };
}

template <typename TEndpoint>
static inline Poco::Net::HTTPRequestHandlerFactory * addFiltersFromConfig(
    RoutingRuleHTTPHandlerFactory <TEndpoint> * factory, Poco::Util::AbstractConfiguration & config, const std::string & prefix)
{
    Poco::Util::AbstractConfiguration::Keys filters_type;
    config.keys(prefix, filters_type);

    for (const auto & filter_type : filters_type)
    {
        if (filter_type == "handler")
            continue;   /// Skip handler config
        else if (filter_type == "method")
            factory->addFilter(methodsExpressionFilter(config.getString(prefix + "." + filter_type)));
        else
            factory->addFilter(expressionFilter(config.getString(prefix + "." + filter_type), filter_type == "url"
                ? uriPathGetter : headerGetter(filter_type)));
    }

    return factory;
}

}
