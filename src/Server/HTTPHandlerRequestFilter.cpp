#include <Server/HTTPHandlerRequestFilter.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/re2.h>
#include <base/find_symbols.h>

#include <Poco/String.h>
#include <Poco/StringTokenizer.h>

#include <absl/container/inlined_vector.h>

#include <fmt/format.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

namespace
{

using CompiledRegexPtr = std::shared_ptr<const re2::RE2>;

/// A parsed filter expression.
struct FilterExpression
{
    /// The value to match against (without marker "regex:" if any).
    String value;

    /// Non-null if the value is a regular expression.
    CompiledRegexPtr regex = nullptr;

    /// Whether to match `value` as a base path: the path itself or anything below it on a path-segment boundary.
    bool match_prefix = false;

    /// Whether to ignore the query string before matching (for `url`/`full_url` filters).
    bool ignore_query_string = false;
};

bool checkRegexExpression(std::string_view match_str, const CompiledRegexPtr & compiled_regex)
{
    int num_captures = compiled_regex->NumberOfCapturingGroups() + 1;

    absl::InlinedVector<std::string_view, 5> matches(num_captures);
    return compiled_regex->Match(
        {match_str.data(), match_str.size()}, 0, match_str.size(), re2::RE2::Anchor::ANCHOR_BOTH, matches.data(), num_captures);
}

bool checkExpression(std::string_view match_str, const FilterExpression & expression)
{
    if (expression.ignore_query_string)
    {
        const auto * query_string = find_first_symbols<'?'>(match_str.data(), match_str.data() + match_str.size());
        match_str = match_str.substr(0, query_string - match_str.data());
    }

    if (expression.regex)
        return checkRegexExpression(match_str, expression.regex);

    if (expression.match_prefix)
    {
        /// Match the base path itself or any path below it, on a path-segment ('/') boundary. E.g. the
        /// base "/api/v1" matches "/api/v1", "/api/v1/" and "/api/v1/write", but not "/api/v1beta".
        const std::string_view prefix = expression.value;
        return match_str.starts_with(prefix)
            && (match_str.size() == prefix.size() || match_str[prefix.size()] == '/');
    }

    return match_str == expression.value;
}

FilterExpression getExpression(const std::string & expression, HTTPRequestFilterMatchType filter_type, bool ignore_query_string)
{
    if (filter_type == HTTPRequestFilterMatchType::Prefix)
    {
        /// Match the value as a base path on a path-segment boundary (see checkExpression). A trailing '/'
        /// is stripped here, so "/api/v1/" and "/api/v1" behave the same.
        std::string_view value = expression;
        if (value.ends_with('/'))
            value.remove_suffix(1);
        return {.value = String{value}, .match_prefix = true, .ignore_query_string = ignore_query_string};
    }

    if (startsWith(expression, "regex:"))
    {
        auto compiled_regex = std::make_shared<const re2::RE2>(expression.substr(strlen("regex:")));

        if (!compiled_regex->ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP, "cannot compile re2: {} for http handling rule, error: {}. "
                            "Look at https://github.com/google/re2/wiki/Syntax for reference.",
                            expression, compiled_regex->error());
        return {.value = expression, .regex = compiled_regex, .ignore_query_string = ignore_query_string};
    }

    return {.value = expression, .ignore_query_string = ignore_query_string};
}

}

HTTPRequestFilter methodsFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path)
{
    std::vector<String> methods;
    Poco::StringTokenizer tokenizer(config.getString(config_path), ",");

    for (const auto & iterator : tokenizer)
        methods.emplace_back(Poco::toUpper(Poco::trim(iterator)));

    return [methods](const HTTPServerRequest & request) { return std::count(methods.begin(), methods.end(), request.getMethod()); };
}

HTTPRequestFilter urlFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path, HTTPRequestFilterMatchType filter_type)
{
    return [expression = getExpression(config.getString(config_path), filter_type, /* ignore_query_string= */ true)](const HTTPServerRequest & request)
    {
        return checkExpression(request.getURI(), expression);
    };
}

HTTPRequestFilter fullUrlFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path, HTTPRequestFilterMatchType filter_type)
{
    return [expression = getExpression(config.getString(config_path), filter_type, /* ignore_query_string= */ true)](const HTTPServerRequest & request)
    {
        const auto & server_address = request.serverAddress();
        std::string url = fmt::format("{}://{}{}",
            request.isSecure() ? "https" : "http",
            server_address.toString(),
            request.getURI());

        return checkExpression(url, expression);
    };
}

HTTPRequestFilter emptyQueryStringFilter()
{
    return [](const HTTPServerRequest & request)
    {
        const auto & uri = request.getURI();
        return !uri.contains('?');
    };
}

HTTPRequestFilter headersFilter(const Poco::Util::AbstractConfiguration & config, const std::string & prefix)
{
    std::unordered_map<String, FilterExpression> headers_expression;
    Poco::Util::AbstractConfiguration::Keys headers_name;
    config.keys(prefix, headers_name);

    for (const auto & header_name : headers_name)
    {
        const auto & expression = getExpression(
            config.getString(prefix + "." + header_name), HTTPRequestFilterMatchType::Full, /* ignore_query_string= */ false);
        checkExpression("", expression);    /// Check expression syntax is correct
        headers_expression.emplace(std::make_pair(header_name, expression));
    }

    return [headers_expression](const HTTPServerRequest & request)
    {
        for (const auto & [header_name, header_expression] : headers_expression)
        {
            const auto header_value = request.get(header_name, "");
            if (!checkExpression(std::string_view(header_value.data(), header_value.size()), header_expression))
                return false;
        }

        return true;
    };
}

}
