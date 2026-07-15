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
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

namespace
{

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

CompiledRegexPtr compileRegex(const std::string & regex)
{
    auto compiled_regex = std::make_shared<const re2::RE2>(regex);

    if (!compiled_regex->ok())
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP, "cannot compile re2: {} for http handling rule, error: {}. "
                        "Look at https://github.com/google/re2/wiki/Syntax for reference.",
                        regex, compiled_regex->error());
    return compiled_regex;
}

/// Whether `compiled_regex` has at least one named capturing group whose name is in `group_names`.
bool hasAnyOfCapturingGroups(const CompiledRegexPtr & compiled_regex, const NameSet & group_names)
{
    const auto & actual_groups = compiled_regex->NamedCapturingGroups();
    return std::any_of(actual_groups.begin(), actual_groups.end(),
        [&](const auto & group) { return group_names.contains(group.first); });
}

FilterExpression getExpression(const std::string & expression, HTTPRequestFilterMatchType match_type, bool ignore_query_string)
{
    if (match_type == HTTPRequestFilterMatchType::Prefix)
    {
        /// Match the value as a base path on a path-segment boundary (see checkExpression). A trailing '/'
        /// is stripped here, so "/api/v1/" and "/api/v1" behave the same.
        std::string_view value = expression;
        if (value.ends_with('/'))
            value.remove_suffix(1);
        return {.value = String{value}, .match_prefix = true, .ignore_query_string = ignore_query_string};
    }

    /// `Regexp` treats the whole value as a regular expression, while `Full` does so only when the value
    /// starts with the "regex:" marker.
    if (match_type == HTTPRequestFilterMatchType::Regexp)
        return {.value = expression, .regex = compileRegex(expression), .ignore_query_string = ignore_query_string};

    if (startsWith(expression, "regex:"))
    {
        auto regex = expression.substr(strlen("regex:"));
        return {.value = regex, .regex = compileRegex(regex), .ignore_query_string = ignore_query_string};
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

HTTPRequestFilter urlFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path, HTTPRequestFilterMatchType match_type)
{
    return [expression = getExpression(config.getString(config_path), match_type, /* ignore_query_string= */ true)](const HTTPServerRequest & request)
    {
        return checkExpression(request.getURI(), expression);
    };
}

HTTPRequestFilter fullUrlFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path, HTTPRequestFilterMatchType match_type)
{
    return [expression = getExpression(config.getString(config_path), match_type, /* ignore_query_string= */ true)](const HTTPServerRequest & request)
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

HTTPRequestFilter headersFilter(const Poco::Util::AbstractConfiguration & config, const std::string & prefix, HTTPRequestFilterMatchType match_type)
{
    std::unordered_map<String, FilterExpression> headers_expression;
    Poco::Util::AbstractConfiguration::Keys headers_name;
    config.keys(prefix, headers_name);

    for (const auto & header_name : headers_name)
    {
        const auto & expression = getExpression(
            config.getString(prefix + "." + header_name), match_type, /* ignore_query_string= */ false);
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

std::vector<HTTPRequestFilter> extractHTTPRequestFiltersFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    std::vector<HTTPRequestFilter> filters;

    Poco::Util::AbstractConfiguration::Keys filter_types;
    config.keys(config_prefix, filter_types);

    for (const auto & filter_type : filter_types)
    {
        if (filter_type == "handler")
            continue;
        /// URL path (the query string is ignored)
        if (filter_type == "url")
            filters.push_back(urlFilter(config, config_prefix + ".url", HTTPRequestFilterMatchType::Full));
        /// URL path matched as a base path
        else if (filter_type == "url_prefix")
            filters.push_back(urlFilter(config, config_prefix + ".url_prefix", HTTPRequestFilterMatchType::Prefix));
        /// URL path matched as a regular expression
        else if (filter_type == "url_regexp")
            filters.push_back(urlFilter(config, config_prefix + ".url_regexp", HTTPRequestFilterMatchType::Regexp));
        /// Complete URL (scheme://host:port/path); the query string is ignored
        else if (filter_type == "full_url")
            filters.push_back(fullUrlFilter(config, config_prefix + ".full_url", HTTPRequestFilterMatchType::Full));
        /// Complete URL matched as a base path
        else if (filter_type == "full_url_prefix")
            filters.push_back(fullUrlFilter(config, config_prefix + ".full_url_prefix", HTTPRequestFilterMatchType::Prefix));
        /// Complete URL matched as a regular expression
        else if (filter_type == "full_url_regexp")
            filters.push_back(fullUrlFilter(config, config_prefix + ".full_url_regexp", HTTPRequestFilterMatchType::Regexp));
        else if (filter_type == "empty_query_string")
            filters.push_back(emptyQueryStringFilter());
        else if (filter_type == "headers")
            filters.push_back(headersFilter(config, config_prefix + ".headers", HTTPRequestFilterMatchType::Full));
        /// Each header value matched as a regular expression
        else if (filter_type == "headers_regexp")
            filters.push_back(headersFilter(config, config_prefix + ".headers_regexp", HTTPRequestFilterMatchType::Regexp));
        else if (filter_type == "methods")
            filters.push_back(methodsFilter(config, config_prefix + ".methods"));
        else
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: {}.{}", config_prefix, filter_type);
    }

    return filters;
}

HTTPHandlerRegexpsWithNamedGroups HTTPHandlerRegexpsWithNamedGroups::fromConfig(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const NameSet & group_names)
{
    HTTPHandlerRegexpsWithNamedGroups result;

    /// The URL is a regular expression when configured via the dedicated `url_regexp` tag,
    /// or via `url` carrying the obsolete "regex:" marker.
    CompiledRegexPtr url_regexp;
    if (config.has(config_prefix + ".url_regexp"))
    {
        url_regexp = compileRegex(config.getString(config_prefix + ".url_regexp"));
    }
    else if (config.has(config_prefix + ".url"))
    {
        const auto url = config.getString(config_prefix + ".url");
        if (startsWith(url, "regex:"))
            url_regexp = compileRegex(url.substr(strlen("regex:")));
    }

    if (url_regexp && hasAnyOfCapturingGroups(url_regexp, group_names))
        result.url_regexp = url_regexp;

    /// Headers configured as regular expressions: `headers` entries carrying the "regex:" marker, and
    /// `headers_regexp` entries (whose value is the regular expression itself).
    const auto collect_header_regexes = [&](const std::string & headers_key, bool has_regex_marker)
    {
        Poco::Util::AbstractConfiguration::Keys header_names;
        config.keys(config_prefix + "." + headers_key, header_names);

        for (const auto & header_name : header_names)
        {
            auto expression = config.getString(config_prefix + "." + headers_key + "." + header_name);

            if (has_regex_marker)
            {
                if (!startsWith(expression, "regex:"))
                    continue;
                expression = expression.substr(strlen("regex:"));
            }

            auto regex = compileRegex(expression);
            if (hasAnyOfCapturingGroups(regex, group_names))
                result.headers_name_with_regexp.emplace(header_name, regex);
        }
    };

    collect_header_regexes("headers", /* has_regex_marker= */ true);
    collect_header_regexes("headers_regexp", /* has_regex_marker= */ false);

    return result;
}

}
