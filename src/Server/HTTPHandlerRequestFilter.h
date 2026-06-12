#pragma once

#include <Server/HTTP/HTTPServerRequest.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <base/find_symbols.h>
#include <Common/re2.h>

#include <Poco/StringTokenizer.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <absl/container/inlined_vector.h>

#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

using CompiledRegexPtr = std::shared_ptr<const re2::RE2>;

/// A parsed filter expression. Depending on the marker prefix of the configured value it is matched as:
///   - an exact string (no marker),
///   - a regular expression ("regex:" marker),
///   - a string prefix ("prefix:" marker), matched on path-segment boundaries for URL filters; see checkExpression().
struct FilterExpression
{
    String value;                      /// The configured value verbatim (including any "regex:"/"prefix:" marker).
    CompiledRegexPtr regex = nullptr;  /// Non-null if the value is a regular expression.
    bool is_prefix = false;            /// True if the value starts with the "prefix:" marker.
};

static inline bool checkRegexExpression(std::string_view match_str, const CompiledRegexPtr & compiled_regex)
{
    int num_captures = compiled_regex->NumberOfCapturingGroups() + 1;

    absl::InlinedVector<std::string_view, 5> matches(num_captures);
    return compiled_regex->Match(
        {match_str.data(), match_str.size()}, 0, match_str.size(), re2::RE2::Anchor::ANCHOR_BOTH, matches.data(), num_captures);
}

/// `is_url` must be true when `match_str` is a URL/path (the `url`/`full_url` filters). In that case the
/// query string is stripped before matching, and a "prefix:" value is matched on a path-segment boundary.
/// For any other filter a "prefix:" value is matched as a plain string prefix (no path-segment handling).
static inline bool checkExpression(std::string_view match_str, const FilterExpression & expression, bool is_url)
{
    if (is_url)
    {
        /// Filters match the path (and host) only, not the query string.
        const auto * end = find_first_symbols<'?'>(match_str.data(), match_str.data() + match_str.size());
        match_str = std::string_view(match_str.data(), end - match_str.data());
    }

    if (expression.regex)
        return checkRegexExpression(match_str, expression.regex);

    if (expression.is_prefix)
    {
        std::string_view prefix = std::string_view{expression.value}.substr(strlen("prefix:"));

        if (!is_url)
            return match_str.starts_with(prefix);

        /// For URL filters match the prefix path itself or any path below it, on a path-segment ('/') boundary.
        /// E.g. prefix "/api/v1" matches "/api/v1", "/api/v1/" and "/api/v1/write", but not "/api/v1beta".
        /// Normalize by stripping a trailing '/', so "prefix:/api/v1/" and "prefix:/api/v1" behave the same.
        if (prefix.ends_with('/'))
            prefix.remove_suffix(1);
        return match_str.starts_with(prefix)
            && (match_str.size() == prefix.size() || match_str[prefix.size()] == '/');
    }

    return match_str == expression.value;
}

static inline auto methodsFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path)
{
    std::vector<String> methods;
    Poco::StringTokenizer tokenizer(config.getString(config_path), ",");

    for (const auto & iterator : tokenizer)
        methods.emplace_back(Poco::toUpper(Poco::trim(iterator)));

    return [methods](const HTTPServerRequest & request) { return std::count(methods.begin(), methods.end(), request.getMethod()); };
}

static inline FilterExpression getExpression(const std::string & expression)
{
    if (startsWith(expression, "regex:"))
    {
        auto compiled_regex = std::make_shared<const re2::RE2>(expression.substr(strlen("regex:")));

        if (!compiled_regex->ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP, "cannot compile re2: {} for http handling rule, error: {}. "
                            "Look at https://github.com/google/re2/wiki/Syntax for reference.",
                            expression, compiled_regex->error());
        return {.value = expression, .regex = compiled_regex};
    }

    /// A "prefix:" value is matched as a string prefix; for URL filters it additionally respects
    /// path-segment boundaries (see checkExpression). The marker is kept in `value` and stripped at
    /// match time.
    if (startsWith(expression, "prefix:"))
        return {.value = expression, .is_prefix = true};

    return {.value = expression};
}

static inline auto urlFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path)
{
    return [expression = getExpression(config.getString(config_path))](const HTTPServerRequest & request)
    {
        return checkExpression(request.getURI(), expression, /* is_url= */ true);
    };
}

static inline auto fullUrlFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path)
{
    return [expression = getExpression(config.getString(config_path))](const HTTPServerRequest & request)
    {
        const auto & server_address = request.serverAddress();
        std::string url(fmt::format("{}://{}{}",
            request.isSecure() ? "https" : "http",
            server_address.toString(),
            request.getURI()
        ));

        return checkExpression(url, expression, /* is_url= */ true);
    };
}

static inline auto emptyQueryStringFilter()
{
    return [](const HTTPServerRequest & request)
    {
        const auto & uri = request.getURI();
        return !uri.contains('?');
    };
}

static inline auto headersFilter(const Poco::Util::AbstractConfiguration & config, const std::string & prefix)
{
    std::unordered_map<String, FilterExpression> headers_expression;
    Poco::Util::AbstractConfiguration::Keys headers_name;
    config.keys(prefix, headers_name);

    for (const auto & header_name : headers_name)
    {
        const auto & expression = getExpression(config.getString(prefix + "." + header_name));
        checkExpression("", expression, /* is_url= */ false);    /// Check expression syntax is correct
        headers_expression.emplace(std::make_pair(header_name, expression));
    }

    return [headers_expression](const HTTPServerRequest & request)
    {
        for (const auto & [header_name, header_expression] : headers_expression)
        {
            const auto header_value = request.get(header_name, "");
            if (!checkExpression(std::string_view(header_value.data(), header_value.size()), header_expression, /* is_url= */ false))
                return false;
        }

        return true;
    };
}

}
