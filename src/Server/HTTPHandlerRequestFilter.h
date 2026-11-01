#pragma once

#include <Server/HTTP/HTTPServerRequest.h>

#include <Core/Names.h>
#include <base/types.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>


namespace re2 { class RE2; }

namespace DB
{

/// A request filter checks whether an HTTP request matches a configured rule.
using HTTPRequestFilter = std::function<bool(const HTTPServerRequest &)>;

using CompiledRegexPtr = std::shared_ptr<const re2::RE2>;

/// How a filter matches the configured value against the request.
enum class HTTPRequestFilterMatchType
{
    /// Match the whole value: as an exact string, or as a regular expression if it starts with the "regex:" marker.
    Full,

    /// Match the whole value as a regular expression (the value is the regexp itself, without the "regex:" marker).
    Regexp,

    /// Match the value as a base path: the path itself or anything below it on a path-segment ('/') boundary.
    /// E.g. "/api/v1" matches "/api/v1", "/api/v1/" and "/api/v1/write", but not "/api/v1beta".
    Prefix,
};

/// The factories below build one filter from a config entry. Unless noted otherwise, the configured value
/// is matched as an exact string, or as a regular expression if it starts with the "regex:" marker.

/// Matches the request method against a comma-separated list of methods (e.g. "GET,POST"). Case-insensitive.
HTTPRequestFilter methodsFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path);

/// Matches the request URL path according to `match_type`. The query string is ignored when matching.
HTTPRequestFilter urlFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path, HTTPRequestFilterMatchType match_type);

/// Matches the complete request URL `scheme://host:port/path` according to `match_type`. The query string is
/// ignored when matching.
HTTPRequestFilter fullUrlFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path, HTTPRequestFilterMatchType match_type);

/// Matches requests whose URI has no query string.
HTTPRequestFilter emptyQueryStringFilter();

/// Matches request headers against the configured expressions (according to `match_type`); all listed
/// headers must match.
HTTPRequestFilter headersFilter(const Poco::Util::AbstractConfiguration & config, const std::string & prefix, HTTPRequestFilterMatchType match_type);

/// Builds the request filters from the rule sub-tags found under `config_prefix` (such as `url`, `url_prefix`,
/// `url_regexp`, `full_url`, `methods`, `headers`, `headers_regexp`, `empty_query_string`, ...), one filter per
/// sub-tag. The `handler` sub-tag is ignored. A request matches the rule only if every returned filter matches.
/// Throws if an unknown sub-tag is encountered.
std::vector<HTTPRequestFilter> extractHTTPRequestFiltersFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

/// Contains regular expressions configured for a rule's URL and headers with named capturing groups.
struct HTTPHandlerRegexpsWithNamedGroups
{
    /// Non-null if the rule's URL path is matched by a regular expression
    /// with at least one referenced named capturing group.
    /// Null if the rule's URL is a plain URL (an exact-match string).
    CompiledRegexPtr url_regexp;

    /// Maps a header name to the regular expression configured for that header.
    std::unordered_map<String, CompiledRegexPtr> headers_name_with_regexp;

    /// Extracts the regular expressions configured under `config_prefix`,
    /// keeping only those that have a named capturing group present in `group_names`.
    static HTTPHandlerRegexpsWithNamedGroups fromConfig(
        const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const NameSet & group_names);
};

}
