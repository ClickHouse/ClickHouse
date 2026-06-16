#pragma once

#include <Server/HTTP/HTTPServerRequest.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <functional>
#include <string>
#include <vector>


namespace DB
{

/// A request filter checks whether an HTTP request matches a configured rule.
using HTTPRequestFilter = std::function<bool(const HTTPServerRequest &)>;

/// How a filter matches the configured value against the request.
enum class HTTPRequestFilterMatchType
{
    /// Match the whole value: as an exact string, or as a regular expression if it starts with the "regex:" marker.
    Full,

    /// Match the whole value as a regular expression (the value is the regex itself, without the "regex:" marker).
    Regex,

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

/// Builds the request filters from the rule sub-tags found under `config_prefix` (such as `url`, `url_regex`,
/// `url_prefix`, `full_url`, `methods`, `headers`, `empty_query_string`, ...), one filter per sub-tag. The
/// `handler` sub-tag is ignored. A request matches the rule only if every returned filter matches. Throws if
/// an unknown sub-tag is encountered.
std::vector<HTTPRequestFilter> buildFiltersFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

}
