#pragma once

#include <Server/HTTP/HTTPServerRequest.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <functional>
#include <string>


namespace DB
{

/// A request filter checks whether an HTTP request matches a configured rule.
using HTTPRequestFilter = std::function<bool(const HTTPServerRequest &)>;

/// The factories below build one filter from a config entry. Unless noted otherwise, the configured value
/// is matched as an exact string, or — depending on a marker prefix — as a regular expression ("regex:")
/// or a string prefix ("prefix:").

/// Matches the request method against a comma-separated list of methods (e.g. "GET,POST"). Case-insensitive.
HTTPRequestFilter methodsFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path);

/// Matches the request URL path. The query string is ignored when matching.
HTTPRequestFilter urlFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path);

/// Matches the complete request URL `schema://host:port/path`. The query string is ignored when matching.
HTTPRequestFilter fullUrlFilter(const Poco::Util::AbstractConfiguration & config, const std::string & config_path);

/// Matches requests whose URI has no query string.
HTTPRequestFilter emptyQueryStringFilter();

/// Matches request headers against the configured expressions; all listed headers must match.
HTTPRequestFilter headersFilter(const Poco::Util::AbstractConfiguration & config, const std::string & prefix);

}
