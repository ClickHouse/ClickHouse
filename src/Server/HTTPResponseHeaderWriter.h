#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <base/types.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

using HTTPResponseHeaderSetup = std::optional<std::unordered_map<String, String>>;

HTTPResponseHeaderSetup parseHTTPResponseHeaders(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

std::unordered_map<String, String> parseHTTPResponseHeaders(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const std::string & default_content_type);

std::unordered_map<String, String> parseHTTPResponseHeaders(const std::string & default_content_type);

void applyHTTPResponseHeaders(Poco::Net::HTTPResponse & response, const HTTPResponseHeaderSetup & setup);

void applyHTTPResponseHeaders(Poco::Net::HTTPResponse & response, const std::unordered_map<String, String> & setup);
}
