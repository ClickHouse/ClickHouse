#include "HTTPResponseHeaderWriter.h"
#include <unordered_map>
#include <utility>
#include <Poco/Net/HTTPMessage.h>

namespace DB
{

std::unordered_map<String, String>
baseParseHTTPResponseHeaders(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    std::unordered_map<String, String> http_response_headers_override;
    String http_response_headers_key = config_prefix + ".handler.http_response_headers";
    String http_response_headers_key_prefix = http_response_headers_key + ".";
    if (config.has(http_response_headers_key))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(http_response_headers_key, keys);
        for (const auto & key : keys)
        {
            http_response_headers_override[key] = config.getString(http_response_headers_key_prefix + key);
        }
    }
    if (config.has(config_prefix + ".handler.content_type"))
        http_response_headers_override[Poco::Net::HTTPMessage::CONTENT_TYPE] = config.getString(config_prefix + ".handler.content_type");

    return http_response_headers_override;
}

HTTPResponseHeaderSetup parseHTTPResponseHeaders(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    std::unordered_map<String, String> http_response_headers_override = baseParseHTTPResponseHeaders(config, config_prefix);

    if (http_response_headers_override.empty())
        return {};

    return std::move(http_response_headers_override);
}

std::unordered_map<String, String> parseHTTPResponseHeaders(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const std::string & default_content_type)
{
    std::unordered_map<String, String> http_response_headers_override = baseParseHTTPResponseHeaders(config, config_prefix);

    if (!http_response_headers_override.contains(Poco::Net::HTTPMessage::CONTENT_TYPE))
        http_response_headers_override[Poco::Net::HTTPMessage::CONTENT_TYPE] = default_content_type;

    return http_response_headers_override;
}

std::unordered_map<String, String> parseHTTPResponseHeaders(const std::string & default_content_type)
{
    return {{{Poco::Net::HTTPMessage::CONTENT_TYPE, default_content_type}}};
}

void applyHTTPResponseHeaders(Poco::Net::HTTPResponse & response, const HTTPResponseHeaderSetup & setup)
{
    if (setup)
        for (const auto & [header_name, header_value] : *setup)
            response.set(header_name, header_value);
}

void applyHTTPResponseHeaders(Poco::Net::HTTPResponse & response, const std::unordered_map<String, String> & setup)
{
    for (const auto & [header_name, header_value] : setup)
        response.set(header_name, header_value);
}

}
