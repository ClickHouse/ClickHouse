#include <Common/HTTPHeaderFilter.h>
#include <Common/StringUtils.h>
#include <Common/Exception.h>
#include <Common/re2.h>
#include <algorithm>
#include <cctype>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void HTTPHeaderFilter::checkAndNormalizeHeaders(HTTPHeaderEntries & entries) const
{
    std::lock_guard guard(mutex);

    for (auto & entry : entries)
    {
        if (entry.name.contains('\n') || entry.value.contains('\n'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" has invalid character", entry.name);
        /// Strip whitespace and control characters from header name for validation
        std::string & normalized_name = entry.name;
        normalized_name.erase(
            std::remove_if(
                normalized_name.begin(),
                normalized_name.end(),
                [](char c) { return std::iscntrl(static_cast<unsigned char>(c)) || std::isspace(static_cast<unsigned char>(c)); }),
            normalized_name.end());

        if (forbidden_headers.contains(normalized_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                    "see <http_forbid_headers>", entry.name);

        for (const auto & header_regex : forbidden_headers_regexp)
            if (re2::RE2::FullMatch(normalized_name, header_regex))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                        "see <http_forbid_headers>", entry.name);
    }
}

void HTTPHeaderFilter::setValuesFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard guard(mutex);

    forbidden_headers.clear();
    forbidden_headers_regexp.clear();

    if (config.has("http_forbid_headers"))
    {
        std::vector<std::string> keys;
        config.keys("http_forbid_headers", keys);

        for (const auto & key : keys)
        {
            if (startsWith(key, "header_regexp"))
                forbidden_headers_regexp.push_back(config.getString("http_forbid_headers." + key));
            else if (startsWith(key, "header"))
                forbidden_headers.insert(config.getString("http_forbid_headers." + key));
        }
    }
}

}
