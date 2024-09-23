#include <Common/HTTPHeaderFilter.h>
#include <Common/StringUtils.h>
#include <Common/Exception.h>
#include <Common/re2.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void HTTPHeaderFilter::checkHeaders(const HTTPHeaderEntries & entries) const
{
    std::lock_guard guard(mutex);

    for (const auto & entry : entries)
    {
        if (entry.name.contains('\n') || entry.value.contains('\n'))
           throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" has invalid character", entry.name);

        if (forbidden_headers.contains(entry.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                    "see <http_forbid_headers>", entry.name);

        for (const auto & header_regex : forbidden_headers_regexp)
            if (re2::RE2::FullMatch(entry.name, header_regex))
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
