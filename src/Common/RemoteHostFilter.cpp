#include <re2/re2.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Formats/FormatFactory.h>
#include <Common/RemoteHostFilter.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNACCEPTABLE_URL;
}

void RemoteHostFilter::checkURL(const Poco::URI & uri) const
{
    if (!checkForDirectEntry(uri.getHost()) &&
        !checkForDirectEntry(uri.getHost() + ":" + toString(uri.getPort())))
        throw Exception("URL \"" + uri.toString() + "\" is not allowed in config.xml", ErrorCodes::UNACCEPTABLE_URL);
}

void RemoteHostFilter::checkHostAndPort(const std::string & host, const std::string & port) const
{
    if (!checkForDirectEntry(host) &&
        !checkForDirectEntry(host + ":" + port))
        throw Exception("URL \"" + host + ":" + port + "\" is not allowed in config.xml", ErrorCodes::UNACCEPTABLE_URL);
}

void RemoteHostFilter::setValuesFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("remote_url_allow_hosts"))
    {
        std::vector<std::string> keys;
        config.keys("remote_url_allow_hosts", keys);
        for (const auto & key : keys)
        {
            if (startsWith(key, "host_regexp"))
                regexp_hosts.push_back(config.getString("remote_url_allow_hosts." + key));
            else if (startsWith(key, "host"))
                primary_hosts.insert(config.getString("remote_url_allow_hosts." + key));
        }
        is_allow_by_default = false;
    }
}

bool RemoteHostFilter::checkForDirectEntry(const std::string & str) const
{
    if (!primary_hosts.empty() || !regexp_hosts.empty())
    {
        if (primary_hosts.find(str) == primary_hosts.end())
        {
            for (const auto & regexp : regexp_hosts)
                if (re2::RE2::FullMatch(str, regexp))
                    return true;
            return false;
        }
        return true;
    }
    return is_allow_by_default;
}
}
