#include <re2/re2.h>
#include <Common/RemoteHostFilter.h>
#include <Poco/URI.h>
#include <Formats/FormatFactory.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNACCEPTABLE_URL;
}

void RemoteHostFilter::checkURL(const Poco::URI & uri)
{
    if (!checkString(uri.getHost()) &&
        !checkString(uri.getHost() + ":" + toString(uri.getPort())))
        throw Exception("URL \"" + uri.toString() + "\" is not allowed in config.xml", ErrorCodes::UNACCEPTABLE_URL);
}

void RemoteHostFilter::checkHostAndPort(const std::string & host, const std::string & port)
{
    if (!checkString(host) &&
        !checkString(host + ":" + port))
        throw Exception("URL \"" + host + ":" + port + "\" is not allowed in config.xml", ErrorCodes::UNACCEPTABLE_URL);
}

void RemoteHostFilter::setValuesFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("remote_url_allow_hosts"))
    {
        std::vector<std::string> keys;
        config.keys("remote_url_allow_hosts", keys);
        for (auto key : keys)
        {
            if (startsWith(key, "host_regexp"))
                regexp_hosts.push_back(config.getString("remote_url_allow_hosts." + key));
            else if (startsWith(key, "host"))
                primary_hosts.insert(config.getString("remote_url_allow_hosts." + key));
        }
    }
}

bool RemoteHostFilter::checkString(const std::string &host)
{
    if (!primary_hosts.empty() || !regexp_hosts.empty())
    {
        if (primary_hosts.find(host) == primary_hosts.end())
        {
            for (size_t i = 0; i < regexp_hosts.size(); ++i)
                if (re2::RE2::FullMatch(host, regexp_hosts[i]))
                    return true;
            return false;
        }
        return true;
    }
    return true;
}
}
