#include <re2/re2.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
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
        throw Exception("URL \"" + uri.toString() + "\" is not allowed in configuration file, see <remote_url_allow_hosts>", ErrorCodes::UNACCEPTABLE_URL);
}

void RemoteHostFilter::checkHostAndPort(const std::string & host, const std::string & port) const
{
    if (!checkForDirectEntry(host) &&
        !checkForDirectEntry(host + ":" + port))
        throw Exception("URL \"" + host + ":" + port + "\" is not allowed in configuration file, see <remote_url_allow_hosts>", ErrorCodes::UNACCEPTABLE_URL);
}

void RemoteHostFilter::setValuesFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("remote_url_allow_hosts"))
    {
        std::vector<std::string> keys;
        config.keys("remote_url_allow_hosts", keys);

        std::lock_guard guard(hosts_mutex);
        primary_hosts.clear();
        regexp_hosts.clear();

        for (const auto & key : keys)
        {
            if (startsWith(key, "host_regexp"))
                regexp_hosts.push_back(config.getString("remote_url_allow_hosts." + key));
            else if (startsWith(key, "host"))
                primary_hosts.insert(config.getString("remote_url_allow_hosts." + key));
        }

        is_initialized = true;
    }
    else
    {
        is_initialized = false;
        std::lock_guard guard(hosts_mutex);
        primary_hosts.clear();
        regexp_hosts.clear();
    }
}

bool RemoteHostFilter::checkForDirectEntry(const std::string & str) const
{
    if (!is_initialized)
        /// Allow everything by default.
        return true;

    std::lock_guard guard(hosts_mutex);

    if (primary_hosts.contains(str))
        return true;

    for (const auto & regexp : regexp_hosts)
        if (re2::RE2::FullMatch(str, regexp))
            return true;

    return false;
}
}
