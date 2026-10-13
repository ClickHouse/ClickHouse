#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/RemoteHostFilter.h>
#include <Common/StringUtils.h>
#include <Common/Exception.h>
#include <Common/maskURIPassword.h>
#include <Common/parseAddress.h>
#include <Common/re2.h>
#include <IO/WriteHelpers.h>

#include <boost/algorithm/string/replace.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNACCEPTABLE_URL;
}

void RemoteHostFilter::checkURL(const Poco::URI & uri) const
{
    if (!checkForDirectEntry(uri.getHost()) &&
        !checkForDirectEntry(uri.getHost() + ":" + toString(uri.getPort())))
    {
        std::string masked_uri = uri.toString();
        maskURIUserinfo(masked_uri);
        /// An S3 URI can carry its presigned query in the path, which `toString` renders as `%3F`.
        boost::replace_all(masked_uri, "%3F", "?");
        maskPresignedURLParameters(masked_uri);
        throw Exception(ErrorCodes::UNACCEPTABLE_URL, "URL \"{}\" is not allowed in configuration file, "
                                                      "see <remote_url_allow_hosts>", masked_uri);
    }
}

void RemoteHostFilter::checkHostAndPort(const std::string & host, const std::string & port) const
{
    if (!checkForDirectEntry(host) &&
        !checkForDirectEntry(host + ":" + port))
        throw Exception(ErrorCodes::UNACCEPTABLE_URL, "URL \"{}:{}\" is not allowed in configuration file, "
                                                      "see <remote_url_allow_hosts>", host, port);
}

std::string RemoteHostFilter::checkAndGetCanonicalHostAndPort(
    const std::string & host_and_port, UInt16 default_port, const std::string & description) const
{
    for (const char c : host_and_port)
    {
        const bool is_visible_ascii = isPrintableASCII(c) && c != ' ';
        const bool is_url_separator = c == '/' || c == '@' || c == '\\';
        if (!is_visible_ascii || is_url_separator)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected character '{}' in {} '{}': expected host[:port]",
                c, description, host_and_port);
    }

    const auto [host, port] = parseAddress(host_and_port, default_port);
    if (host.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty host in {} '{}'", description, host_and_port);

    checkHostAndPort(host, toString(port));

    return host + ':' + toString(port);
}

void RemoteHostFilter::setValuesFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::unordered_set<std::string> new_primary_hosts;
    std::vector<std::string> new_regexp_hosts;
    std::unordered_set<std::string> new_s3_buckets;
    const bool has_config = config.has("remote_url_allow_hosts");

    if (has_config)
    {
        std::vector<std::string> keys;
        config.keys("remote_url_allow_hosts", keys);

        for (const auto & key : keys)
        {
            if (startsWith(key, "host_regexp"))
                new_regexp_hosts.push_back(config.getString("remote_url_allow_hosts." + key));
            else if (startsWith(key, "host"))
                new_primary_hosts.insert(config.getString("remote_url_allow_hosts." + key));
            else if (startsWith(key, "s3_bucket"))
            {
                const auto value = config.getString("remote_url_allow_hosts." + key);
                const auto slash = value.find('/');
                if (slash == 0 || slash == std::string::npos || slash + 1 == value.size() || value.find('/', slash + 1) != std::string::npos)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "<s3_bucket> \"{}\" in <remote_url_allow_hosts> must have the form host/bucket or host:port/bucket", value);
                new_s3_buckets.insert(value);
            }
        }
    }

    std::lock_guard guard(hosts_mutex);
    primary_hosts = std::move(new_primary_hosts);
    regexp_hosts = std::move(new_regexp_hosts);
    s3_buckets = std::move(new_s3_buckets);
    is_initialized = has_config;
}

bool RemoteHostFilter::isBucketAllowed(const std::string & endpoint_host, UInt16 endpoint_port, const std::string & bucket) const
{
    if (!is_initialized)
        return true;

    std::lock_guard guard(hosts_mutex);
    return s3_buckets.contains(endpoint_host + "/" + bucket)
        || s3_buckets.contains(endpoint_host + ":" + toString(endpoint_port) + "/" + bucket);
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
