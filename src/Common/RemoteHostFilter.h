#pragma once

#include <atomic>
#include <string>
#include <vector>
#include <mutex>
#include <unordered_set>
#include <base/defines.h>
#include <base/types.h>


namespace Poco { class URI; }
namespace Poco { namespace Util { class AbstractConfiguration; } }

namespace DB
{
class RemoteHostFilter
{
/**
 * This class checks if URL is allowed.
 * If primary_hosts and regexp_hosts are empty all urls are allowed.
 */
public:
    void checkURL(const Poco::URI & uri) const; /// If URL not allowed in config.xml throw UNACCEPTABLE_URL Exception

    void setValuesFromConfig(const Poco::Util::AbstractConfiguration & config);

    void checkHostAndPort(const std::string & host, const std::string & port) const; /// Does the same as checkURL, but for host and port.

    /// Parses a `host[:port]` string, checks it as checkHostAndPort does, and returns it rebuilt as
    /// `host:port` with an explicit port - the string to hand to a client library in place of the
    /// original value, so the library dials exactly what the filter saw. A string whose re-parse by
    /// a client library could disagree with this parse is rejected with `BAD_ARGUMENTS`: only
    /// visible ASCII is accepted, and `/`, `@`, `\` and an empty host are rejected.
    /// `description` names the checked value in error messages, e.g. "Kafka broker".
    std::string checkAndGetCanonicalHostAndPort(const std::string & host_and_port, UInt16 default_port, const std::string & description) const;

private:
    std::atomic_bool is_initialized = false;

    mutable std::mutex hosts_mutex;
    std::unordered_set<std::string> primary_hosts TSA_GUARDED_BY(hosts_mutex);  /// Allowed primary (<host>) URL from config.xml
    std::vector<std::string> regexp_hosts TSA_GUARDED_BY(hosts_mutex);          /// Allowed regexp (<hots_regexp>) URL from config.xml

    /// Checks if the primary_hosts and regexp_hosts contain str. If primary_hosts and regexp_hosts are empty return true.
    bool checkForDirectEntry(const std::string & str) const;
};
}
