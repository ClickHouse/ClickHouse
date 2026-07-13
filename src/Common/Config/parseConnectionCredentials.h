#pragma once

#include <string>
#include <optional>
#include <base/types.h>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

struct ConnectionsCredentials
{
    /// .hostname or fallback to connection
    std::optional<std::string> hostname;
    std::optional<UInt16> port;
    std::optional<bool> secure;
    std::optional<std::string> user;
    std::optional<std::string> password;
    std::optional<std::string> database;
    std::optional<std::string> history_file;
    std::optional<UInt32> history_max_entries;
    std::optional<bool> accept_invalid_certificate;
    std::optional<std::string> prompt;
};

/// Parse <connections_credentials> section from client config.
///
/// @param config - to search for connection_credentials for
/// @param host - will look for a connection with name equals to host, but will not fail if such does not exist
/// @param connection_name - connection name to look at, fails if there is no such connection defined
ConnectionsCredentials parseConnectionsCredentials(const Poco::Util::AbstractConfiguration & config, const std::string & host, const std::optional<std::string> & connection_name);

}
