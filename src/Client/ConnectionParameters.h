#pragma once

#include <Common/SSHWrapper.h>
#include <Core/Protocol.h>
#include <IO/ConnectionTimeouts.h>

#include <string>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{
struct ConnectionParameters
{
    std::string host;
    UInt16 port{};
    std::string default_database;
    std::string user;
    std::string password;
    std::string quota_key;
    SSHKey ssh_private_key;
    Protocol::Secure security = Protocol::Secure::Disable;
    Protocol::Compression compression = Protocol::Compression::Enable;
    ConnectionTimeouts timeouts;

    ConnectionParameters() = default;
    ConnectionParameters(const Poco::Util::AbstractConfiguration & config, std::string host);
    ConnectionParameters(const Poco::Util::AbstractConfiguration & config, std::string host, std::optional<UInt16> port);

    static UInt16 getPortFromConfig(const Poco::Util::AbstractConfiguration & config, std::string connection_host);

    /// Ask to enter the user's password if password option contains this value.
    /// "\n" is used because there is hardly a chance that a user would use '\n' as password.
    static constexpr std::string_view ASK_PASSWORD = "\n";
};

}
