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
    std::string jwt;
    Protocol::Secure security = Protocol::Secure::Disable;
    Protocol::Compression compression = Protocol::Compression::Enable;
    ConnectionTimeouts timeouts;

    using Database = StrongTypedef<String, struct DatabaseTag>;
    using Host = StrongTypedef<String, struct HostTag>;

    // // We don't take database from config, as it can be changed after query execution
    // ConnectionParameters(const Poco::Util::AbstractConfiguration & config, const std::string & database, std::string host);
    // ConnectionParameters(
    //     const Poco::Util::AbstractConfiguration & config, const std::string & database, std::string host, std::optional<UInt16> port
    // );

    ConnectionParameters() = default;
    ConnectionParameters(const Poco::Util::AbstractConfiguration & config_, const Host & host_, const Database & database_);
    ConnectionParameters(const Poco::Util::AbstractConfiguration & config_, const Host & host_, const Database & database_, std::optional<UInt16> port_);

    static UInt16 getPortFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & connection_host);

    /// Ask to enter the user's password if password option contains this value.
    /// "\n" is used because there is hardly a chance that a user would use '\n' as password.
    static constexpr std::string_view ASK_PASSWORD = "\n";

    static ConnectionParameters createForEmbedded(const String & user, const String & database);
};

}
