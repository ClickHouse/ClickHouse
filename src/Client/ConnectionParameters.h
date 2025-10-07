#pragma once

#include <base/strong_typedef.h>
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

class JWTProvider;

struct ConnectionParameters
{
    String host;
    UInt16 port{};
    std::string default_database;
    std::string user;
    std::string password;
    std::string proto_send_chunked = "notchunked";
    std::string proto_recv_chunked = "notchunked";
    std::string quota_key;
    SSHKey ssh_private_key;
    std::string jwt;
#if USE_JWT_CPP && USE_SSL
    std::shared_ptr<JWTProvider> jwt_provider;
#endif
    Protocol::Secure security = Protocol::Secure::Disable;
    std::string bind_host;
    Protocol::Compression compression = Protocol::Compression::Enable;
    ConnectionTimeouts timeouts;

    using Database = StrongTypedef<String, struct DatabaseTag>;
    using Host = StrongTypedef<String, struct HostTag>;

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
