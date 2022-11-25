#pragma once

#include <string>
#include <Core/Protocol.h>
#include <IO/ConnectionTimeouts.h>

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
    Protocol::Secure security = Protocol::Secure::Disable;
    Protocol::Compression compression = Protocol::Compression::Enable;
    ConnectionTimeouts timeouts;

    ConnectionParameters() = default;
    ConnectionParameters(const Poco::Util::AbstractConfiguration & config);
    ConnectionParameters(const Poco::Util::AbstractConfiguration & config, std::string host, std::optional<UInt16> port);

    static UInt16 getPortFromConfig(const Poco::Util::AbstractConfiguration & config);
};

}
