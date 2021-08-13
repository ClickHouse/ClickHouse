#pragma once

#include <Common/Exception.h>
#include <Server/InterfaceConfig.h>

#include <Poco/Logger.h>
#include <Poco/Timespan.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace Poco::Net
{

class SocketAddress;
class ServerSocket;

}

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace Util
{

InterfaceConfigs parseInterfaces(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings,
    const ProxyConfigs & proxies
);

Poco::Net::SocketAddress makeSocketAddress(
    const std::string & host,
    UInt16 port,
    Poco::Logger * logger
);

Poco::Net::SocketAddress socketBindListen(
    Poco::Net::ServerSocket & socket,
    const std::string & host,
    UInt16 port,
    [[maybe_unused]] bool secure,
    bool reuse_port,
    UInt32 backlog,
    Poco::Logger * logger
);

template <typename Duration>
Poco::Timespan toTimespan(const Duration & duration)
{
    return Poco::Timespan(std::chrono::duration_cast<std::chrono::microseconds>(duration).count());
}

template <typename InterfaceConfig>
std::unique_ptr<InterfaceConfig> tryParseLegacyInterfaceHelper(
    const bool secure_,
    const std::string & port_prefix,
    const std::string & name,
    const DB::LegacyGlobalConfigOverrides & global_overrides,
    const Poco::Util::AbstractConfiguration & config,
    const DB::Settings & settings
)
{
    if (!config.has(port_prefix))
        return {};

    const auto raw_port = config.getUInt(port_prefix);

    if (raw_port > std::numeric_limits<UInt16>::max())
        throw DB::Exception{"Value for port is out of range", DB::ErrorCodes::INVALID_CONFIG_PARAMETER};

    auto interface = std::make_unique<InterfaceConfig>(name);

    interface->updateConfig(global_overrides);
    interface->port = raw_port;
    interface->secure = secure_;
    interface->updateConfig(settings);

    return interface;
}

}

}
