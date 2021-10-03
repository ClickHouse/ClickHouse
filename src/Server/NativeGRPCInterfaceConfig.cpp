#include <Server/NativeGRPCInterfaceConfig.h>
#include <Server/InterfaceConfigUtil.h>
#include <Server/IServer.h>
#include <Server/ProtocolServerAdapter.h>
#include <Common/Exception.h>
#include <base/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>

#if USE_GRPC
#   include <Server/GRPCServer.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SUPPORT_IS_DISABLED;
}

NativeGRPCInterfaceConfig::NativeGRPCInterfaceConfig(const std::string & name_)
    : MultiEndpointInterfaceConfigBase(name_, "native_grpc")
{
}

void NativeGRPCInterfaceConfig::updateConfig(
    const Poco::Util::AbstractConfiguration & config,
    const ProxyConfigs & proxies_
)
{
    MultiEndpointInterfaceConfigBase::updateConfig(config, proxies_);

    if (config.has("allow_direct") && !config.getBool("allow_direct"))
        throw Exception("Since proxies are not supported for Native gRPC connections, allow_direct, if specified, must be set to true", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    if (config.has("allow_proxies"))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("allow_proxies", keys);

        if (!keys.empty())
            throw Exception("Proxies are not supported for Native gRPC connections", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
    }
}

void NativeGRPCInterfaceConfig::createSingleServer([[maybe_unused]] ProtocolServerAdapter & adapter, [[maybe_unused]] const std::string & host, [[maybe_unused]] IServer & server,[[maybe_unused]] Poco::ThreadPool & pool, [[maybe_unused]] AsynchronousMetrics * async_metrics)
{
#if USE_GRPC
    auto address = Util::makeSocketAddress(host, port, &server.logger());
    adapter.add(std::make_unique<GRPCServer>(server, address, *this));

    LOG_INFO(&server.logger(), "Listening for connections with Native gRPC protocol ({}): {}", name, address.toString());
#else
    throw Exception("ClickHouse server was built without gRPC library. Cannot use Native gRPC protocol.", ErrorCodes::SUPPORT_IS_DISABLED);
#endif
}

std::unique_ptr<NativeGRPCInterfaceConfig> NativeGRPCInterfaceConfig::tryParseLegacyInterface(
    const LegacyGlobalConfigOverrides & global_overrides,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    const std::string port_prefix = "grpc_port";

    if (!config.has(port_prefix))
        return {};

    const auto raw_port = config.getUInt(port_prefix);

    if (raw_port > std::numeric_limits<decltype(port)>::max())
        throw Exception{"Value for port is out of range", ErrorCodes::INVALID_CONFIG_PARAMETER};

    auto interface = std::make_unique<NativeGRPCInterfaceConfig>("LegacyGRPC");

    interface->updateConfig(global_overrides);
    interface->port = raw_port;
    interface->updateConfig(settings);

    return interface;
}

}
