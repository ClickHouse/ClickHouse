#pragma once

#include <Server/InterfaceConfig.h>

namespace DB
{

/// Class for server listening interface configs for Native gRPC protocol.
class NativeGRPCInterfaceConfig final : public MultiEndpointInterfaceConfigBase
{
public:
    explicit NativeGRPCInterfaceConfig(const std::string & name_);

    static std::unique_ptr<NativeGRPCInterfaceConfig> tryParseLegacyInterface(
        const LegacyGlobalConfigOverrides & global_overrides,
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings
    );

public:
    virtual void updateConfig(
        const Poco::Util::AbstractConfiguration & config,
        const ProxyConfigs & proxies_
    ) override;

    using MultiEndpointInterfaceConfigBase::updateConfig;

protected:
    virtual void createSingleServer(
        ProtocolServerAdapter & adapter,
        const std::string & host,
        IServer & server,
        Poco::ThreadPool & pool,
        AsynchronousMetrics * async_metrics
    ) override;
};

}
