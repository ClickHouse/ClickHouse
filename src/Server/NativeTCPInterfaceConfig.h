#pragma once

#include <Server/TCPInterfaceConfig.h>

namespace DB
{

/// Class for server listening interface configs for Native TCP protocol.
class NativeTCPInterfaceConfig final : public TCPInterfaceConfigBase
{
public:
    explicit NativeTCPInterfaceConfig(const std::string & name_);

    static std::unique_ptr<NativeTCPInterfaceConfig> tryParseLegacyInterface(
        const bool secure_,
        const LegacyGlobalConfigOverrides & global_overrides,
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings
    );

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
