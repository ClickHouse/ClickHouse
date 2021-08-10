#pragma once

#include <Server/HTTPInterfaceConfig.h>

namespace DB
{

/// Class for server listening interface configs for Interserver HTTP protocol.
class InterserverHTTPInterfaceConfig final : public HTTPInterfaceConfigBase
{
public:
    explicit InterserverHTTPInterfaceConfig(const std::string & name_);

    static std::unique_ptr<InterserverHTTPInterfaceConfig> tryParseLegacyInterface(
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
