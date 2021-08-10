#pragma once

#include <Server/HTTPInterfaceConfig.h>

namespace DB
{

/// Class for server listening interface configs for Prometheus protocol.
class PrometheusInterfaceConfig final : public HTTPInterfaceConfigBase
{
public:
    explicit PrometheusInterfaceConfig(const std::string & name_);

    static std::unique_ptr<PrometheusInterfaceConfig> tryParseLegacyInterface(
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
