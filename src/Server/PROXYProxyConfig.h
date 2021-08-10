#pragma once

#include <Server/ProxyConfig.h>

namespace DB
{

/// Config class for PROXY v1/v2 protocols.
class PROXYProxyConfig final : public ProxyConfig
{
public:
    explicit PROXYProxyConfig(const std::string & name_);

    virtual std::unique_ptr<ProxyConfig> clone() const override;
    virtual void updateConfig(const Poco::Util::AbstractConfiguration & config) override;
    virtual std::unique_ptr<ProxyProtocolHandler> createProxyProtocolHandler() const override;

public:
    enum class Version { v1, v2 };

    Version version = Version::v1;
    bool allow_http_x_forwarded_for = false;
};

}
