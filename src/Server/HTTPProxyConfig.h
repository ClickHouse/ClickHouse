#pragma once

#include <Server/ProxyConfig.h>

namespace DB
{

/// Config class for HTTP v1/v2 protocols.
class HTTPProxyConfig final : public ProxyConfig
{
public:
    explicit HTTPProxyConfig(const std::string & name_);

    virtual std::unique_ptr<ProxyConfig> clone() const override;
    virtual void updateConfig(const Poco::Util::AbstractConfiguration & config) override;
    virtual std::unique_ptr<ProxyProtocolHandler> createProxyProtocolHandler() const override;

public:
    std::size_t proxy_chain_limit = 1;
};

}
