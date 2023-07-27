#pragma once

#include <Common/ProxyConfigurationResolver.h>

namespace DB
{

class ProtocolAwareProxyConfigurationResolver : public ProxyConfigurationResolver
{
public:
    ProtocolAwareProxyConfigurationResolver(
        std::shared_ptr<ProxyConfigurationResolver> http_resolver_,
        std::shared_ptr<ProxyConfigurationResolver> https_resolver_,
        std::shared_ptr<ProxyConfigurationResolver> any_resolver_);

    ProxyConfiguration resolve(Protocol protocol) override;
    void errorReport(const ProxyConfiguration & config) override;
private:
    std::shared_ptr<ProxyConfigurationResolver> http_resolver;
    std::shared_ptr<ProxyConfigurationResolver> https_resolver;
    std::shared_ptr<ProxyConfigurationResolver> any_resolver;
};

}
