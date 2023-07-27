#include <Common/ProtocolAwareProxyConfigurationResolver.h>

namespace DB
{

ProtocolAwareProxyConfigurationResolver::ProtocolAwareProxyConfigurationResolver(
    std::shared_ptr<ProxyConfigurationResolver> http_resolver_,
    std::shared_ptr<ProxyConfigurationResolver> https_resolver_,
    std::shared_ptr<ProxyConfigurationResolver> any_resolver_)
    : http_resolver(http_resolver_), https_resolver(https_resolver_), any_resolver(any_resolver_)
{}

ProxyConfiguration ProtocolAwareProxyConfigurationResolver::resolve(Protocol protocol)
{
    auto resolveOrDefault = [protocol](auto resolver)
    {
        return resolver ? resolver->resolve(protocol) : ProxyConfiguration {};
    };

    switch (protocol)
    {
        case Protocol::HTTP:
            return resolveOrDefault(http_resolver);
        case Protocol::HTTPS:
            return resolveOrDefault(https_resolver);
        case Protocol::ANY:
            return resolveOrDefault(any_resolver);
    }
}

void ProtocolAwareProxyConfigurationResolver::errorReport(const ProxyConfiguration & config)
{
    if (config.protocol == "http")
        http_resolver->errorReport(config);
    else if (config.protocol == "https")
        https_resolver->errorReport(config);
    else
        any_resolver->errorReport(config);
}

}
