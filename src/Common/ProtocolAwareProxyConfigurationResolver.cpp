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
    auto reportOrNothing = [config](auto resolver)
    {
        if (resolver)
        {
            resolver->errorReport(config);
        }
    };

    switch (config.protocol)
    {
        case Protocol::HTTP:
            reportOrNothing(http_resolver);
            break;
        case Protocol::HTTPS:
            reportOrNothing(https_resolver);
            break;
        case Protocol::ANY:
            reportOrNothing(any_resolver);
            break;
    }
}

}
