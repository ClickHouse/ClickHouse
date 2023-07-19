#include <Common/MethodAwareProxyConfigurationResolver.h>

namespace DB
{

MethodAwareProxyConfigurationResolver::MethodAwareProxyConfigurationResolver(
    std::shared_ptr<ProxyConfigurationResolver> http_resolver_,
    std::shared_ptr<ProxyConfigurationResolver> https_resolver_,
    std::shared_ptr<ProxyConfigurationResolver> any_resolver_)
    : http_resolver(http_resolver_), https_resolver(https_resolver_), any_resolver(any_resolver_)
{}

ProxyConfiguration MethodAwareProxyConfigurationResolver::resolve(Method method)
{
    auto resolveOrDefault = [method](auto resolver)
    {
        return resolver ? resolver->resolve(method) : ProxyConfiguration {};
    };

    switch (method)
    {
        case Method::HTTP:
            return resolveOrDefault(http_resolver);
        case Method::HTTPS:
            return resolveOrDefault(https_resolver);
        case Method::ANY:
            return resolveOrDefault(any_resolver);
    }
}

void MethodAwareProxyConfigurationResolver::errorReport(const ProxyConfiguration & config)
{
    if (config.scheme == "http")
        http_resolver->errorReport(config);
    else if (config.scheme == "https")
        https_resolver->errorReport(config);
    else
        any_resolver->errorReport(config);
}

}
