#include <IO/S3/ProxyConfigurationProvider.h>

#if USE_AWS_S3

#include <IO/S3/ProxyConfigurationResolverAdapter.h>
#include <Common/ProxyConfigurationResolverProvider.h>

namespace DB::S3
{

namespace
{
    auto adapt(auto resolver)
    {
        return std::make_shared<ProxyConfigurationResolverAdapter>(resolver);
    }
}

using Protocol = ProxyConfigurationResolverProvider::Protocol;

std::shared_ptr<ProxyConfiguration> ProxyConfigurationProvider::get(
    const String & config_prefix,
    const Poco::Util::AbstractConfiguration & configuration
)
{
    /*
     * First try to get it from settings only using the combination of config_prefix and configuration.
     * This logic exists for backward compatibility with old S3 storage specific proxy configuration.
     * */
    if (auto resolver = ProxyConfigurationResolverProvider::getFromSettings(Protocol::ANY, config_prefix, configuration))
    {
        return adapt(resolver);
    }

    /*
     * In case the combination of config_prefix and configuration does not provide a resolver, try to get it from general / new settings.
     * Falls back to Environment resolver if no configuration is found.
     * */
    return adapt(ProxyConfigurationResolverProvider::get(Protocol::ANY));
}

std::shared_ptr<ProxyConfiguration> ProxyConfigurationProvider::get(const String & protocol)
{
    return adapt(ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::protocolFromString(protocol)));
}

}

#endif
