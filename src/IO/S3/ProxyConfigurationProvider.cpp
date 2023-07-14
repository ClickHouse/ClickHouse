#include <IO/S3/ProxyConfigurationProvider.h>

#if USE_AWS_S3

#include <IO/S3/ProxyConfigurationResolverAdapter.h>
#include <Common/ProxyConfigurationResolverProvider.h>

namespace DB::S3
{

std::shared_ptr<ProxyConfiguration> ProxyConfigurationProvider::get(
    const String & config_prefix,
    const Poco::Util::AbstractConfiguration & configuration
)
{
    return std::make_shared<ProxyConfigurationResolverAdapter>(ProxyConfigurationResolverProvider::get(config_prefix, configuration));
}

std::shared_ptr<ProxyConfiguration> ProxyConfigurationProvider::get()
{
    return std::make_shared<ProxyConfigurationResolverAdapter>(ProxyConfigurationResolverProvider::get());
}

}

#endif
