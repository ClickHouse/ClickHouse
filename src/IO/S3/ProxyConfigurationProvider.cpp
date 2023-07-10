#include <IO/S3/ProxyConfigurationProvider.h>

#if USE_AWS_S3

#include <IO/S3/ProxyConfigurationResolverAdapter.h>
#include <Common/ProxyConfigurationResolverProvider.h>

namespace DB::S3
{

std::shared_ptr<ProxyConfiguration> ProxyConfigurationProvider::get(const String & prefix,
                                                                    const Poco::Util::AbstractConfiguration & config)
{
    return std::make_shared<ProxyConfigurationResolverAdapter>(ProxyConfigurationResolverProvider::get(prefix, config));
}

std::shared_ptr<ProxyConfiguration> ProxyConfigurationProvider::get(const Poco::Util::AbstractConfiguration & config)
{
    return get("", config);
}

}

#endif
