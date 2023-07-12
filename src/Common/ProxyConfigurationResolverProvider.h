#pragma once

#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ProxyConfigurationResolver.h>

namespace DB
{

class ProxyConfigurationResolverProvider
{
public:
    static std::shared_ptr<ProxyConfigurationResolver> get(const String & config_prefix);
    static std::shared_ptr<ProxyConfigurationResolver> get();

private:
    static std::shared_ptr<ProxyConfigurationResolver> getRemoteResolver(
        const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration);

    static std::shared_ptr<ProxyConfigurationResolver> getListResolver(
        const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration);
};

}
