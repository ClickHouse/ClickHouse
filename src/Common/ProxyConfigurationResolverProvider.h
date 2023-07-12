#pragma once

#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ProxyConfigurationResolver.h>

namespace DB
{

/*
 * Returns appropriate ProxyConfigurationResolver based on current CH settings (Remote resolver or List resolver).
 * If no configuration is found, returns Environment Resolver.
 * */
class ProxyConfigurationResolverProvider
{
public:
    static std::shared_ptr<ProxyConfigurationResolver> get();
    static std::shared_ptr<ProxyConfigurationResolver> get(const String & config_prefix);

private:
    static std::shared_ptr<ProxyConfigurationResolver> getRemoteResolver(
        const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration);

    static std::shared_ptr<ProxyConfigurationResolver> getListResolver(
        const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration);
};

}
