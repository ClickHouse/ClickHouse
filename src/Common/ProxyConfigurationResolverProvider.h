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

    /*
     * This API exists exclusively for backward compatibility with old S3 storage specific proxy configuration.
     * */
    static std::shared_ptr<ProxyConfigurationResolver> get(const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration);
};

}
