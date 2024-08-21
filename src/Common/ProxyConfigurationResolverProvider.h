#pragma once

#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ProxyConfigurationResolver.h>

namespace DB
{


class ProxyConfigurationResolverProvider
{
public:

    using Protocol = ProxyConfiguration::Protocol;

    /*
     * Returns appropriate ProxyConfigurationResolver based on current CH settings (Remote resolver or List resolver).
     * If no configuration is found, returns Environment Resolver.
     * */
    static std::shared_ptr<ProxyConfigurationResolver> get(Protocol protocol);

    /*
     * This API exists exclusively for backward compatibility with old S3 storage specific proxy configuration.
     * If no configuration is found, returns nullptr.
     * */
    static std::shared_ptr<ProxyConfigurationResolver> getFromOldSettingsFormat(
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration & configuration
    );

private:
    static std::shared_ptr<ProxyConfigurationResolver> getFromSettings(
        Protocol protocol,
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration & configuration
    );
};

}
