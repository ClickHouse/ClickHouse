#include <Common/ProxyConfigurationResolverProvider.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Common/ProxyListConfigurationResolver.h>
#include <Common/RemoteProxyConfigurationResolver.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::get()
{
    return get("");
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::get(const String & config_prefix)
{
    if (auto context = Context::getGlobalContextInstance())
    {
        const auto & configuration = context->getConfigRef();

        auto proxy_prefix = config_prefix + ".proxy";

        if (configuration.has(proxy_prefix))
        {
            std::vector<String> config_keys;
            configuration.keys(proxy_prefix, config_keys);

            if (auto resolver_configs = std::count(config_keys.begin(), config_keys.end(), "resolver"))
            {
                if (resolver_configs > 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple proxy resolver configurations aren't allowed");

                return getRemoteResolver(proxy_prefix + ".resolver", configuration);
            }

            if (auto list_resolver = getListResolver(proxy_prefix, configuration))
            {
                return list_resolver;
            }
        }
    }

    return std::make_shared<EnvironmentProxyConfigurationResolver>();
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::getRemoteResolver(
    const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration)
{
    auto endpoint = Poco::URI(configuration.getString(config_prefix + ".endpoint"));
    auto proxy_scheme = configuration.getString(config_prefix + ".proxy_scheme");
    if (proxy_scheme != "http" && proxy_scheme != "https")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only HTTP/HTTPS schemas allowed in proxy resolver config: {}", proxy_scheme);
    auto proxy_port = configuration.getUInt(config_prefix + ".proxy_port");
    auto cache_ttl = configuration.getUInt(config_prefix + ".proxy_cache_time", 10);

    LOG_DEBUG(&Poco::Logger::get("ProxyConfigurationResolverProvider"), "Configured remote proxy resolver: {}, Scheme: {}, Port: {}",
              endpoint.toString(), proxy_scheme, proxy_port);

    return std::make_shared<RemoteProxyConfigurationResolver>(endpoint, proxy_scheme, proxy_port, cache_ttl);
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::getListResolver(
    const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration)
{
    std::vector<String> keys;
    configuration.keys(config_prefix, keys);

    std::vector<Poco::URI> proxies;
    for (const auto & key : keys)
        if (startsWith(key, "uri"))
        {
            Poco::URI proxy_uri(configuration.getString(config_prefix + "." + key));

            if (proxy_uri.getScheme() != "http" && proxy_uri.getScheme() != "https")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only HTTP/HTTPS schemas allowed in proxy uri: {}", proxy_uri.toString());
            if (proxy_uri.getHost().empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty host in proxy uri: {}", proxy_uri.toString());

            proxies.push_back(proxy_uri);

            LOG_DEBUG(&Poco::Logger::get("ProxyConfigurationResolverProvider"), "Configured proxy: {}", proxy_uri.toString());
        }

    if (!proxies.empty())
        return std::make_shared<ProxyListConfigurationResolver>(proxies);

    return nullptr;
}

}
