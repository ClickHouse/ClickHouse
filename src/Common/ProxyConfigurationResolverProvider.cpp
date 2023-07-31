#include <Common/ProxyConfigurationResolverProvider.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Common/Exception.h>
#include <Common/ProtocolAwareProxyConfigurationResolver.h>
#include <Common/ProxyListConfigurationResolver.h>
#include <Common/RemoteProxyConfigurationResolver.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    std::shared_ptr<ProxyConfigurationResolver> getRemoteResolver(
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

    std::shared_ptr<ProxyConfigurationResolver> getRemoteResolver(
        const String & protocol, const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration)
    {
        std::vector<String> keys;
        configuration.keys(config_prefix, keys);

        std::vector<Poco::URI> uris;
        for (const auto & key : keys)
        {
            if (startsWith(key, "resolver"))
            {
                auto config_protocol = configuration.getString(config_prefix + ".resolver.proxy_scheme");
                if (config_protocol == protocol)
                {
                    return getRemoteResolver(config_prefix + "." + key, configuration);
                }
            }
        }

        return nullptr;
    }

    std::pair<std::shared_ptr<ProxyConfigurationResolver>, std::shared_ptr<ProxyConfigurationResolver>> getRemoteResolvers(
        const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration)
    {
        auto http_resolver = getRemoteResolver("http", config_prefix, configuration);
        auto https_resolver = getRemoteResolver("https", config_prefix, configuration);

        if (!https_resolver)
        {
            https_resolver = http_resolver;
        }

        return {http_resolver, https_resolver};
    }

    auto extractURIList(const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration)
    {
        std::vector<String> keys;
        configuration.keys(config_prefix, keys);

        std::vector<Poco::URI> uris;
        for (const auto & key : keys)
        {
            if (startsWith(key, "uri"))
            {
                Poco::URI proxy_uri(configuration.getString(config_prefix + "." + key));

                if (proxy_uri.getScheme() != "http" && proxy_uri.getScheme() != "https")
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only HTTP/HTTPS schemas allowed in proxy uri: {}", proxy_uri.toString());
                if (proxy_uri.getHost().empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty host in proxy uri: {}", proxy_uri.toString());

                uris.push_back(proxy_uri);

                LOG_DEBUG(&Poco::Logger::get("ProxyConfigurationResolverProvider"), "Configured proxy: {}", proxy_uri.toString());
            }
        }

        return uris;
    }

    std::shared_ptr<ProxyConfigurationResolver> getListResolver(
        const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration)
    {
        std::vector<String> keys;
        configuration.keys(config_prefix, keys);

        bool new_setting_syntax = std::find_if(
                                        keys.begin(),
                                        keys.end(),
                                        [](const String & key)
                                        {
                                            return startsWith(key, "http") || startsWith(key, "https");
                                        }) != keys.end();

        std::vector<Poco::URI> http_uris;
        std::vector<Poco::URI> https_uris;

        if (new_setting_syntax)
        {
            for (const auto & key : keys)
            {
                if (key == "http")
                {
                    for (const auto & uri : extractURIList(config_prefix + "." + key, configuration))
                    {
                        http_uris.push_back(uri);
                    }
                }
                else if (key == "https")
                {
                    for (const auto & uri : extractURIList(config_prefix + "." + key, configuration))
                    {
                        https_uris.push_back(uri);
                    }
                }
                else if (key == "uri")
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Old proxy syntax can't be mixed with new one");
                }
            }
        }
        else
        {
            http_uris = extractURIList(config_prefix, configuration);

            // Old syntax does not make a distinction between HTTP and HTTPs proxies.
            https_uris = http_uris;
        }

        if (http_uris.empty() && https_uris.empty())
        {
            return nullptr;
        }

        auto http_resolver = std::make_shared<ProxyListConfigurationResolver>(http_uris);
        auto https_resolver = std::make_shared<ProxyListConfigurationResolver>(https_uris);
        auto any_resolver = https_resolver;

        return std::make_shared<ProtocolAwareProxyConfigurationResolver>(http_resolver, https_resolver, any_resolver);
    }
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::get()
{
    if (auto context = Context::getGlobalContextInstance())
    {
        if (auto resolver = getFromSettings("", context->getConfigRef()))
        {
            return resolver;
        }
    }

    return std::make_shared<EnvironmentProxyConfigurationResolver>();
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::getFromSettings(
    const String & config_prefix,
    const Poco::Util::AbstractConfiguration & configuration
)
{
    auto proxy_prefix = config_prefix.empty() ? "proxy" : config_prefix + ".proxy";

    if (configuration.has(proxy_prefix))
    {
        std::vector<String> config_keys;
        configuration.keys(proxy_prefix, config_keys);

        if (auto resolver_configs = std::count(config_keys.begin(), config_keys.end(), "resolver"))
        {
            if (resolver_configs > 2)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only two remote proxy resolvers are allowed, one for HTTP and one for HTTPs");
            }

            auto [http_resolver, https_resolver] = getRemoteResolvers(proxy_prefix, configuration);
            auto any_resolver = https_resolver;

            return std::make_shared<ProtocolAwareProxyConfigurationResolver>(http_resolver, https_resolver, any_resolver);
        }

        if (auto list_resolver = getListResolver(proxy_prefix, configuration))
        {
            return list_resolver;
        }
    }

    return nullptr;
}

}
