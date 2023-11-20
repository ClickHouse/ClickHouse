#include <Common/ProxyConfigurationResolverProvider.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Common/Exception.h>
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
        ProxyConfiguration::Protocol protocol, const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration)
    {
        std::vector<String> keys;
        configuration.keys(config_prefix, keys);

        std::vector<Poco::URI> uris;
        for (const auto & key : keys)
        {
            if (startsWith(key, "resolver"))
            {
                auto prefix_with_key = config_prefix + "." + key;
                auto proxy_scheme_config_string = prefix_with_key + ".proxy_scheme";
                auto config_protocol = configuration.getString(proxy_scheme_config_string);

                if (ProxyConfiguration::Protocol::ANY == protocol || config_protocol == ProxyConfiguration::protocolToString(protocol))
                {
                    return getRemoteResolver(prefix_with_key, configuration);
                }
            }
        }

        return nullptr;
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

    std::shared_ptr<ProxyConfigurationResolver> getListResolverNewSyntax(
        ProxyConfiguration::Protocol protocol,
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration & configuration
    )
    {
        std::vector<Poco::URI> uris;

        bool include_http_uris = ProxyConfiguration::Protocol::ANY == protocol || ProxyConfiguration::Protocol::HTTP == protocol;

        if (include_http_uris && configuration.has(config_prefix + ".http"))
        {
            auto http_uris = extractURIList(config_prefix + ".http", configuration);
            uris.insert(uris.end(), http_uris.begin(), http_uris.end());
        }

        bool include_https_uris = ProxyConfiguration::Protocol::ANY == protocol || ProxyConfiguration::Protocol::HTTPS == protocol;

        if (include_https_uris && configuration.has(config_prefix + ".https"))
        {
            auto https_uris = extractURIList(config_prefix + ".https", configuration);
            uris.insert(uris.end(), https_uris.begin(), https_uris.end());
        }

        return uris.empty() ? nullptr : std::make_shared<ProxyListConfigurationResolver>(uris);
    }

    std::shared_ptr<ProxyConfigurationResolver> getListResolverOldSyntax(
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration & configuration
    )
    {
        auto uris = extractURIList(config_prefix, configuration);

        return uris.empty() ? nullptr : std::make_shared<ProxyListConfigurationResolver>(uris);
    }

    std::shared_ptr<ProxyConfigurationResolver> getListResolver(
        ProxyConfiguration::Protocol protocol, const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration
    )
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

        return new_setting_syntax ? getListResolverNewSyntax(protocol, config_prefix, configuration)
                                  : getListResolverOldSyntax(config_prefix, configuration);
    }
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::get(Protocol protocol)
{
    auto context = Context::getGlobalContextInstance();

    chassert(context);

    if (auto resolver = getFromSettings(protocol, "", context->getConfigRef()))
    {
        return resolver;
    }

    return std::make_shared<EnvironmentProxyConfigurationResolver>(protocol);
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::getFromSettings(
    Protocol protocol,
    const String & config_prefix,
    const Poco::Util::AbstractConfiguration & configuration
)
{
    auto proxy_prefix = config_prefix.empty() ? "proxy" : config_prefix + ".proxy";

    if (configuration.has(proxy_prefix))
    {
        std::vector<String> config_keys;
        configuration.keys(proxy_prefix, config_keys);

        if (auto remote_resolver = getRemoteResolver(protocol, proxy_prefix, configuration))
        {
            return remote_resolver;
        }

        if (auto list_resolver = getListResolver(protocol, proxy_prefix, configuration))
        {
            return list_resolver;
        }
    }

    return nullptr;
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::getFromOldSettingsFormat(
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
        return resolver;
    }

    /*
     * In case the combination of config_prefix and configuration does not provide a resolver, try to get it from general / new settings.
     * Falls back to Environment resolver if no configuration is found.
     * */
    return ProxyConfigurationResolverProvider::get(Protocol::ANY);
}

}
