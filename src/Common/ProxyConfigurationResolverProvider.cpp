#include <Common/ProxyConfigurationResolverProvider.h>

#include <Common/EnvironmentProxyConfigurationResolver.h>
#include <Common/proxyConfigurationToPocoProxyConfig.h>
#include <Common/Exception.h>
#include <Common/ProxyListConfigurationResolver.h>
#include <Common/RemoteProxyConfigurationResolver.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    std::string getNoProxyHosts(const Poco::Util::AbstractConfiguration & configuration)
    {
        return configuration.getString("proxy.no_proxy", "");
    }

    bool isTunnelingDisabledForHTTPSRequestsOverHTTPProxy(
        const Poco::Util::AbstractConfiguration & configuration)
    {
        return configuration.getBool("proxy.disable_tunneling_for_https_requests_over_http_proxy", false);
    }

    std::shared_ptr<ProxyConfigurationResolver> getRemoteResolver(
        ProxyConfiguration::Protocol request_protocol,
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration & configuration)
    {
        auto resolver_prefix = config_prefix + ".resolver";
        auto endpoint = Poco::URI(configuration.getString(resolver_prefix + ".endpoint"));
        auto proxy_scheme = configuration.getString(resolver_prefix + ".proxy_scheme");
        if (proxy_scheme != "http" && proxy_scheme != "https")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only HTTP/HTTPS schemas allowed in proxy resolver config: {}", proxy_scheme);
        auto proxy_port = configuration.getUInt(resolver_prefix + ".proxy_port");
        auto cache_ttl = configuration.getUInt(resolver_prefix + ".proxy_cache_time", 10);

        LOG_DEBUG(getLogger("ProxyConfigurationResolverProvider"), "Configured remote proxy resolver: {}, Scheme: {}, Port: {}",
                  endpoint.toString(), proxy_scheme, proxy_port);

        auto server_configuration = RemoteProxyConfigurationResolver::RemoteServerConfiguration {
            endpoint,
            proxy_scheme,
            proxy_port,
            std::chrono::seconds {cache_ttl}
        };

        return std::make_shared<RemoteProxyConfigurationResolver>(
            server_configuration,
            request_protocol,
            buildPocoNonProxyHosts(getNoProxyHosts(configuration)),
            std::make_shared<RemoteProxyHostFetcherImpl>(),
            isTunnelingDisabledForHTTPSRequestsOverHTTPProxy(configuration));
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

                LOG_DEBUG(getLogger("ProxyConfigurationResolverProvider"), "Configured proxy: {}", proxy_uri.toString());
            }
        }

        return uris;
    }

    std::shared_ptr<ProxyConfigurationResolver> getListResolver(
        ProxyConfiguration::Protocol request_protocol,
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration & configuration)
    {
        auto uris = extractURIList(config_prefix, configuration);

        return uris.empty()
            ? nullptr
            : std::make_shared<ProxyListConfigurationResolver>(
                  uris,
                  request_protocol,
                  buildPocoNonProxyHosts(getNoProxyHosts(configuration)),
                  isTunnelingDisabledForHTTPSRequestsOverHTTPProxy(configuration));
    }

    bool hasRemoteResolver(const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration)
    {
        return configuration.has(config_prefix + ".resolver");
    }

    bool hasListResolver(const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration)
    {
        return configuration.has(config_prefix + ".uri");
    }

    /* New syntax requires protocol prefix "<http> or <https>"
     */
    std::optional<std::string> getProtocolPrefix(
        ProxyConfiguration::Protocol request_protocol,
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration & configuration
    )
    {
        auto protocol_prefix = config_prefix + "." + ProxyConfiguration::protocolToString(request_protocol);
        if (!configuration.has(protocol_prefix))
        {
            return std::nullopt;
        }

        return protocol_prefix;
    }

    std::optional<std::string> calculatePrefixBasedOnSettingsSyntax(
        bool new_syntax,
        ProxyConfiguration::Protocol request_protocol,
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration & configuration
    )
    {
        if (!configuration.has(config_prefix))
            return std::nullopt;

        if (new_syntax)
            return getProtocolPrefix(request_protocol, config_prefix, configuration);

        return config_prefix;
    }
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::get(
    Protocol request_protocol,
    const Poco::Util::AbstractConfiguration & configuration)
{
    if (auto resolver = getFromSettings(true, request_protocol, "proxy", configuration))
        return resolver;

    return std::make_shared<EnvironmentProxyConfigurationResolver>(
        request_protocol,
        isTunnelingDisabledForHTTPSRequestsOverHTTPProxy(configuration));
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::getFromSettings(
    bool new_syntax,
    Protocol request_protocol,
    const String & config_prefix,
    const Poco::Util::AbstractConfiguration & configuration)
{
    auto prefix_opt = calculatePrefixBasedOnSettingsSyntax(new_syntax, request_protocol, config_prefix, configuration);

    if (!prefix_opt)
    {
        return nullptr;
    }
    const auto & prefix = *prefix_opt;

    if (hasRemoteResolver(prefix, configuration))
    {
        return getRemoteResolver(request_protocol, prefix, configuration);
    }
    if (hasListResolver(prefix, configuration))
    {
        return getListResolver(request_protocol, prefix, configuration);
    }

    return nullptr;
}

std::shared_ptr<ProxyConfigurationResolver> ProxyConfigurationResolverProvider::getFromOldSettingsFormat(
    Protocol request_protocol,
    const String & config_prefix,
    const Poco::Util::AbstractConfiguration & configuration)
{
    /* First try to get it from settings only using the combination of config_prefix and configuration.
     * This logic exists for backward compatibility with old S3 storage specific proxy configuration.
     * */
    if (auto resolver = ProxyConfigurationResolverProvider::getFromSettings(false, request_protocol, config_prefix + ".proxy", configuration))
    {
        return resolver;
    }

    /* In case the combination of config_prefix and configuration does not provide a resolver, try to get it from general / new settings.
     * Falls back to Environment resolver if no configuration is found.
     * */
    return ProxyConfigurationResolverProvider::get(request_protocol, configuration);
}

}
