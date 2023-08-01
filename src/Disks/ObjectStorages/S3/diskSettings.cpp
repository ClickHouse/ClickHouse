#include <Disks/ObjectStorages/S3/diskSettings.h>

#if USE_AWS_S3

#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/Throttler.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include "Disks/DiskFactory.h"

#include <aws/core/client/DefaultRetryStrategy.h>
#include <base/getFQDNOrHostName.h>
#include <IO/S3Common.h>
#include <IO/S3/Credentials.h>

#include <Storages/StorageS3Settings.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/S3/ProxyConfiguration.h>
#include <Disks/ObjectStorages/S3/ProxyListConfiguration.h>
#include <Disks/ObjectStorages/S3/ProxyResolverConfiguration.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/DiskLocal.h>
#include <Common/Macros.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::unique_ptr<S3ObjectStorageSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    S3Settings::RequestSettings request_settings(config, config_prefix, settings, "s3_");

    return std::make_unique<S3ObjectStorageSettings>(
        request_settings,
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".list_object_keys_size", 1000),
        config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000));
}

std::shared_ptr<S3::ProxyResolverConfiguration> getProxyResolverConfiguration(
    const String & prefix, const Poco::Util::AbstractConfiguration & proxy_resolver_config)
{
    auto endpoint = Poco::URI(proxy_resolver_config.getString(prefix + ".endpoint"));
    auto proxy_scheme = proxy_resolver_config.getString(prefix + ".proxy_scheme");
    if (proxy_scheme != "http" && proxy_scheme != "https")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only HTTP/HTTPS schemas allowed in proxy resolver config: {}", proxy_scheme);
    auto proxy_port = proxy_resolver_config.getUInt(prefix + ".proxy_port");
    auto cache_ttl = proxy_resolver_config.getUInt(prefix + ".proxy_cache_time", 10);

    LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Configured proxy resolver: {}, Scheme: {}, Port: {}",
        endpoint.toString(), proxy_scheme, proxy_port);

    return std::make_shared<S3::ProxyResolverConfiguration>(endpoint, proxy_scheme, proxy_port, cache_ttl);
}

std::shared_ptr<S3::ProxyListConfiguration> getProxyListConfiguration(
    const String & prefix, const Poco::Util::AbstractConfiguration & proxy_config)
{
    std::vector<String> keys;
    proxy_config.keys(prefix, keys);

    std::vector<Poco::URI> proxies;
    for (const auto & key : keys)
        if (startsWith(key, "uri"))
        {
            Poco::URI proxy_uri(proxy_config.getString(prefix + "." + key));

            if (proxy_uri.getScheme() != "http" && proxy_uri.getScheme() != "https")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only HTTP/HTTPS schemas allowed in proxy uri: {}", proxy_uri.toString());
            if (proxy_uri.getHost().empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty host in proxy uri: {}", proxy_uri.toString());

            proxies.push_back(proxy_uri);

            LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Configured proxy: {}", proxy_uri.toString());
        }

    if (!proxies.empty())
        return std::make_shared<S3::ProxyListConfiguration>(proxies);

    return nullptr;
}

std::shared_ptr<S3::ProxyConfiguration> getProxyConfiguration(const String & prefix, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(prefix + ".proxy"))
        return nullptr;

    std::vector<String> config_keys;
    config.keys(prefix + ".proxy", config_keys);

    if (auto resolver_configs = std::count(config_keys.begin(), config_keys.end(), "resolver"))
    {
        if (resolver_configs > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple proxy resolver configurations aren't allowed");

        return getProxyResolverConfiguration(prefix + ".proxy.resolver", config);
    }

    return getProxyListConfiguration(prefix + ".proxy", config);
}


std::unique_ptr<S3::Client> getClient(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const S3ObjectStorageSettings & settings)
{
    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        config.getString(config_prefix + ".region", ""),
        context->getRemoteHostFilter(),
        static_cast<int>(context->getGlobalContext()->getSettingsRef().s3_max_redirects),
        context->getGlobalContext()->getSettingsRef().enable_s3_requests_logging,
        /* for_disk_s3 = */ true,
        settings.request_settings.get_request_throttler,
        settings.request_settings.put_request_throttler);

    String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
    S3::URI uri(endpoint);
    if (uri.key.back() != '/')
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "S3 path must ends with '/', but '{}' doesn't.", uri.key);

    client_configuration.connectTimeoutMs = config.getUInt(config_prefix + ".connect_timeout_ms", 1000);
    client_configuration.requestTimeoutMs = config.getUInt(config_prefix + ".request_timeout_ms", 3000);
    client_configuration.maxConnections = config.getUInt(config_prefix + ".max_connections", 100);
    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.http_keep_alive_timeout_ms = config.getUInt(config_prefix + ".http_keep_alive_timeout_ms", 10000);
    client_configuration.http_connection_pool_size = config.getUInt(config_prefix + ".http_connection_pool_size", 1000);
    client_configuration.wait_on_pool_size_limit = false;

    auto proxy_config = getProxyConfiguration(config_prefix, config);
    if (proxy_config)
    {
        client_configuration.per_request_configuration
            = [proxy_config](const auto & request) { return proxy_config->getConfiguration(request); };
        client_configuration.error_report
            = [proxy_config](const auto & request_config) { proxy_config->errorReport(request_config); };
    }

    HTTPHeaderEntries headers = S3::getHTTPHeaders(config_prefix, config);
    S3::ServerSideEncryptionKMSConfig sse_kms_config = S3::getSSEKMSConfig(config_prefix, config);

    client_configuration.retryStrategy
        = std::make_shared<Aws::Client::DefaultRetryStrategy>(
            config.getUInt64(config_prefix + ".retry_attempts", settings.request_settings.retry_attempts));

    return S3::ClientFactory::instance().create(
        client_configuration,
        uri.is_virtual_hosted_style,
        config.getString(config_prefix + ".access_key_id", ""),
        config.getString(config_prefix + ".secret_access_key", ""),
        config.getString(config_prefix + ".server_side_encryption_customer_key_base64", ""),
        std::move(sse_kms_config),
        std::move(headers),
        S3::CredentialsConfiguration
        {
            config.getBool(config_prefix + ".use_environment_credentials", config.getBool("s3.use_environment_credentials", true)),
            config.getBool(config_prefix + ".use_insecure_imds_request", config.getBool("s3.use_insecure_imds_request", false)),
            config.getUInt64(config_prefix + ".expiration_window_seconds", config.getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
            config.getBool(config_prefix + ".no_sign_request", config.getBool("s3.no_sign_request", false))
        });
}

}

#endif
