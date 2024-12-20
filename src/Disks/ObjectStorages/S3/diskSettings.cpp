#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <IO/S3/Client.h>
#include <Common/Exception.h>

#if USE_AWS_S3

#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/Throttler.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Disks/DiskFactory.h>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <base/getFQDNOrHostName.h>
#include <IO/S3Common.h>
#include <IO/S3/Credentials.h>

#include <Storages/StorageS3Settings.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskLocal.h>
#include <Common/Macros.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NO_ELEMENTS_IN_CONFIG;
}

std::unique_ptr<S3ObjectStorageSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    S3Settings::RequestSettings request_settings(config, config_prefix, settings, "s3_");

    return std::make_unique<S3ObjectStorageSettings>(
        request_settings,
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".list_object_keys_size", 1000),
        config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000),
        config.getBool(config_prefix + ".readonly", false));
}

std::unique_ptr<S3::Client> getClient(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const S3ObjectStorageSettings & settings)
{
    const Settings & global_settings = context->getGlobalContext()->getSettingsRef();
    const Settings & local_settings = context->getSettingsRef();

    const String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
    S3::URI uri(endpoint);
    if (!uri.key.ends_with('/'))
        uri.key.push_back('/');

    if (S3::isS3ExpressEndpoint(endpoint) && !config.has(config_prefix + ".region"))
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Region should be explicitly specified for directory buckets ({})", config_prefix);

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        config.getString(config_prefix + ".region", ""),
        context->getRemoteHostFilter(),
        static_cast<int>(global_settings.s3_max_redirects),
        static_cast<int>(global_settings.s3_retry_attempts),
        global_settings.enable_s3_requests_logging,
        /* for_disk_s3 = */ true,
        settings.request_settings.get_request_throttler,
        settings.request_settings.put_request_throttler,
        uri.uri.getScheme());

    client_configuration.connectTimeoutMs = config.getUInt(config_prefix + ".connect_timeout_ms", S3::DEFAULT_CONNECT_TIMEOUT_MS);
    client_configuration.requestTimeoutMs = config.getUInt(config_prefix + ".request_timeout_ms", S3::DEFAULT_REQUEST_TIMEOUT_MS);
    client_configuration.maxConnections = config.getUInt(config_prefix + ".max_connections", S3::DEFAULT_MAX_CONNECTIONS);
    client_configuration.http_keep_alive_timeout = config.getUInt(config_prefix + ".http_keep_alive_timeout", S3::DEFAULT_KEEP_ALIVE_TIMEOUT);
    client_configuration.http_keep_alive_max_requests = config.getUInt(config_prefix + ".http_keep_alive_max_requests", S3::DEFAULT_KEEP_ALIVE_MAX_REQUESTS);

    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.s3_use_adaptive_timeouts = config.getBool(
        config_prefix + ".use_adaptive_timeouts", client_configuration.s3_use_adaptive_timeouts);

    /*
     * Override proxy configuration for backwards compatibility with old configuration format.
     * */
    auto proxy_config = DB::ProxyConfigurationResolverProvider::getFromOldSettingsFormat(
        ProxyConfiguration::protocolFromString(uri.uri.getScheme()),
        config_prefix,
        config
    );
    if (proxy_config)
    {
        client_configuration.per_request_configuration
            = [proxy_config]() { return proxy_config->resolve(); };
        client_configuration.error_report
            = [proxy_config](const auto & request_config) { proxy_config->errorReport(request_config); };
    }

    HTTPHeaderEntries headers = S3::getHTTPHeaders(config_prefix, config);
    S3::ServerSideEncryptionKMSConfig sse_kms_config = S3::getSSEKMSConfig(config_prefix, config);

    S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .disable_checksum = local_settings.s3_disable_checksum,
        .gcs_issue_compose_request = config.getBool("s3.gcs_issue_compose_request", false),
        .is_s3express_bucket = S3::isS3ExpressEndpoint(endpoint),
    };

    return S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
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
