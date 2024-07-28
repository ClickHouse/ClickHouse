#include <Disks/ObjectStorages/S3/diskSettings.h>

#if USE_AWS_S3

#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/Throttler.h>
#include <Common/ProxyConfigurationResolverProvider.h>
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
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/DiskLocal.h>
#include <Common/Macros.h>

namespace DB
{

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

std::unique_ptr<S3::Client> getClient(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const S3ObjectStorageSettings & settings)
{
    String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
    S3::URI uri(endpoint);
    if (!uri.key.ends_with('/'))
        uri.key.push_back('/');

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        config.getString(config_prefix + ".region", ""),
        context->getRemoteHostFilter(),
        static_cast<int>(context->getGlobalContext()->getSettingsRef().s3_max_redirects),
        context->getGlobalContext()->getSettingsRef().enable_s3_requests_logging,
        /* for_disk_s3 = */ true,
        settings.request_settings.get_request_throttler,
        settings.request_settings.put_request_throttler,
        uri.uri.getScheme());

    client_configuration.connectTimeoutMs = config.getUInt(config_prefix + ".connect_timeout_ms", 1000);
    client_configuration.requestTimeoutMs = config.getUInt(config_prefix + ".request_timeout_ms", 3000);
    client_configuration.maxConnections = config.getUInt(config_prefix + ".max_connections", 100);
    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.http_keep_alive_timeout_ms = config.getUInt(config_prefix + ".http_keep_alive_timeout_ms", 10000);
    client_configuration.http_connection_pool_size = config.getUInt(config_prefix + ".http_connection_pool_size", 1000);
    client_configuration.wait_on_pool_size_limit = false;

    /*
     * Override proxy configuration for backwards compatibility with old configuration format.
     * */
    auto proxy_config = DB::ProxyConfigurationResolverProvider::getFromOldSettingsFormat(config_prefix, config);
    if (proxy_config)
    {
        client_configuration.per_request_configuration
            = [proxy_config]() { return proxy_config->resolve(); };
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
