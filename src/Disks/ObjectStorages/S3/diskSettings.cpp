#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <IO/S3/Client.h>
#include <Common/Exception.h>

#if USE_AWS_S3

#include <Common/StringUtils.h>
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
    extern const int LOGICAL_ERROR;
}

namespace ErrorCodes
{
extern const int NO_ELEMENTS_IN_CONFIG;
}

std::unique_ptr<S3ObjectStorageSettings> getSettings(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    bool validate_settings)
{
    const Settings & settings = context->getSettingsRef();
    auto request_settings = S3Settings::RequestSettings(config, config_prefix, settings, "s3_", validate_settings);
    auto auth_settings = S3::AuthSettings::loadFromConfig(config_prefix, config);

    return std::make_unique<S3ObjectStorageSettings>(
        request_settings,
        auth_settings,
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".list_object_keys_size", 1000),
        config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000),
        config.getBool(config_prefix + ".readonly", false));
}

std::unique_ptr<S3::Client> getClient(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const S3ObjectStorageSettings & settings,
    bool for_disk_s3,
    const S3::URI * url_)
{
    const Settings & global_settings = context->getGlobalContext()->getSettingsRef();
    const Settings & local_settings = context->getSettingsRef();

    const auto & auth_settings = settings.auth_settings;
    const auto & request_settings = settings.request_settings;

    S3::URI url;
    if (for_disk_s3)
    {
        String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
        url = S3::URI(endpoint);
        if (!url.key.ends_with('/'))
            url.key.push_back('/');
    }
    else
    {
        if (!url_)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "URL not passed");
        url = *url_;
    }
    const bool is_s3_express_bucket = S3::isS3ExpressEndpoint(url.endpoint);
    if (is_s3_express_bucket && !config.has(config_prefix + ".region"))
    {
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Region should be explicitly specified for directory buckets ({})", config_prefix);
    }

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        auth_settings.region,
        context->getRemoteHostFilter(),
        static_cast<int>(global_settings.s3_max_redirects),
        static_cast<int>(global_settings.s3_retry_attempts),
        global_settings.enable_s3_requests_logging,
        for_disk_s3,
        settings.request_settings.get_request_throttler,
        settings.request_settings.put_request_throttler,
        url.uri.getScheme());

    client_configuration.connectTimeoutMs = config.getUInt64(config_prefix + ".connect_timeout_ms", local_settings.s3_connect_timeout_ms.value);
    client_configuration.requestTimeoutMs = config.getUInt64(config_prefix + ".request_timeout_ms", local_settings.s3_request_timeout_ms.value);
    client_configuration.maxConnections = config.getUInt(config_prefix + ".max_connections", static_cast<unsigned>(request_settings.max_connections));
    client_configuration.http_keep_alive_timeout = config.getUInt(config_prefix + ".http_keep_alive_timeout", S3::DEFAULT_KEEP_ALIVE_TIMEOUT);
    client_configuration.http_keep_alive_max_requests = config.getUInt(config_prefix + ".http_keep_alive_max_requests", S3::DEFAULT_KEEP_ALIVE_MAX_REQUESTS);

    client_configuration.endpointOverride = url.endpoint;
    client_configuration.s3_use_adaptive_timeouts = config.getBool(
        config_prefix + ".use_adaptive_timeouts", client_configuration.s3_use_adaptive_timeouts);

    if (for_disk_s3)
    {
        /*
        * Override proxy configuration for backwards compatibility with old configuration format.
        * */
        if (auto proxy_config = DB::ProxyConfigurationResolverProvider::getFromOldSettingsFormat(
                ProxyConfiguration::protocolFromString(url.uri.getScheme()), config_prefix, config))
        {
            client_configuration.per_request_configuration
                = [proxy_config]() { return proxy_config->resolve(); };
            client_configuration.error_report
                = [proxy_config](const auto & request_config) { proxy_config->errorReport(request_config); };
        }
    }

    S3::ServerSideEncryptionKMSConfig sse_kms_config = S3::getSSEKMSConfig(config_prefix, config);
    S3::ClientSettings client_settings{
        .use_virtual_addressing = url.is_virtual_hosted_style,
        .disable_checksum = local_settings.s3_disable_checksum,
        .gcs_issue_compose_request = config.getBool("s3.gcs_issue_compose_request", false),
        .is_s3express_bucket = is_s3_express_bucket,
    };

    auto credentials_configuration = S3::CredentialsConfiguration
    {
        auth_settings.use_environment_credentials.value_or(context->getConfigRef().getBool("s3.use_environment_credentials", true)),
        auth_settings.use_insecure_imds_request.value_or(context->getConfigRef().getBool("s3.use_insecure_imds_request", false)),
        auth_settings.expiration_window_seconds.value_or(context->getConfigRef().getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
        auth_settings.no_sign_request.value_or(context->getConfigRef().getBool("s3.no_sign_request", false)),
    };

    return S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        auth_settings.access_key_id,
        auth_settings.secret_access_key,
        auth_settings.server_side_encryption_customer_key_base64,
        std::move(sse_kms_config),
        auth_settings.headers,
        credentials_configuration,
        auth_settings.session_token);
}

}

#endif
