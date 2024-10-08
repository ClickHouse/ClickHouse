#include <Disks/ObjectStorages/S3/diskSettings.h>

#if USE_AWS_S3

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/Macros.h>
#include <Common/Throttler.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Disks/DiskFactory.h>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <base/getFQDNOrHostName.h>
#include <IO/S3Common.h>
#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>

#include <IO/S3Settings.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskLocal.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_s3_requests_logging;
    extern const SettingsUInt64 s3_max_redirects;
    extern const SettingsUInt64 s3_retry_attempts;
}

namespace ErrorCodes
{
extern const int NO_ELEMENTS_IN_CONFIG;
}

std::unique_ptr<S3ObjectStorageSettings> getSettings(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const std::string & endpoint,
    bool validate_settings)
{
    const auto & settings = context->getSettingsRef();

    auto auth_settings = S3::AuthSettings(config, settings, config_prefix);
    auto request_settings = S3::RequestSettings(config, settings, config_prefix, "s3_", validate_settings);

    request_settings.proxy_resolver = DB::ProxyConfigurationResolverProvider::getFromOldSettingsFormat(
        ProxyConfiguration::protocolFromString(S3::URI(endpoint).uri.getScheme()), config_prefix, config);

    return std::make_unique<S3ObjectStorageSettings>(
        request_settings,
        auth_settings,
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".list_object_keys_size", 1000),
        config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000),
        config.getBool(config_prefix + ".readonly", false));
}

std::unique_ptr<S3::Client> getClient(
    const std::string & endpoint,
    const S3ObjectStorageSettings & settings,
    ContextPtr context,
    bool for_disk_s3)
{
    auto url = S3::URI(endpoint);
    if (!url.key.ends_with('/'))
        url.key.push_back('/');
    return getClient(url, settings, context, for_disk_s3);
}

std::unique_ptr<S3::Client> getClient(
    const S3::URI & url,
    const S3ObjectStorageSettings & settings,
    ContextPtr context,
    bool for_disk_s3)
{
    const Settings & global_settings = context->getGlobalContext()->getSettingsRef();
    const auto & auth_settings = settings.auth_settings;
    const auto & request_settings = settings.request_settings;

    const bool is_s3_express_bucket = S3::isS3ExpressEndpoint(url.endpoint);
    if (is_s3_express_bucket && auth_settings.region.value.empty())
    {
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Region should be explicitly specified for directory buckets");
    }

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        auth_settings.region,
        context->getRemoteHostFilter(),
        static_cast<int>(global_settings[Setting::s3_max_redirects]),
        static_cast<int>(global_settings[Setting::s3_retry_attempts]),
        global_settings[Setting::enable_s3_requests_logging],
        for_disk_s3,
        request_settings.get_request_throttler,
        request_settings.put_request_throttler,
        url.uri.getScheme());

    client_configuration.connectTimeoutMs = auth_settings.connect_timeout_ms;
    client_configuration.requestTimeoutMs = auth_settings.request_timeout_ms;
    client_configuration.maxConnections = static_cast<uint32_t>(auth_settings.max_connections);
    client_configuration.http_keep_alive_timeout = auth_settings.http_keep_alive_timeout;
    client_configuration.http_keep_alive_max_requests = auth_settings.http_keep_alive_max_requests;

    client_configuration.endpointOverride = url.endpoint;
    client_configuration.s3_use_adaptive_timeouts = auth_settings.use_adaptive_timeouts;

    if (request_settings.proxy_resolver)
    {
        /*
        * Override proxy configuration for backwards compatibility with old configuration format.
        * */
        client_configuration.per_request_configuration = [=]() { return request_settings.proxy_resolver->resolve(); };
        client_configuration.error_report = [=](const auto & request_config) { request_settings.proxy_resolver->errorReport(request_config); };
    }

    S3::ClientSettings client_settings{
        .use_virtual_addressing = url.is_virtual_hosted_style,
        .disable_checksum = auth_settings.disable_checksum,
        .gcs_issue_compose_request = auth_settings.gcs_issue_compose_request,
    };

    auto credentials_configuration = S3::CredentialsConfiguration
    {
        auth_settings.use_environment_credentials,
        auth_settings.use_insecure_imds_request,
        auth_settings.expiration_window_seconds,
        auth_settings.no_sign_request,
    };

    return S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        auth_settings.access_key_id,
        auth_settings.secret_access_key,
        auth_settings.server_side_encryption_customer_key_base64,
        auth_settings.server_side_encryption_kms_config,
        auth_settings.headers,
        credentials_configuration,
        auth_settings.session_token);
}

}

#endif
