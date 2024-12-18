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

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsUInt64 connect_timeout_ms;
    extern const S3AuthSettingsBool disable_checksum;
    extern const S3AuthSettingsUInt64 expiration_window_seconds;
    extern const S3AuthSettingsBool gcs_issue_compose_request;
    extern const S3AuthSettingsUInt64 http_keep_alive_max_requests;
    extern const S3AuthSettingsUInt64 http_keep_alive_timeout;
    extern const S3AuthSettingsUInt64 max_connections;
    extern const S3AuthSettingsBool no_sign_request;
    extern const S3AuthSettingsString region;
    extern const S3AuthSettingsUInt64 request_timeout_ms;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString server_side_encryption_customer_key_base64;
    extern const S3AuthSettingsString session_token;
    extern const S3AuthSettingsBool use_adaptive_timeouts;
    extern const S3AuthSettingsBool use_environment_credentials;
    extern const S3AuthSettingsBool use_insecure_imds_request;
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

    auto auth_settings = S3::S3AuthSettings(config, settings, config_prefix);
    auto request_settings = S3::S3RequestSettings(config, settings, config_prefix, "s3_", validate_settings);

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
    if (is_s3_express_bucket && auth_settings[S3AuthSetting::region].value.empty())
    {
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Region should be explicitly specified for directory buckets");
    }

    const Settings & local_settings = context->getSettingsRef();

    int s3_max_redirects = static_cast<int>(global_settings[Setting::s3_max_redirects]);
    if (!for_disk_s3 && local_settings.isChanged("s3_max_redirects"))
        s3_max_redirects = static_cast<int>(local_settings[Setting::s3_max_redirects]);

    int s3_retry_attempts = static_cast<int>(global_settings[Setting::s3_retry_attempts]);
    if (!for_disk_s3 && local_settings.isChanged("s3_retry_attempts"))
        s3_retry_attempts = static_cast<int>(local_settings[Setting::s3_retry_attempts]);

    bool enable_s3_requests_logging = global_settings[Setting::enable_s3_requests_logging];
    if (!for_disk_s3 && local_settings.isChanged("enable_s3_requests_logging"))
        enable_s3_requests_logging = local_settings[Setting::enable_s3_requests_logging];

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        auth_settings[S3AuthSetting::region],
        context->getRemoteHostFilter(),
        s3_max_redirects,
        s3_retry_attempts,
        enable_s3_requests_logging,
        for_disk_s3,
        request_settings.get_request_throttler,
        request_settings.put_request_throttler,
        url.uri.getScheme());

    client_configuration.connectTimeoutMs = auth_settings[S3AuthSetting::connect_timeout_ms];
    client_configuration.requestTimeoutMs = auth_settings[S3AuthSetting::request_timeout_ms];
    client_configuration.maxConnections = static_cast<uint32_t>(auth_settings[S3AuthSetting::max_connections]);
    client_configuration.http_keep_alive_timeout = auth_settings[S3AuthSetting::http_keep_alive_timeout];
    client_configuration.http_keep_alive_max_requests = auth_settings[S3AuthSetting::http_keep_alive_max_requests];

    client_configuration.endpointOverride = url.endpoint;
    client_configuration.s3_use_adaptive_timeouts = auth_settings[S3AuthSetting::use_adaptive_timeouts];

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
        .disable_checksum = auth_settings[S3AuthSetting::disable_checksum],
        .gcs_issue_compose_request = auth_settings[S3AuthSetting::gcs_issue_compose_request],
    };

    auto credentials_configuration = S3::CredentialsConfiguration
    {
        auth_settings[S3AuthSetting::use_environment_credentials],
        auth_settings[S3AuthSetting::use_insecure_imds_request],
        auth_settings[S3AuthSetting::expiration_window_seconds],
        auth_settings[S3AuthSetting::no_sign_request],
    };

    return S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        auth_settings[S3AuthSetting::access_key_id],
        auth_settings[S3AuthSetting::secret_access_key],
        auth_settings[S3AuthSetting::server_side_encryption_customer_key_base64],
        auth_settings.server_side_encryption_kms_config,
        auth_settings.getHeaders(),
        credentials_configuration,
        auth_settings[S3AuthSetting::session_token]);
}

}

#endif
