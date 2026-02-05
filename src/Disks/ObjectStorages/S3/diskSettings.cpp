#include <Disks/ObjectStorages/S3/diskSettings.h>

#if USE_AWS_S3

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/Macros.h>
#include <Common/Throttler.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
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
    extern const SettingsBool s3_slow_all_threads_after_network_error;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 s3_max_redirects;
    extern const ServerSettingsUInt64 s3_retry_attempts;
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

    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString role_session_name;
    extern const S3AuthSettingsString http_client;
    extern const S3AuthSettingsString service_account;
    extern const S3AuthSettingsString metadata_service;
    extern const S3AuthSettingsString request_token_path;
}

namespace S3RequestSetting
{
    extern const S3RequestSettingsUInt64 max_redirects;
    extern const S3RequestSettingsUInt64 retry_attempts;
    extern const S3RequestSettingsUInt64 retry_initial_delay_ms;
    extern const S3RequestSettingsUInt64 retry_max_delay_ms;
    extern const S3RequestSettingsBool slow_all_threads_after_network_error;
    extern const S3RequestSettingsBool enable_request_logging;
}

namespace ErrorCodes
{
extern const int NO_ELEMENTS_IN_CONFIG;
}

std::unique_ptr<S3::Client> getClient(
    const std::string & endpoint,
    const S3Settings & settings,
    ContextPtr context,
    bool for_disk_s3,
    std::optional<std::string> opt_disk_name)

{
    auto url = S3::URI(endpoint);
    if (!url.key.ends_with('/'))
        url.key.push_back('/');
    return getClient(url, settings, context, for_disk_s3, opt_disk_name);
}

std::unique_ptr<S3::Client>
getClient(const S3::URI & url, const S3Settings & settings, ContextPtr context, bool for_disk_s3, std::optional<std::string> opt_disk_name)
{
    const auto & auth_settings = settings.auth_settings;
    const auto & server_settings = context->getGlobalContext()->getServerSettings();
    const auto & request_settings = settings.request_settings;

    const bool is_s3_express_bucket = S3::isS3ExpressEndpoint(url.endpoint);
    if (is_s3_express_bucket && auth_settings[S3AuthSetting::region].value.empty())
    {
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Region should be explicitly specified for directory buckets");
    }

    const Settings & local_settings = context->getSettingsRef();

    unsigned int s3_max_redirects = static_cast<unsigned int>(server_settings[ServerSetting::s3_max_redirects]);
    if (request_settings[S3RequestSetting::max_redirects].changed)
        s3_max_redirects = static_cast<unsigned int>(request_settings[S3RequestSetting::max_redirects]);
    else
        // just for compatibility with old setting
        if (!for_disk_s3 && local_settings.isChanged("s3_max_redirects"))
            s3_max_redirects = static_cast<unsigned int>(local_settings[Setting::s3_max_redirects]);

    unsigned int s3_retry_attempts = static_cast<unsigned int>(server_settings[ServerSetting::s3_retry_attempts]);
    if (request_settings[S3RequestSetting::retry_attempts].changed)
        s3_retry_attempts = static_cast<unsigned int>(request_settings[S3RequestSetting::retry_attempts]);
    else
        // just for compatibility with old setting
        if (!for_disk_s3 && local_settings.isChanged("s3_retry_attempts"))
            s3_retry_attempts = static_cast<unsigned int>(local_settings[Setting::s3_retry_attempts]);

    bool s3_slow_all_threads_after_network_error = request_settings[S3RequestSetting::slow_all_threads_after_network_error];
    if (!for_disk_s3 && local_settings.isChanged("s3_slow_all_threads_after_network_error"))
        s3_slow_all_threads_after_network_error = local_settings[Setting::s3_slow_all_threads_after_network_error];

    bool enable_s3_requests_logging = request_settings[S3RequestSetting::enable_request_logging];
    if (!for_disk_s3 && local_settings.isChanged("enable_s3_requests_logging"))
        enable_s3_requests_logging = local_settings[Setting::enable_s3_requests_logging];

    S3::PocoHTTPClientConfiguration::RetryStrategy retry_strategy;
    retry_strategy.max_retries = s3_retry_attempts;
    retry_strategy.initial_delay_ms = static_cast<unsigned int>(request_settings[S3RequestSetting::retry_initial_delay_ms]);
    retry_strategy.max_delay_ms = static_cast<unsigned int>(request_settings[S3RequestSetting::retry_max_delay_ms]);

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        auth_settings[S3AuthSetting::region],
        context->getRemoteHostFilter(),
        s3_max_redirects,
        retry_strategy,
        s3_slow_all_threads_after_network_error,
        /* s3_slow_all_threads_after_retryable_error = */ false,
        enable_s3_requests_logging,
        for_disk_s3,
        opt_disk_name,
        request_settings.request_throttler,
        url.uri.getScheme());

    client_configuration.connectTimeoutMs = auth_settings[S3AuthSetting::connect_timeout_ms];
    client_configuration.requestTimeoutMs = auth_settings[S3AuthSetting::request_timeout_ms];
    client_configuration.maxConnections = static_cast<uint32_t>(auth_settings[S3AuthSetting::max_connections]);
    client_configuration.http_keep_alive_timeout = auth_settings[S3AuthSetting::http_keep_alive_timeout];
    client_configuration.http_keep_alive_max_requests = auth_settings[S3AuthSetting::http_keep_alive_max_requests];

    client_configuration.http_client = auth_settings[S3AuthSetting::http_client];
    client_configuration.service_account = auth_settings[S3AuthSetting::service_account];
    client_configuration.metadata_service = auth_settings[S3AuthSetting::metadata_service];
    client_configuration.request_token_path = auth_settings[S3AuthSetting::request_token_path];

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
        .is_s3express_bucket = is_s3_express_bucket
    };

    auto credentials_configuration = S3::CredentialsConfiguration
    {
        auth_settings[S3AuthSetting::use_environment_credentials],
        auth_settings[S3AuthSetting::use_insecure_imds_request],
        auth_settings[S3AuthSetting::expiration_window_seconds],
        auth_settings[S3AuthSetting::no_sign_request],
        auth_settings[S3AuthSetting::role_arn],
        auth_settings[S3AuthSetting::role_session_name],
        /*sts_endpoint_override=*/""
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
