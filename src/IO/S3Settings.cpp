#include <IO/S3Settings.h>

#include <Core/Settings.h>
#include <IO/S3Common.h>
#include <Interpreters/Context.h>

#include <Common/ProxyConfigurationResolverProvider.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool s3_validate_request_settings;
}

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString google_adc_client_id;
    extern const S3AuthSettingsString google_adc_client_secret;
    extern const S3AuthSettingsString google_adc_refresh_token;
    extern const S3AuthSettingsString http_client;
    extern const S3AuthSettingsString metadata_service;
    extern const S3AuthSettingsString request_token_path;
    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString role_session_name;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString server_side_encryption_customer_key_base64;
    extern const S3AuthSettingsString service_account;
    extern const S3AuthSettingsString session_token;
    extern const S3AuthSettingsBool no_sign_request;
    extern const S3AuthSettingsBool use_environment_credentials;
    extern const S3AuthSettingsBool use_insecure_imds_request;
}

namespace S3RequestSetting
{
    extern const S3RequestSettingsBool read_only;
    extern const S3RequestSettingsUInt64 min_bytes_for_seek;
    extern const S3RequestSettingsUInt64 list_object_keys_size;
    extern const S3RequestSettingsUInt64 objects_chunk_size_to_delete;
}

namespace
{

bool pathStartsWithBoundary(const String & path, const String & prefix)
{
    if (prefix.empty() || prefix == "/")
        return true;

    if (!path.starts_with(prefix))
        return false;

    return path.size() == prefix.size() || prefix.ends_with("/") || path[prefix.size()] == '/';
}

bool endpointMatches(const Poco::URI & endpoint, const String & endpoint_prefix)
{
    Poco::URI prefix(endpoint_prefix);
    return endpoint.getScheme() == prefix.getScheme()
        && endpoint.getAuthority() == prefix.getAuthority()
        && pathStartsWithBoundary(endpoint.getPath(), prefix.getPath());
}

}


void S3Settings::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DB::Settings & settings)
{
    auth_settings = S3::S3AuthSettings(config, settings, config_prefix);
    request_settings = S3::S3RequestSettings(config, settings, config_prefix);
}

void S3Settings::loadFromConfigForObjectStorage(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DB::Settings & settings,
    const std::string & scheme,
    bool validate_settings)
{
    auth_settings = S3::S3AuthSettings(config, settings, config_prefix);
    request_settings = S3::S3RequestSettings(config, settings, config_prefix, "s3_", validate_settings);

    request_settings.proxy_resolver = DB::ProxyConfigurationResolverProvider::getFromOldSettingsFormat(
        ProxyConfiguration::protocolFromString(scheme), config_prefix, config);

    /// We override these request settings from configuration, because they are related to disk configuration,
    /// which shouldn't be changed from user query
    request_settings[S3RequestSetting::read_only] = config.getBool(config_prefix + ".readonly", false);
    request_settings[S3RequestSetting::min_bytes_for_seek] = config.getUInt64(config_prefix + ".min_bytes_for_seek", S3::DEFAULT_MIN_BYTES_FOR_SEEK);
    request_settings[S3RequestSetting::list_object_keys_size] = config.getUInt64(config_prefix + ".list_object_keys_size", S3::DEFAULT_LIST_OBJECT_KEYS_SIZE);
    request_settings[S3RequestSetting::objects_chunk_size_to_delete] = config.getUInt(config_prefix + ".objects_chunk_size_to_delete", S3::DEFAULT_OBJECTS_CHUNK_SIZE_TO_DELETE);
}

void S3Settings::resetCredentialsForUserControlledRequest()
{
    auth_settings[S3AuthSetting::access_key_id] = "";
    auth_settings[S3AuthSetting::secret_access_key] = "";
    auth_settings[S3AuthSetting::session_token] = "";
    auth_settings[S3AuthSetting::server_side_encryption_customer_key_base64] = "";
    auth_settings[S3AuthSetting::no_sign_request] = false;
    auth_settings[S3AuthSetting::use_environment_credentials] = false;
    auth_settings[S3AuthSetting::use_insecure_imds_request] = false;
    auth_settings[S3AuthSetting::role_arn] = "";
    auth_settings[S3AuthSetting::role_session_name] = "";
    auth_settings[S3AuthSetting::http_client] = "";
    auth_settings[S3AuthSetting::service_account] = "";
    auth_settings[S3AuthSetting::metadata_service] = "";
    auth_settings[S3AuthSetting::request_token_path] = "";
    auth_settings[S3AuthSetting::google_adc_client_id] = "";
    auth_settings[S3AuthSetting::google_adc_client_secret] = "";
    auth_settings[S3AuthSetting::google_adc_refresh_token] = "";
    auth_settings.headers.clear();
    auth_settings.access_headers.clear();
    auth_settings.server_side_encryption_kms_config = {};
}

void S3Settings::copyCredentialsFrom(const S3Settings & settings)
{
    auth_settings[S3AuthSetting::access_key_id] = settings.auth_settings[S3AuthSetting::access_key_id];
    auth_settings[S3AuthSetting::secret_access_key] = settings.auth_settings[S3AuthSetting::secret_access_key];
    auth_settings[S3AuthSetting::session_token] = settings.auth_settings[S3AuthSetting::session_token];
    auth_settings[S3AuthSetting::no_sign_request] = settings.auth_settings[S3AuthSetting::no_sign_request];
    auth_settings[S3AuthSetting::use_environment_credentials] = settings.auth_settings[S3AuthSetting::use_environment_credentials];
    auth_settings[S3AuthSetting::use_insecure_imds_request] = settings.auth_settings[S3AuthSetting::use_insecure_imds_request];
    auth_settings[S3AuthSetting::role_arn] = settings.auth_settings[S3AuthSetting::role_arn];
    auth_settings[S3AuthSetting::role_session_name] = settings.auth_settings[S3AuthSetting::role_session_name];
    auth_settings[S3AuthSetting::http_client] = settings.auth_settings[S3AuthSetting::http_client];
    auth_settings[S3AuthSetting::service_account] = settings.auth_settings[S3AuthSetting::service_account];
    auth_settings[S3AuthSetting::metadata_service] = settings.auth_settings[S3AuthSetting::metadata_service];
    auth_settings[S3AuthSetting::request_token_path] = settings.auth_settings[S3AuthSetting::request_token_path];
    auth_settings[S3AuthSetting::google_adc_client_id] = settings.auth_settings[S3AuthSetting::google_adc_client_id];
    auth_settings[S3AuthSetting::google_adc_client_secret] = settings.auth_settings[S3AuthSetting::google_adc_client_secret];
    auth_settings[S3AuthSetting::google_adc_refresh_token] = settings.auth_settings[S3AuthSetting::google_adc_refresh_token];
    auth_settings[S3AuthSetting::server_side_encryption_customer_key_base64]
        = settings.auth_settings[S3AuthSetting::server_side_encryption_customer_key_base64];
    auth_settings.headers = settings.auth_settings.headers;
    auth_settings.access_headers = settings.auth_settings.access_headers;
    auth_settings.server_side_encryption_kms_config = settings.auth_settings.server_side_encryption_kms_config;
}


void S3Settings::updateIfChanged(const S3Settings & settings)
{
    auth_settings.updateIfChanged(settings.auth_settings);
    request_settings.updateIfChanged(settings.request_settings);
}

void S3SettingsByEndpoint::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DB::Settings & settings)
{
    std::lock_guard lock(mutex);
    s3_settings.clear();
    if (!config.has(config_prefix))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);
    for (const String & key : config_keys)
    {
        const auto key_path = config_prefix + "." + key;
        const auto endpoint_path = key_path + ".endpoint";
        if (config.has(endpoint_path))
        {
            s3_settings.emplace(
                config.getString(endpoint_path),
                S3Settings{
                    S3::S3AuthSettings(config, settings, key_path),
                    S3::S3RequestSettings(config, settings, key_path, "", settings[Setting::s3_validate_request_settings])});
        }
    }
}

std::optional<S3Settings> S3SettingsByEndpoint::getSettings(
    const String & endpoint,
    const String & user,
    bool ignore_user) const
{
    std::lock_guard lock(mutex);
    Poco::URI endpoint_uri(endpoint);

    const S3Settings * matched_settings = nullptr;
    size_t matched_prefix_size = 0;

    for (const auto & [endpoint_prefix, settings] : s3_settings)
    {
        if (endpoint_prefix.size() > matched_prefix_size
            && endpointMatches(endpoint_uri, endpoint_prefix)
            && (ignore_user || settings.auth_settings.canBeUsedByUser(user)))
        {
            matched_settings = &settings;
            matched_prefix_size = endpoint_prefix.size();
        }
    }

    if (matched_settings)
        return *matched_settings;

    return {};
}

std::optional<String> S3SettingsByEndpoint::getMatchedEndpoint(
    const String & endpoint,
    const String & user,
    bool ignore_user) const
{
    std::lock_guard lock(mutex);
    Poco::URI endpoint_uri(endpoint);

    const String * matched_endpoint = nullptr;
    size_t matched_prefix_size = 0;

    for (const auto & [endpoint_prefix, settings] : s3_settings)
    {
        if (endpoint_prefix.size() > matched_prefix_size
            && endpointMatches(endpoint_uri, endpoint_prefix)
            && (ignore_user || settings.auth_settings.canBeUsedByUser(user)))
        {
            matched_endpoint = &endpoint_prefix;
            matched_prefix_size = endpoint_prefix.size();
        }
    }

    if (matched_endpoint)
        return *matched_endpoint;

    return {};
}

void S3Settings::serialize(WriteBuffer & os, ContextPtr context) const
{
    auth_settings.serialize(os, context);
    request_settings.serialize(os, context);
}

S3Settings S3Settings::deserialize(ReadBuffer & is, ContextPtr context)
{
    S3Settings result;
    result.auth_settings = S3::S3AuthSettings::deserialize(is, context);
    result.request_settings = S3::S3RequestSettings::deserialize(is, context);
    return result;
}

}
