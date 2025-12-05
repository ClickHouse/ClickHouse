#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/Settings.h>
#include <IO/S3AuthSettings.h>
#include <IO/S3Defines.h>
#include <IO/S3Common.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

#define CLIENT_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, connect_timeout_ms, S3::DEFAULT_CONNECT_TIMEOUT_MS, "", 0) \
    DECLARE(UInt64, request_timeout_ms, S3::DEFAULT_REQUEST_TIMEOUT_MS, "", 0) \
    DECLARE(UInt64, max_connections, S3::DEFAULT_MAX_CONNECTIONS, "", 0) \
    DECLARE(UInt64, http_keep_alive_timeout, S3::DEFAULT_KEEP_ALIVE_TIMEOUT, "", 0) \
    DECLARE(UInt64, http_keep_alive_max_requests, S3::DEFAULT_KEEP_ALIVE_MAX_REQUESTS, "", 0) \
    DECLARE(UInt64, expiration_window_seconds, S3::DEFAULT_EXPIRATION_WINDOW_SECONDS, "", 0) \
    DECLARE(Bool, use_environment_credentials, S3::DEFAULT_USE_ENVIRONMENT_CREDENTIALS, "", 0) \
    DECLARE(Bool, no_sign_request, S3::DEFAULT_NO_SIGN_REQUEST, "", 0) \
    DECLARE(Bool, use_insecure_imds_request, false, "", 0) \
    DECLARE(Bool, use_adaptive_timeouts, S3::DEFAULT_USE_ADAPTIVE_TIMEOUTS, "", 0) \
    DECLARE(Bool, is_virtual_hosted_style, false, "", 0) \
    DECLARE(Bool, disable_checksum, S3::DEFAULT_DISABLE_CHECKSUM, "", 0) \
    DECLARE(Bool, gcs_issue_compose_request, false, "", 0)

#define AUTH_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, access_key_id, "", "", 0) \
    DECLARE(String, secret_access_key, "", "", 0) \
    DECLARE(String, session_token, "", "", 0) \
    DECLARE(String, region, "", "", 0) \
    DECLARE(String, server_side_encryption_customer_key_base64, "", "", 0)

#define CLIENT_SETTINGS_LIST(M, ALIAS) \
    CLIENT_SETTINGS(M, ALIAS) \
    AUTH_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(S3AuthSettingsTraits, CLIENT_SETTINGS_LIST)
IMPLEMENT_SETTINGS_TRAITS(S3AuthSettingsTraits, CLIENT_SETTINGS_LIST)

struct S3AuthSettingsImpl : public BaseSettings<S3AuthSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) S3AuthSettings##TYPE NAME = &S3AuthSettingsImpl ::NAME;

namespace S3AuthSetting
{
CLIENT_SETTINGS_LIST(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

namespace S3
{

namespace
{
bool setValueFromConfig(
    const Poco::Util::AbstractConfiguration & config, const std::string & path, typename S3AuthSettingsImpl::SettingFieldRef & field)
{
    if (!config.has(path))
        return false;

    auto which = field.getValue().getType();
    if (isInt64OrUInt64FieldType(which))
        field.setValue(config.getUInt64(path));
    else if (which == Field::Types::String)
        field.setValue(config.getString(path));
    else if (which == Field::Types::Bool)
        field.setValue(config.getBool(path));
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type: {}", field.getTypeName());

    return true;
}
}


S3AuthSettings::S3AuthSettings() : impl(std::make_unique<S3AuthSettingsImpl>())
{
}

S3AuthSettings::S3AuthSettings(
    const Poco::Util::AbstractConfiguration & config, const DB::Settings & settings, const std::string & config_prefix)
    : S3AuthSettings()
{
    for (auto & field : impl->allMutable())
    {
        auto path = fmt::format("{}.{}", config_prefix, field.getName());

        bool updated = setValueFromConfig(config, path, field);
        if (!updated)
        {
            auto setting_name = "s3_" + field.getName();
            if (settings.has(setting_name) && settings.isChanged(setting_name))
                field.setValue(settings.get(setting_name));
        }
    }

    headers = getHTTPHeaders(config_prefix, config, "header");
    access_headers = getHTTPHeaders(config_prefix, config, "access_header");

    server_side_encryption_kms_config = getSSEKMSConfig(config_prefix, config);

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
    {
        if (startsWith(key, "user"))
            users.insert(config.getString(config_prefix + "." + key));
    }
}

S3AuthSettings::S3AuthSettings(const S3AuthSettings & settings)
    : headers(settings.headers)
    , access_headers(settings.access_headers)
    , users(settings.users)
    , server_side_encryption_kms_config(settings.server_side_encryption_kms_config)
    , impl(std::make_unique<S3AuthSettingsImpl>(*settings.impl))
{
}

S3AuthSettings::S3AuthSettings(S3AuthSettings && settings) noexcept
    : headers(std::move(settings.headers))
    , access_headers(std::move(settings.access_headers))
    , users(std::move(settings.users))
    , server_side_encryption_kms_config(std::move(settings.server_side_encryption_kms_config))
    , impl(std::make_unique<S3AuthSettingsImpl>(std::move(*settings.impl)))
{
}

S3AuthSettings::S3AuthSettings(const DB::Settings & settings) : impl(std::make_unique<S3AuthSettingsImpl>())
{
    updateFromSettings(settings, /* if_changed */ false);
}

S3AuthSettings::~S3AuthSettings() = default;

S3AUTH_SETTINGS_SUPPORTED_TYPES(S3AuthSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

S3AuthSettings & S3AuthSettings::operator=(S3AuthSettings && settings) noexcept
{
    headers = std::move(settings.headers);
    access_headers = std::move(settings.access_headers);
    users = std::move(settings.users);
    server_side_encryption_kms_config = std::move(settings.server_side_encryption_kms_config);
    *impl = std::move(*settings.impl);

    return *this;
}

bool S3AuthSettings::operator==(const S3AuthSettings & right)
{
    if (headers != right.headers)
        return false;

    if (access_headers != right.access_headers)
        return false;

    if (users != right.users)
        return false;

    if (server_side_encryption_kms_config != right.server_side_encryption_kms_config)
        return false;

    return *impl == *right.impl;
}

void S3AuthSettings::updateFromSettings(const DB::Settings & settings, bool if_changed)
{
    for (auto & field : impl->allMutable())
    {
        const auto setting_name = "s3_" + field.getName();
        if (settings.has(setting_name) && (!if_changed || settings.isChanged(setting_name)))
        {
            field.setValue(settings.get(setting_name));
        }
    }
}

bool S3AuthSettings::hasUpdates(const S3AuthSettings & other) const
{
    S3AuthSettings copy{*this};
    copy.updateIfChanged(other);
    return *this != copy;
}

void S3AuthSettings::updateIfChanged(const S3AuthSettings & settings)
{
    for (auto & setting : settings.impl->all())
    {
        if (setting.isValueChanged())
            impl->set(setting.getName(), setting.getValue());
    }

    if (!settings.headers.empty())
        headers = settings.headers;

    if (!settings.access_headers.empty())
        access_headers = settings.access_headers;

    if (!settings.users.empty())
        users.insert(settings.users.begin(), settings.users.end());

    if (settings.server_side_encryption_kms_config.key_id.has_value()
        || settings.server_side_encryption_kms_config.encryption_context.has_value()
        || settings.server_side_encryption_kms_config.key_id.has_value())
        server_side_encryption_kms_config = settings.server_side_encryption_kms_config;
}

HTTPHeaderEntries S3AuthSettings::getHeaders() const
{
    bool auth_settings_is_default = !impl->isChanged("access_key_id");
    if (access_headers.empty() || !auth_settings_is_default)
        return headers;

    HTTPHeaderEntries result(headers);
    result.insert(result.end(), access_headers.begin(), access_headers.end());

    return result;
}

}
}
