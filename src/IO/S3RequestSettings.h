#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>

namespace Poco::Util
{
class AbstractConfiguration;
};

namespace DB
{
class NamedCollection;
struct ProxyConfigurationResolver;
struct S3RequestSettingsImpl;
struct Settings;

/// List of available types supported in MaterializedMySQLSettings object
#define S3REQUEST_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, String)

S3REQUEST_SETTINGS_SUPPORTED_TYPES(S3RequestSettings, DECLARE_SETTING_TRAIT)

namespace S3
{

/// We use s3 settings for DiskS3, StorageS3 (StorageS3Cluster, S3Queue, etc), BackupIO_S3, etc.
/// 1. For DiskS3 we usually have configuration in disk section in configuration file.
///    REQUEST_SETTINGS, PART_UPLOAD_SETTINGS start with "s3_" prefix there, while AUTH_SETTINGS and CLIENT_SETTINGS do not
///    (does not make sense, but it happened this way).
///    If some setting is absent from disk configuration, we look up for it in the "s3." server config section,
///    where s3 settings no longer have "s3_" prefix like in disk configuration section.
///    If the settings is absent there as well, we look up for it in Users config (where query/session settings are also updated).
/// 2. For StorageS3 and similar - we look up to "s3." config section (again - settings there do not have "s3_" prefix).
///    If some setting is absent from there, we look up for it in Users config.

struct S3RequestSettings
{
    S3RequestSettings();
    S3RequestSettings(const S3RequestSettings & settings);
    S3RequestSettings(S3RequestSettings && settings) noexcept;

    /// Create request settings from Config.
    S3RequestSettings(
        const Poco::Util::AbstractConfiguration & config,
        const DB::Settings & settings,
        const std::string & config_prefix,
        const std::string & setting_name_prefix = "",
        bool validate_settings = true);

    /// Create request settings from DB::Settings.
    explicit S3RequestSettings(const DB::Settings & settings, bool validate_settings = true);

    /// Create request settings from NamedCollection.
    S3RequestSettings(const NamedCollection & collection, const DB::Settings & settings, bool validate_settings = true);

    ~S3RequestSettings();

    S3RequestSettings & operator=(S3RequestSettings && settings) noexcept;

    S3REQUEST_SETTINGS_SUPPORTED_TYPES(S3RequestSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void updateFromSettings(const DB::Settings & settings, bool if_changed, bool validate_settings = true);
    void updateIfChanged(const S3RequestSettings & settings);
    void validateUploadSettings();

    ThrottlerPtr get_request_throttler;
    ThrottlerPtr put_request_throttler;
    std::shared_ptr<ProxyConfigurationResolver> proxy_resolver;

private:
    void finishInit(const DB::Settings & settings, bool validate_settings);
    void normalizeSettings();

    std::unique_ptr<S3RequestSettingsImpl> impl;
};

}

}
