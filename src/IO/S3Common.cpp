#include <IO/S3Common.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Core/Settings.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/String.h>

#include "config.h"

#if USE_AWS_S3

#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>


namespace ProfileEvents
{
    extern const Event S3GetObjectAttributes;
    extern const Event S3GetObjectMetadata;
    extern const Event S3HeadObject;
    extern const Event DiskS3GetObjectAttributes;
    extern const Event DiskS3GetObjectMetadata;
    extern const Event DiskS3HeadObject;
}

namespace DB
{

bool S3Exception::isRetryableError() const
{
    /// Looks like these list is quite conservative, add more codes if you wish
    static const std::unordered_set<Aws::S3::S3Errors> unretryable_errors = {
        Aws::S3::S3Errors::NO_SUCH_KEY,
        Aws::S3::S3Errors::ACCESS_DENIED,
        Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID,
        Aws::S3::S3Errors::INVALID_SIGNATURE,
        Aws::S3::S3Errors::NO_SUCH_UPLOAD,
        Aws::S3::S3Errors::NO_SUCH_BUCKET,
    };

    return !unretryable_errors.contains(code);
}

}

namespace DB::ErrorCodes
{
    extern const int S3_ERROR;
}

#endif

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 s3_max_get_burst;
    extern const SettingsUInt64 s3_max_get_rps;
    extern const SettingsUInt64 s3_max_put_burst;
    extern const SettingsUInt64 s3_max_put_rps;
}

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_SETTING_VALUE;
}

namespace S3
{

HTTPHeaderEntries getHTTPHeaders(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    HTTPHeaderEntries headers;
    Poco::Util::AbstractConfiguration::Keys subconfig_keys;
    config.keys(config_elem, subconfig_keys);
    for (const std::string & subkey : subconfig_keys)
    {
        if (subkey.starts_with("header"))
        {
            auto header_str = config.getString(config_elem + "." + subkey);
            auto delimiter = header_str.find(':');
            if (delimiter == std::string::npos)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Malformed s3 header value");
            headers.emplace_back(header_str.substr(0, delimiter), header_str.substr(delimiter + 1, String::npos));
        }
    }
    return headers;
}

ServerSideEncryptionKMSConfig getSSEKMSConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    ServerSideEncryptionKMSConfig sse_kms_config;

    if (config.has(config_elem + ".server_side_encryption_kms_key_id"))
        sse_kms_config.key_id = config.getString(config_elem + ".server_side_encryption_kms_key_id");

    if (config.has(config_elem + ".server_side_encryption_kms_encryption_context"))
        sse_kms_config.encryption_context = config.getString(config_elem + ".server_side_encryption_kms_encryption_context");

    if (config.has(config_elem + ".server_side_encryption_kms_bucket_key_enabled"))
        sse_kms_config.bucket_key_enabled = config.getBool(config_elem + ".server_side_encryption_kms_bucket_key_enabled");

    return sse_kms_config;
}

template <typename Settings>
static bool setValueFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    typename Settings::SettingFieldRef & field)
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

AuthSettings::AuthSettings(
    const Poco::Util::AbstractConfiguration & config,
    const DB::Settings & settings,
    const std::string & config_prefix)
{
    for (auto & field : allMutable())
    {
        auto path = fmt::format("{}.{}", config_prefix, field.getName());

        bool updated = setValueFromConfig<AuthSettings>(config, path, field);
        if (!updated)
        {
            auto setting_name = "s3_" + field.getName();
            if (settings.has(setting_name) && settings.isChanged(setting_name))
                field.setValue(settings.get(setting_name));
        }
    }

    headers = getHTTPHeaders(config_prefix, config);
    server_side_encryption_kms_config = getSSEKMSConfig(config_prefix, config);

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
    {
        if (startsWith(key, "user"))
            users.insert(config.getString(config_prefix + "." + key));
    }
}

AuthSettings::AuthSettings(const DB::Settings & settings)
{
    updateFromSettings(settings, /* if_changed */false);
}

void AuthSettings::updateFromSettings(const DB::Settings & settings, bool if_changed)
{
    for (auto & field : allMutable())
    {
        const auto setting_name = "s3_" + field.getName();
        if (settings.has(setting_name) && (!if_changed || settings.isChanged(setting_name)))
        {
            field.setValue(settings.get(setting_name));
        }
    }
}

bool AuthSettings::hasUpdates(const AuthSettings & other) const
{
    AuthSettings copy = *this;
    copy.updateIfChanged(other);
    return *this != copy;
}

void AuthSettings::updateIfChanged(const AuthSettings & settings)
{
    for (auto & setting : settings.all())
    {
        if (setting.isValueChanged())
            set(setting.getName(), setting.getValue());
    }

    if (!settings.headers.empty())
        headers = settings.headers;

    if (!settings.users.empty())
        users.insert(settings.users.begin(), settings.users.end());

    if (settings.server_side_encryption_kms_config.key_id.has_value()
        || settings.server_side_encryption_kms_config.encryption_context.has_value()
        || settings.server_side_encryption_kms_config.key_id.has_value())
        server_side_encryption_kms_config = settings.server_side_encryption_kms_config;
}

RequestSettings::RequestSettings(
    const Poco::Util::AbstractConfiguration & config,
    const DB::Settings & settings,
    const std::string & config_prefix,
    const std::string & setting_name_prefix,
    bool validate_settings)
{
    for (auto & field : allMutable())
    {
        auto path = fmt::format("{}.{}{}", config_prefix, setting_name_prefix, field.getName());

        bool updated = setValueFromConfig<RequestSettings>(config, path, field);
        if (!updated)
        {
            auto setting_name = "s3_" + field.getName();
            if (settings.has(setting_name) && settings.isChanged(setting_name))
                field.setValue(settings.get(setting_name));
        }
    }
    finishInit(settings, validate_settings);
}

RequestSettings::RequestSettings(
    const NamedCollection & collection,
    const DB::Settings & settings,
    bool validate_settings)
{
    auto values = allMutable();
    for (auto & field : values)
    {
        const auto path = field.getName();
        if (collection.has(path))
        {
            auto which = field.getValue().getType();
            if (isInt64OrUInt64FieldType(which))
                field.setValue(collection.get<UInt64>(path));
            else if (which == Field::Types::String)
                field.setValue(collection.get<String>(path));
            else if (which == Field::Types::Bool)
                field.setValue(collection.get<bool>(path));
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type: {}", field.getTypeName());
        }
    }
    finishInit(settings, validate_settings);
}

RequestSettings::RequestSettings(const DB::Settings & settings, bool validate_settings)
{
    updateFromSettings(settings, /* if_changed */false, validate_settings);
    finishInit(settings, validate_settings);
}

void RequestSettings::updateFromSettings(
    const DB::Settings & settings, bool if_changed, bool validate_settings)
{
    for (auto & field : allMutable())
    {
        const auto setting_name = "s3_" + field.getName();
        if (settings.has(setting_name) && (!if_changed || settings.isChanged(setting_name)))
        {
            set(field.getName(), settings.get(setting_name));
        }
    }

    normalizeSettings();
    if (validate_settings)
        validateUploadSettings();
}

void RequestSettings::updateIfChanged(const RequestSettings & settings)
{
    for (auto & setting : settings.all())
    {
        if (setting.isValueChanged())
            set(setting.getName(), setting.getValue());
    }
}

void RequestSettings::normalizeSettings()
{
    if (!storage_class_name.value.empty() && storage_class_name.changed)
        storage_class_name = Poco::toUpperInPlace(storage_class_name.value);
}

void RequestSettings::finishInit(const DB::Settings & settings, bool validate_settings)
{
    normalizeSettings();
    if (validate_settings)
        validateUploadSettings();

    /// NOTE: it would be better to reuse old throttlers
    /// to avoid losing token bucket state on every config reload,
    /// which could lead to exceeding limit for short time.
    /// But it is good enough unless very high `burst` values are used.
    if (UInt64 max_get_rps = isChanged("max_get_rps") ? get("max_get_rps").safeGet<UInt64>() : settings[Setting::s3_max_get_rps])
    {
        size_t default_max_get_burst
            = settings[Setting::s3_max_get_burst] ? settings[Setting::s3_max_get_burst] : (Throttler::default_burst_seconds * max_get_rps);

        size_t max_get_burst = isChanged("max_get_burts") ? get("max_get_burst").safeGet<UInt64>() : default_max_get_burst;
        get_request_throttler = std::make_shared<Throttler>(max_get_rps, max_get_burst);
    }
    if (UInt64 max_put_rps = isChanged("max_put_rps") ? get("max_put_rps").safeGet<UInt64>() : settings[Setting::s3_max_put_rps])
    {
        size_t default_max_put_burst
            = settings[Setting::s3_max_put_burst] ? settings[Setting::s3_max_put_burst] : (Throttler::default_burst_seconds * max_put_rps);
        size_t max_put_burst = isChanged("max_put_burts") ? get("max_put_burst").safeGet<UInt64>() : default_max_put_burst;
        put_request_throttler = std::make_shared<Throttler>(max_put_rps, max_put_burst);
    }
}

void RequestSettings::validateUploadSettings()
{
    static constexpr size_t min_upload_part_size_limit = 5 * 1024 * 1024;
    if (strict_upload_part_size && strict_upload_part_size < min_upload_part_size_limit)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting strict_upload_part_size has invalid value {} which is less than the s3 API limit {}",
            ReadableSize(strict_upload_part_size), ReadableSize(min_upload_part_size_limit));

    if (min_upload_part_size < min_upload_part_size_limit)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting min_upload_part_size has invalid value {} which is less than the s3 API limit {}",
            ReadableSize(min_upload_part_size), ReadableSize(min_upload_part_size_limit));

    static constexpr size_t max_upload_part_size_limit = 5ull * 1024 * 1024 * 1024;
    if (max_upload_part_size > max_upload_part_size_limit)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting max_upload_part_size has invalid value {} which is greater than the s3 API limit {}",
            ReadableSize(max_upload_part_size), ReadableSize(max_upload_part_size_limit));

    if (max_single_part_upload_size > max_upload_part_size_limit)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting max_single_part_upload_size has invalid value {} which is grater than the s3 API limit {}",
            ReadableSize(max_single_part_upload_size), ReadableSize(max_upload_part_size_limit));

    if (max_single_operation_copy_size > max_upload_part_size_limit)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting max_single_operation_copy_size has invalid value {} which is grater than the s3 API limit {}",
            ReadableSize(max_single_operation_copy_size), ReadableSize(max_upload_part_size_limit));

    if (max_upload_part_size < min_upload_part_size)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting max_upload_part_size ({}) can't be less than setting min_upload_part_size {}",
            ReadableSize(max_upload_part_size), ReadableSize(min_upload_part_size));

    if (!upload_part_size_multiply_factor)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting upload_part_size_multiply_factor cannot be zero");

    if (!upload_part_size_multiply_parts_count_threshold)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting upload_part_size_multiply_parts_count_threshold cannot be zero");

    if (!max_part_number)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting max_part_number cannot be zero");

    static constexpr size_t max_part_number_limit = 10000;
    if (max_part_number > max_part_number_limit)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting max_part_number has invalid value {} which is grater than the s3 API limit {}",
            ReadableSize(max_part_number), ReadableSize(max_part_number_limit));

    size_t maybe_overflow;
    if (common::mulOverflow(max_upload_part_size.value, upload_part_size_multiply_factor.value, maybe_overflow))
        throw Exception(
                        ErrorCodes::INVALID_SETTING_VALUE,
                        "Setting upload_part_size_multiply_factor is too big ({}). "
                        "Multiplication to max_upload_part_size ({}) will cause integer overflow",
                        ReadableSize(max_part_number), ReadableSize(max_part_number_limit));

    std::unordered_set<String> storage_class_names {"STANDARD", "INTELLIGENT_TIERING"};
    if (!storage_class_name.value.empty() && !storage_class_names.contains(storage_class_name))
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting storage_class has invalid value {} which only supports STANDARD and INTELLIGENT_TIERING",
            storage_class_name.value);

    /// TODO: it's possible to set too small limits.
    /// We can check that max possible object size is not too small.
}

bool operator==(const AuthSettings & left, const AuthSettings & right)
{
    if (left.headers != right.headers)
        return false;

    if (left.users != right.users)
        return false;

    if (left.server_side_encryption_kms_config != right.server_side_encryption_kms_config)
        return false;

    auto l = left.begin();
    for (const auto & r : right)
    {
        if ((l == left.end()) || (*l != r))
            return false;
        ++l;
    }
    return l == left.end();
}
}

IMPLEMENT_SETTINGS_TRAITS(S3::AuthSettingsTraits, CLIENT_SETTINGS_LIST)
IMPLEMENT_SETTINGS_TRAITS(S3::RequestSettingsTraits, REQUEST_SETTINGS_LIST)

}
