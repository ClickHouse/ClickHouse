#include <Storages/StorageS3Settings.h>

#include <IO/S3Common.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/Exception.h>
#include <Common/Throttler.h>
#include <Common/formatReadable.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
}

S3Settings::RequestSettings::PartUploadSettings::PartUploadSettings(const Settings & settings)
{
    updateFromSettingsImpl(settings, false);
    validate();
}

S3Settings::RequestSettings::PartUploadSettings::PartUploadSettings(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const Settings & settings,
    String setting_name_prefix)
    : PartUploadSettings(settings)
{
    String key = config_prefix + "." + setting_name_prefix;
    strict_upload_part_size = config.getUInt64(key + "strict_upload_part_size", strict_upload_part_size);
    min_upload_part_size = config.getUInt64(key + "min_upload_part_size", min_upload_part_size);
    max_upload_part_size = config.getUInt64(key + "max_upload_part_size", max_upload_part_size);
    upload_part_size_multiply_factor = config.getUInt64(key + "upload_part_size_multiply_factor", upload_part_size_multiply_factor);
    upload_part_size_multiply_parts_count_threshold = config.getUInt64(key + "upload_part_size_multiply_parts_count_threshold", upload_part_size_multiply_parts_count_threshold);
    max_inflight_parts_for_one_file = config.getUInt64(key + "max_inflight_parts_for_one_file", max_inflight_parts_for_one_file);
    max_part_number = config.getUInt64(key + "max_part_number", max_part_number);
    max_single_part_upload_size = config.getUInt64(key + "max_single_part_upload_size", max_single_part_upload_size);
    max_single_operation_copy_size = config.getUInt64(key + "max_single_operation_copy_size", max_single_operation_copy_size);

    /// This configuration is only applicable to s3. Other types of object storage are not applicable or have different meanings.
    storage_class_name = config.getString(config_prefix + ".s3_storage_class", storage_class_name);
    storage_class_name = Poco::toUpperInPlace(storage_class_name);

    validate();
}

S3Settings::RequestSettings::PartUploadSettings::PartUploadSettings(const NamedCollection & collection)
{
    strict_upload_part_size = collection.getOrDefault<UInt64>("strict_upload_part_size", strict_upload_part_size);
    min_upload_part_size = collection.getOrDefault<UInt64>("min_upload_part_size", min_upload_part_size);
    max_single_part_upload_size = collection.getOrDefault<UInt64>("max_single_part_upload_size", max_single_part_upload_size);
    upload_part_size_multiply_factor = collection.getOrDefault<UInt64>("upload_part_size_multiply_factor", upload_part_size_multiply_factor);
    upload_part_size_multiply_parts_count_threshold = collection.getOrDefault<UInt64>("upload_part_size_multiply_parts_count_threshold", upload_part_size_multiply_parts_count_threshold);
    max_inflight_parts_for_one_file = collection.getOrDefault<UInt64>("max_inflight_parts_for_one_file", max_inflight_parts_for_one_file);

    /// This configuration is only applicable to s3. Other types of object storage are not applicable or have different meanings.
    storage_class_name = collection.getOrDefault<String>("s3_storage_class", storage_class_name);
    storage_class_name = Poco::toUpperInPlace(storage_class_name);

    validate();
}

void S3Settings::RequestSettings::PartUploadSettings::updateFromSettingsImpl(const Settings & settings, bool if_changed)
{
    if (!if_changed || settings.s3_strict_upload_part_size.changed)
        strict_upload_part_size = settings.s3_strict_upload_part_size;

    if (!if_changed || settings.s3_min_upload_part_size.changed)
        min_upload_part_size = settings.s3_min_upload_part_size;

    if (!if_changed || settings.s3_max_upload_part_size.changed)
        max_upload_part_size = settings.s3_max_upload_part_size;

    if (!if_changed || settings.s3_upload_part_size_multiply_factor.changed)
        upload_part_size_multiply_factor = settings.s3_upload_part_size_multiply_factor;

    if (!if_changed || settings.s3_upload_part_size_multiply_parts_count_threshold.changed)
        upload_part_size_multiply_parts_count_threshold = settings.s3_upload_part_size_multiply_parts_count_threshold;

    if (!if_changed || settings.s3_max_inflight_parts_for_one_file.changed)
        max_inflight_parts_for_one_file = settings.s3_max_inflight_parts_for_one_file;

    if (!if_changed || settings.s3_max_single_part_upload_size.changed)
        max_single_part_upload_size = settings.s3_max_single_part_upload_size;
}

void S3Settings::RequestSettings::PartUploadSettings::validate()
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
            "Setting max_upload_part_size has invalid value {} which is grater than the s3 API limit {}",
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
    if (common::mulOverflow(max_upload_part_size, upload_part_size_multiply_factor, maybe_overflow))
        throw Exception(
                        ErrorCodes::INVALID_SETTING_VALUE,
                        "Setting upload_part_size_multiply_factor is too big ({}). "
                        "Multiplication to max_upload_part_size ({}) will cause integer overflow",
                        ReadableSize(max_part_number), ReadableSize(max_part_number_limit));

    std::unordered_set<String> storage_class_names {"STANDARD", "INTELLIGENT_TIERING"};
    if (!storage_class_name.empty() && !storage_class_names.contains(storage_class_name))
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting storage_class has invalid value {} which only supports STANDARD and INTELLIGENT_TIERING",
            storage_class_name);

    /// TODO: it's possible to set too small limits. We can check that max possible object size is not too small.
}


S3Settings::RequestSettings::RequestSettings(const Settings & settings)
    : upload_settings(settings)
{
    updateFromSettingsImpl(settings, false);
}

S3Settings::RequestSettings::RequestSettings(const NamedCollection & collection)
    : upload_settings(collection)
{
    max_single_read_retries = collection.getOrDefault<UInt64>("max_single_read_retries", max_single_read_retries);
    max_connections = collection.getOrDefault<UInt64>("max_connections", max_connections);
    list_object_keys_size = collection.getOrDefault<UInt64>("list_object_keys_size", list_object_keys_size);
    allow_native_copy = collection.getOrDefault<bool>("allow_native_copy", allow_native_copy);
    throw_on_zero_files_match = collection.getOrDefault<bool>("throw_on_zero_files_match", throw_on_zero_files_match);
}

S3Settings::RequestSettings::RequestSettings(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const Settings & settings,
    String setting_name_prefix)
    : upload_settings(config, config_prefix, settings, setting_name_prefix)
{
    String key = config_prefix + "." + setting_name_prefix;
    max_single_read_retries = config.getUInt64(key + "max_single_read_retries", settings.s3_max_single_read_retries);
    max_connections = config.getUInt64(key + "max_connections", settings.s3_max_connections);
    check_objects_after_upload = config.getBool(key + "check_objects_after_upload", settings.s3_check_objects_after_upload);
    list_object_keys_size = config.getUInt64(key + "list_object_keys_size", settings.s3_list_object_keys_size);
    allow_native_copy = config.getBool(key + "allow_native_copy", allow_native_copy);
    throw_on_zero_files_match = config.getBool(key + "throw_on_zero_files_match", settings.s3_throw_on_zero_files_match);
    retry_attempts = config.getUInt64(key + "retry_attempts", settings.s3_retry_attempts);
    request_timeout_ms = config.getUInt64(key + "request_timeout_ms", settings.s3_request_timeout_ms);

    /// NOTE: it would be better to reuse old throttlers to avoid losing token bucket state on every config reload,
    /// which could lead to exceeding limit for short time. But it is good enough unless very high `burst` values are used.
    if (UInt64 max_get_rps = config.getUInt64(key + "max_get_rps", settings.s3_max_get_rps))
    {
        size_t default_max_get_burst = settings.s3_max_get_burst
            ? settings.s3_max_get_burst
            : (Throttler::default_burst_seconds * max_get_rps);

        size_t max_get_burst = config.getUInt64(key + "max_get_burst", default_max_get_burst);

        get_request_throttler = std::make_shared<Throttler>(max_get_rps, max_get_burst);
    }
    if (UInt64 max_put_rps = config.getUInt64(key + "max_put_rps", settings.s3_max_put_rps))
    {
        size_t default_max_put_burst = settings.s3_max_put_burst
            ? settings.s3_max_put_burst
            : (Throttler::default_burst_seconds * max_put_rps);

        size_t max_put_burst = config.getUInt64(key + "max_put_burst", default_max_put_burst);

        put_request_throttler = std::make_shared<Throttler>(max_put_rps, max_put_burst);
    }
}

void S3Settings::RequestSettings::updateFromSettingsImpl(const Settings & settings, bool if_changed)
{
    if (!if_changed || settings.s3_max_single_read_retries.changed)
        max_single_read_retries = settings.s3_max_single_read_retries;

    if (!if_changed || settings.s3_max_connections.changed)
        max_connections = settings.s3_max_connections;

    if (!if_changed || settings.s3_check_objects_after_upload.changed)
        check_objects_after_upload = settings.s3_check_objects_after_upload;

    if (!if_changed || settings.s3_max_unexpected_write_error_retries.changed)
        max_unexpected_write_error_retries = settings.s3_max_unexpected_write_error_retries;

    if (!if_changed || settings.s3_list_object_keys_size.changed)
        list_object_keys_size = settings.s3_list_object_keys_size;

    if ((!if_changed || settings.s3_max_get_rps.changed || settings.s3_max_get_burst.changed) && settings.s3_max_get_rps)
        get_request_throttler = std::make_shared<Throttler>(
            settings.s3_max_get_rps, settings.s3_max_get_burst ? settings.s3_max_get_burst : Throttler::default_burst_seconds * settings.s3_max_get_rps);

    if ((!if_changed || settings.s3_max_put_rps.changed || settings.s3_max_put_burst.changed) && settings.s3_max_put_rps)
        put_request_throttler = std::make_shared<Throttler>(
            settings.s3_max_put_rps, settings.s3_max_put_burst ? settings.s3_max_put_burst : Throttler::default_burst_seconds * settings.s3_max_put_rps);

    if (!if_changed || settings.s3_throw_on_zero_files_match.changed)
        throw_on_zero_files_match = settings.s3_throw_on_zero_files_match;

    if (!if_changed || settings.s3_retry_attempts.changed)
        retry_attempts = settings.s3_retry_attempts;

    if (!if_changed || settings.s3_request_timeout_ms.changed)
        request_timeout_ms = settings.s3_request_timeout_ms;
}

void S3Settings::RequestSettings::updateFromSettings(const Settings & settings)
{
    updateFromSettingsImpl(settings, true);
    upload_settings.updateFromSettings(settings);
}


void StorageS3Settings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config, const Settings & settings)
{
    std::lock_guard lock(mutex);
    s3_settings.clear();
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    for (const String & key : config_keys)
    {
        if (config.has(config_elem + "." + key + ".endpoint"))
        {
            auto endpoint = config.getString(config_elem + "." + key + ".endpoint");
            auto auth_settings = S3::AuthSettings::loadFromConfig(config_elem + "." + key, config);
            S3Settings::RequestSettings request_settings(config, config_elem + "." + key, settings);

            s3_settings.emplace(endpoint, S3Settings{std::move(auth_settings), std::move(request_settings)});
        }
    }
}

S3Settings StorageS3Settings::getSettings(const String & endpoint, const String & user, bool ignore_user) const
{
    std::lock_guard lock(mutex);
    auto next_prefix_setting = s3_settings.upper_bound(endpoint);

    /// Linear time algorithm may be replaced with logarithmic with prefix tree map.
    for (auto possible_prefix_setting = next_prefix_setting; possible_prefix_setting != s3_settings.begin();)
    {
        std::advance(possible_prefix_setting, -1);
        const auto & [endpoint_prefix, settings] = *possible_prefix_setting;
        if (endpoint.starts_with(endpoint_prefix) && (ignore_user || settings.auth_settings.canBeUsedByUser(user)))
            return possible_prefix_setting->second;
    }

    return {};
}

}
