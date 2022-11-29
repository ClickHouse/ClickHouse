#include <Storages/StorageS3Settings.h>

#include <IO/S3Common.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/Throttler.h>
#include <Interpreters/Context.h>
#include <base/unit.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{

namespace
{
    /// An object up to 5 GB can be copied in a single atomic operation.
    constexpr UInt64 DEFAULT_MAX_SINGLE_OPERATION_COPY_SIZE = 5_GiB;

    /// The maximum size of an uploaded part.
    constexpr UInt64 DEFAULT_MAX_UPLOAD_PART_SIZE = 5_GiB;
}


void StorageS3Settings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config, const Settings & settings)
{
    std::lock_guard lock(mutex);
    s3_settings.clear();
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    auto get_string_for_key = [&](const String & key, const String & elem, bool with_default = true, const String & default_value = "")
    {
        return with_default ? config.getString(config_elem + "." + key + "." + elem, default_value) : config.getString(config_elem + "." + key + "." + elem);
    };

    auto get_uint_for_key = [&](const String & key, const String & elem, bool with_default = true, UInt64 default_value = 0)
    {
        return with_default ? config.getUInt64(config_elem + "." + key + "." + elem, default_value) : config.getUInt64(config_elem + "." + key + "." + elem);
    };


    auto get_bool_for_key = [&](const String & key, const String & elem, bool with_default = true, bool default_value = false)
    {
        return with_default ? config.getBool(config_elem + "." + key + "." + elem, default_value) : config.getBool(config_elem + "." + key + "." + elem);
    };


    for (const String & key : config_keys)
    {
        if (config.has(config_elem + "." + key + ".endpoint"))
        {
            auto endpoint = get_string_for_key(key, "endpoint", false);

            auto auth_settings = S3::AuthSettings::loadFromConfig(config_elem + "." + key, config);

            S3Settings::RequestSettings request_settings;
            request_settings.max_single_read_retries = get_uint_for_key(key, "max_single_read_retries", true, settings.s3_max_single_read_retries);
            request_settings.min_upload_part_size = get_uint_for_key(key, "min_upload_part_size", true, settings.s3_min_upload_part_size);
            request_settings.max_upload_part_size = get_uint_for_key(key, "max_upload_part_size", true, DEFAULT_MAX_UPLOAD_PART_SIZE);
            request_settings.upload_part_size_multiply_factor = get_uint_for_key(key, "upload_part_size_multiply_factor", true, settings.s3_upload_part_size_multiply_factor);
            request_settings.upload_part_size_multiply_parts_count_threshold = get_uint_for_key(key, "upload_part_size_multiply_parts_count_threshold", true, settings.s3_upload_part_size_multiply_parts_count_threshold);
            request_settings.max_single_part_upload_size = get_uint_for_key(key, "max_single_part_upload_size", true, settings.s3_max_single_part_upload_size);
            request_settings.max_single_operation_copy_size = get_uint_for_key(key, "max_single_operation_copy_size", true, DEFAULT_MAX_SINGLE_OPERATION_COPY_SIZE);
            request_settings.max_connections = get_uint_for_key(key, "max_connections", true, settings.s3_max_connections);
            request_settings.check_objects_after_upload = get_bool_for_key(key, "check_objects_after_upload", true, false);

            // NOTE: it would be better to reuse old throttlers to avoid losing token bucket state on every config reload, which could lead to exceeding limit for short time. But it is good enough unless very high `burst` values are used.
            if (UInt64 max_get_rps = get_uint_for_key(key, "max_get_rps", true, settings.s3_max_get_rps))
                request_settings.get_request_throttler = std::make_shared<Throttler>(
                    max_get_rps, get_uint_for_key(key, "max_get_burst", true, settings.s3_max_get_burst ? settings.s3_max_get_burst : Throttler::default_burst_seconds * max_get_rps));
            if (UInt64 max_put_rps = get_uint_for_key(key, "max_put_rps", true, settings.s3_max_put_rps))
                request_settings.put_request_throttler = std::make_shared<Throttler>(
                    max_put_rps, get_uint_for_key(key, "max_put_burst", true, settings.s3_max_put_burst ? settings.s3_max_put_burst : Throttler::default_burst_seconds * max_put_rps));

            s3_settings.emplace(endpoint, S3Settings{std::move(auth_settings), std::move(request_settings)});
        }
    }
}

S3Settings StorageS3Settings::getSettings(const String & endpoint) const
{
    std::lock_guard lock(mutex);
    auto next_prefix_setting = s3_settings.upper_bound(endpoint);

    /// Linear time algorithm may be replaced with logarithmic with prefix tree map.
    for (auto possible_prefix_setting = next_prefix_setting; possible_prefix_setting != s3_settings.begin();)
    {
        std::advance(possible_prefix_setting, -1);
        if (boost::algorithm::starts_with(endpoint, possible_prefix_setting->first))
            return possible_prefix_setting->second;
    }

    return {};
}

S3Settings::RequestSettings::RequestSettings(const Settings & settings)
{
    max_single_read_retries = settings.s3_max_single_read_retries;
    min_upload_part_size = settings.s3_min_upload_part_size;
    upload_part_size_multiply_factor = settings.s3_upload_part_size_multiply_factor;
    upload_part_size_multiply_parts_count_threshold = settings.s3_upload_part_size_multiply_parts_count_threshold;
    max_single_part_upload_size = settings.s3_max_single_part_upload_size;
    max_connections = settings.s3_max_connections;
    check_objects_after_upload = settings.s3_check_objects_after_upload;
    max_unexpected_write_error_retries = settings.s3_max_unexpected_write_error_retries;
    if (settings.s3_max_get_rps)
        get_request_throttler = std::make_shared<Throttler>(
            settings.s3_max_get_rps, settings.s3_max_get_burst ? settings.s3_max_get_burst : Throttler::default_burst_seconds * settings.s3_max_get_rps);
    if (settings.s3_max_put_rps)
        put_request_throttler = std::make_shared<Throttler>(
            settings.s3_max_put_rps, settings.s3_max_put_burst ? settings.s3_max_put_burst : Throttler::default_burst_seconds * settings.s3_max_put_rps);
}

void S3Settings::RequestSettings::updateFromSettingsIfEmpty(const Settings & settings)
{
    if (!max_single_read_retries)
        max_single_read_retries = settings.s3_max_single_read_retries;
    if (!min_upload_part_size)
        min_upload_part_size = settings.s3_min_upload_part_size;
    if (!max_upload_part_size)
        max_upload_part_size = DEFAULT_MAX_UPLOAD_PART_SIZE;
    if (!upload_part_size_multiply_factor)
        upload_part_size_multiply_factor = settings.s3_upload_part_size_multiply_factor;
    if (!upload_part_size_multiply_parts_count_threshold)
        upload_part_size_multiply_parts_count_threshold = settings.s3_upload_part_size_multiply_parts_count_threshold;
    if (!max_single_part_upload_size)
        max_single_part_upload_size = settings.s3_max_single_part_upload_size;
    if (!max_single_operation_copy_size)
        max_single_operation_copy_size = DEFAULT_MAX_SINGLE_OPERATION_COPY_SIZE;
    if (!max_connections)
        max_connections = settings.s3_max_connections;
    if (!max_unexpected_write_error_retries)
        max_unexpected_write_error_retries = settings.s3_max_unexpected_write_error_retries;
    check_objects_after_upload = settings.s3_check_objects_after_upload;
    if (!get_request_throttler && settings.s3_max_get_rps)
        get_request_throttler = std::make_shared<Throttler>(
            settings.s3_max_get_rps, settings.s3_max_get_burst ? settings.s3_max_get_burst : Throttler::default_burst_seconds * settings.s3_max_get_rps);
    if (!put_request_throttler && settings.s3_max_put_rps)
        put_request_throttler = std::make_shared<Throttler>(
            settings.s3_max_put_rps, settings.s3_max_put_burst ? settings.s3_max_put_burst : Throttler::default_burst_seconds * settings.s3_max_put_rps);
}

}
