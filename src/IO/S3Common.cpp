#include <IO/S3Common.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Poco/Util/AbstractConfiguration.h>

#include "config.h"

#if USE_AWS_S3

#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Common/NamedCollections/NamedCollections.h>


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
    extern const int INVALID_SETTING_VALUE;
}

#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
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

AuthSettings AuthSettings::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DB::Settings & settings,
    const std::string & setting_name_prefix)
{
    auto auth_settings = AuthSettings::loadFromSettings(settings);

    const std::string prefix = config_prefix + "." + setting_name_prefix;
    auto has = [&](const std::string & key) -> bool { return config.has(prefix + key); };
    auto get_uint = [&](const std::string & key) -> size_t { return config.getUInt64(prefix + key); };
    auto get_bool = [&](const std::string & key) -> bool { return config.getBool(prefix + key); };
    auto get_string = [&](const std::string & key) -> std::string { return config.getString(prefix + key); };

    if (has("access_key_id"))
        auth_settings.access_key_id = get_string("access_key_id");
    if (has("secret_access_key"))
        auth_settings.secret_access_key = get_string("secret_access_key");
    if (has("session_token"))
        auth_settings.secret_access_key = get_string("session_token");

    if (has("region"))
        auth_settings.region = get_string("region");
    if (has("server_side_encryption_customer_key_base64"))
        auth_settings.region = get_string("server_side_encryption_customer_key_base64");

    if (has("connect_timeout_ms"))
        auth_settings.connect_timeout_ms = get_uint("connect_timeout_ms");
    if (has("request_timeout_ms"))
        auth_settings.request_timeout_ms = get_uint("request_timeout_ms");
    if (has("max_connections"))
        auth_settings.max_connections = get_uint("max_connections");

    if (has("http_keep_alive_timeout"))
        auth_settings.http_keep_alive_timeout = get_uint("http_keep_alive_timeout");
    if (has("http_keep_alive_max_requests"))
        auth_settings.http_keep_alive_max_requests = get_uint("http_keep_alive_max_requests");

    if (has("use_environment_credentials"))
        auth_settings.use_environment_credentials = get_bool("use_environment_credentials");
    if (has("use_adaptive_timeouts"))
        auth_settings.use_adaptive_timeouts = get_bool("use_adaptive_timeouts");
    if (has("no_sing_request"))
        auth_settings.no_sign_request = get_bool("no_sing_request");
    if (has("expiration_window_seconds"))
        auth_settings.expiration_window_seconds = get_uint("expiration_window_seconds");
    if (has("gcs_issue_compose_request"))
        auth_settings.gcs_issue_compose_request = get_bool("gcs_issue_compose_request");
    if (has("use_insecure_imds_request"))
        auth_settings.use_insecure_imds_request = get_bool("use_insecure_imds_request");

    auth_settings.headers = getHTTPHeaders(config_prefix, config);
    auth_settings.server_side_encryption_kms_config = getSSEKMSConfig(config_prefix, config);

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
    {
        if (startsWith(key, "user"))
            auth_settings.users.insert(config.getString(config_prefix + "." + key));
    }

    return auth_settings;
}

AuthSettings AuthSettings::loadFromSettings(const DB::Settings & settings)
{
    AuthSettings auth_settings{};
    auth_settings.updateFromSettings(settings, /* if_changed */false);
    return auth_settings;
}

void AuthSettings::updateFromSettings(const DB::Settings & settings, bool if_changed)
{
    if (!if_changed || settings.s3_connect_timeout_ms.changed)
        connect_timeout_ms = settings.s3_connect_timeout_ms;
    if (!if_changed || settings.s3_request_timeout_ms.changed)
        request_timeout_ms = settings.s3_request_timeout_ms;
    if (!if_changed || settings.s3_max_connections.changed)
        max_connections = settings.s3_max_connections;
    if (!if_changed || settings.s3_use_adaptive_timeouts.changed)
        use_adaptive_timeouts = settings.s3_use_adaptive_timeouts;
    if (!if_changed || settings.s3_disable_checksum.changed)
        disable_checksum = settings.s3_disable_checksum;
}

bool AuthSettings::hasUpdates(const AuthSettings & other) const
{
    AuthSettings copy = *this;
    copy.updateFrom(other);
    return *this != copy;
}

void AuthSettings::updateFrom(const AuthSettings & from)
{
    /// Update with check for emptyness only parameters which
    /// can be passed not only from config, but via ast.

    if (!from.access_key_id.empty())
        access_key_id = from.access_key_id;
    if (!from.secret_access_key.empty())
        secret_access_key = from.secret_access_key;
    if (!from.session_token.empty())
        session_token = from.session_token;

    if (!from.headers.empty())
        headers = from.headers;
    if (!from.region.empty())
        region = from.region;

    server_side_encryption_customer_key_base64 = from.server_side_encryption_customer_key_base64;
    server_side_encryption_kms_config = from.server_side_encryption_kms_config;

    if (from.use_environment_credentials.has_value())
       use_environment_credentials = from.use_environment_credentials;

    if (from.use_insecure_imds_request.has_value())
        use_insecure_imds_request = from.use_insecure_imds_request;

    if (from.expiration_window_seconds.has_value())
        expiration_window_seconds = from.expiration_window_seconds;

    if (from.no_sign_request.has_value())
        no_sign_request = from.no_sign_request;

    users.insert(from.users.begin(), from.users.end());
}

RequestSettings RequestSettings::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DB::Settings & settings,
    bool validate_settings,
    const std::string & setting_name_prefix)
{
    auto request_settings = RequestSettings::loadFromSettings(settings, validate_settings);

    String prefix = config_prefix + "." + setting_name_prefix;
    auto has = [&](const std::string & key) -> bool { return config.has(prefix + key); };
    auto get_uint = [&](const std::string & key) -> size_t { return config.getUInt64(prefix + key); };
    auto get_string = [&](const std::string & key) -> std::string { return config.getString(prefix + key); };
    auto get_bool = [&](const std::string & key) -> bool { return config.getBool(prefix + key); };

    if (has("strict_upload_part_size"))
        request_settings.upload_settings.strict_upload_part_size = get_uint("strict_upload_part_size");
    if (has("min_upload_part_size"))
        request_settings.upload_settings.min_upload_part_size = get_uint("min_upload_part_size");
    if (has("max_upload_part_size"))
        request_settings.upload_settings.max_upload_part_size = get_uint("max_upload_part_size");
    if (has("upload_part_size_multiply_factor"))
        request_settings.upload_settings.upload_part_size_multiply_factor = get_uint("upload_part_size_multiply_factor");
    if (has("upload_part_size_multiply_parts_count_threshold"))
        request_settings.upload_settings.upload_part_size_multiply_parts_count_threshold = get_uint("upload_part_size_multiply_parts_count_threshold");
    if (has("max_inflight_parts_for_one_file"))
        request_settings.upload_settings.max_inflight_parts_for_one_file = get_uint("max_inflight_parts_for_one_file");
    if (has("max_part_number"))
        request_settings.upload_settings.max_part_number = get_uint("max_part_number");
    if (has("max_single_part_upload_size"))
        request_settings.upload_settings.max_single_part_upload_size = get_uint("max_single_part_upload_size");
    if (has("max_single_operation_copy_size"))
        request_settings.upload_settings.max_single_operation_copy_size = get_uint("max_single_operation_copy_size");
    if (has("s3_storage_class"))
        request_settings.upload_settings.storage_class_name = get_string("s3_storage_class");

    request_settings.upload_settings.storage_class_name = Poco::toUpperInPlace(request_settings.upload_settings.storage_class_name);
    if (validate_settings)
        request_settings.upload_settings.validate();

    if (has("max_single_read_retries"))
        request_settings.max_single_read_retries = get_uint("max_single_read_retries");
    if (has("check_objects_after_upload"))
        request_settings.check_objects_after_upload = get_bool("check_objects_after_upload");
    if (has("list_object_keys_size"))
        request_settings.list_object_keys_size = get_uint("list_object_keys_size");
    if (has("allow_native_copy"))
        request_settings.allow_native_copy = get_bool("allow_native_copy");
    if (has("throw_on_zero_files_match"))
        request_settings.throw_on_zero_files_match = get_bool("throw_on_zero_files_match");
    if (has("request_timeout_ms"))
        request_settings.request_timeout_ms = get_uint("request_timeout_ms");

    /// NOTE: it would be better to reuse old throttlers
    /// to avoid losing token bucket state on every config reload,
    /// which could lead to exceeding limit for short time.
    /// But it is good enough unless very high `burst` values are used.
    if (UInt64 max_get_rps = has("max_get_rps") ? get_uint("max_get_rps") : settings.s3_max_get_rps)
    {
        size_t default_max_get_burst = settings.s3_max_get_burst
            ? settings.s3_max_get_burst
            : (Throttler::default_burst_seconds * max_get_rps);
        size_t max_get_burst = has("max_get_burst") ? get_uint("max_get_burst") : default_max_get_burst;
        request_settings.get_request_throttler = std::make_shared<Throttler>(max_get_rps, max_get_burst);
    }
    if (UInt64 max_put_rps = has("max_put_rps") ? get_uint("max_put_rps") : settings.s3_max_put_rps)
    {
        size_t default_max_put_burst = settings.s3_max_put_burst
            ? settings.s3_max_put_burst
            : (Throttler::default_burst_seconds * max_put_rps);
        size_t max_put_burst = has("max_put_burst") ? get_uint("max_put_burst") : default_max_put_burst;
        request_settings.put_request_throttler = std::make_shared<Throttler>(max_put_rps, max_put_burst);
    }
    return request_settings;
}

RequestSettings RequestSettings::loadFromNamedCollection(const NamedCollection & collection, bool validate_settings)
{
    RequestSettings settings{};

    if (collection.has("strict_upload_part_size"))
        settings.upload_settings.strict_upload_part_size = collection.get<UInt64>("strict_upload_part_size");
    if (collection.has("min_upload_part_size"))
        settings.upload_settings.min_upload_part_size = collection.get<UInt64>("min_upload_part_size");
    if (collection.has("max_upload_part_size"))
        settings.upload_settings.min_upload_part_size = collection.get<UInt64>("max_upload_part_size");
    if (collection.has("upload_part_size_multiply_factor"))
        settings.upload_settings.upload_part_size_multiply_factor = collection.get<UInt64>("upload_part_size_multiply_factor");
    if (collection.has("upload_part_size_multiply_parts_count_threshold"))
        settings.upload_settings.upload_part_size_multiply_parts_count_threshold = collection.get<UInt64>("upload_part_size_multiply_parts_count_threshold");
    if (collection.has("max_inflight_parts_for_one_file"))
        settings.upload_settings.max_inflight_parts_for_one_file = collection.get<UInt64>("max_inflight_parts_for_one_file");
    if (collection.has("max_part_number"))
        settings.upload_settings.max_single_part_upload_size = collection.get<UInt64>("max_part_number");
    if (collection.has("max_single_part_upload_size"))
        settings.upload_settings.max_single_part_upload_size = collection.get<UInt64>("max_single_part_upload_size");
    if (collection.has("max_single_operation_copy_size"))
        settings.upload_settings.max_single_part_upload_size = collection.get<UInt64>("max_single_operation_copy_size");
    if (collection.has("s3_storage_class"))
        settings.upload_settings.storage_class_name = collection.get<String>("s3_storage_class");

    settings.upload_settings.storage_class_name = Poco::toUpperInPlace(settings.upload_settings.storage_class_name);
    if (validate_settings)
        settings.upload_settings.validate();

    if (collection.has("max_single_read_retries"))
        settings.max_single_read_retries = collection.get<UInt64>("max_single_read_retries");
    if (collection.has("list_object_keys_size"))
        settings.list_object_keys_size = collection.get<UInt64>("list_object_keys_size");
    if (collection.has("allow_native_copy"))
        settings.allow_native_copy = collection.get<bool>("allow_native_copy");
    if (collection.has("throw_on_zero_files_match"))
        settings.throw_on_zero_files_match = collection.get<bool>("throw_on_zero_files_match");

    return settings;
}

RequestSettings RequestSettings::loadFromSettings(const DB::Settings & settings, bool validate_settings)
{
    RequestSettings request_settings{};
    request_settings.updateFromSettings(settings, /* if_changed */false, validate_settings);
    return request_settings;
}

void RequestSettings::updateFromSettings(const DB::Settings & settings, bool if_changed, bool validate_settings)
{
    if (!if_changed || settings.s3_strict_upload_part_size.changed)
        upload_settings.strict_upload_part_size = settings.s3_strict_upload_part_size;
    if (!if_changed || settings.s3_min_upload_part_size.changed)
        upload_settings.min_upload_part_size = settings.s3_min_upload_part_size;
    if (!if_changed || settings.s3_max_upload_part_size.changed)
        upload_settings.max_upload_part_size = settings.s3_max_upload_part_size;
    if (!if_changed || settings.s3_upload_part_size_multiply_factor.changed)
        upload_settings.upload_part_size_multiply_factor = settings.s3_upload_part_size_multiply_factor;
    if (!if_changed || settings.s3_upload_part_size_multiply_parts_count_threshold.changed)
        upload_settings.upload_part_size_multiply_parts_count_threshold = settings.s3_upload_part_size_multiply_parts_count_threshold;
    if (!if_changed || settings.s3_max_inflight_parts_for_one_file.changed)
        upload_settings.max_inflight_parts_for_one_file = settings.s3_max_inflight_parts_for_one_file;
    if (!if_changed || settings.s3_max_part_number.changed)
        upload_settings.max_part_number = settings.s3_max_part_number;
    if (!if_changed || settings.s3_max_single_part_upload_size.changed)
        upload_settings.max_single_part_upload_size = settings.s3_max_single_part_upload_size;
    if (!if_changed || settings.s3_max_single_operation_copy_size.changed)
        upload_settings.max_part_number = settings.s3_max_single_operation_copy_size;

    if (validate_settings)
        upload_settings.validate();

    if (!if_changed || settings.s3_max_single_read_retries.changed)
        max_single_read_retries = settings.s3_max_single_read_retries;
    if (!if_changed || settings.s3_check_objects_after_upload.changed)
        check_objects_after_upload = settings.s3_check_objects_after_upload;
    if (!if_changed || settings.s3_max_unexpected_write_error_retries.changed)
        max_unexpected_write_error_retries = settings.s3_max_unexpected_write_error_retries;
    if (!if_changed || settings.s3_list_object_keys_size.changed)
        list_object_keys_size = settings.s3_list_object_keys_size;
    if (!if_changed || settings.s3_throw_on_zero_files_match.changed)
        throw_on_zero_files_match = settings.s3_throw_on_zero_files_match;
    if (!if_changed || settings.s3_request_timeout_ms.changed)
        request_timeout_ms = settings.s3_request_timeout_ms;

    if ((!if_changed || settings.s3_max_get_rps.changed || settings.s3_max_get_burst.changed) && settings.s3_max_get_rps)
    {
        size_t max_get_burst = settings.s3_max_get_burst
            ? settings.s3_max_get_burst
            : Throttler::default_burst_seconds * settings.s3_max_get_rps;
        get_request_throttler = std::make_shared<Throttler>(settings.s3_max_get_rps, max_get_burst);
    }
    if ((!if_changed || settings.s3_max_put_rps.changed || settings.s3_max_put_burst.changed) && settings.s3_max_put_rps)
    {
        size_t max_put_burst = settings.s3_max_put_burst
            ? settings.s3_max_put_burst
            : Throttler::default_burst_seconds * settings.s3_max_put_rps;
        put_request_throttler = std::make_shared<Throttler>(settings.s3_max_put_rps, max_put_burst);
    }
}

void RequestSettings::PartUploadSettings::validate()
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

    /// TODO: it's possible to set too small limits.
    /// We can check that max possible object size is not too small.
}

}

}
