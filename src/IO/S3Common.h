#pragma once

#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/HTTPHeaderEntries.h>

#include "config.h"

#if USE_AWS_S3

#include <base/types.h>
#include <Common/Exception.h>
#include <Common/Throttler_fwd.h>
#include <Common/Throttler.h>
#include <Core/Settings.h>

#include <IO/S3/URI.h>
#include <IO/S3/Credentials.h>
#include <IO/S3Defines.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Errors.h>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
}

class RemoteHostFilter;
class NamedCollection;

class S3Exception : public Exception
{
public:

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    S3Exception(Aws::S3::S3Errors code_, fmt::format_string<Args...> fmt, Args &&... args)
        : Exception(fmt::format(fmt, std::forward<Args>(args)...), ErrorCodes::S3_ERROR)
        , code(code_)
    {
    }

    S3Exception(const std::string & msg, Aws::S3::S3Errors code_)
        : Exception(msg, ErrorCodes::S3_ERROR)
        , code(code_)
    {}

    Aws::S3::S3Errors getS3ErrorCode() const
    {
        return code;
    }

    bool isRetryableError() const;

private:
    Aws::S3::S3Errors code;
};
}

#endif

namespace Poco::Util
{
    class AbstractConfiguration;
};

namespace DB::S3
{

HTTPHeaderEntries getHTTPHeaders(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config);

ServerSideEncryptionKMSConfig getSSEKMSConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config);

struct AuthSettings
{
    std::string access_key_id;
    std::string secret_access_key;
    std::string session_token;
    std::string region;
    std::string server_side_encryption_customer_key_base64;

    HTTPHeaderEntries headers;
    std::unordered_set<std::string> users;
    ServerSideEncryptionKMSConfig server_side_encryption_kms_config;

    std::optional<size_t> connect_timeout_ms;
    std::optional<size_t> request_timeout_ms;
    std::optional<size_t> max_connections;
    std::optional<size_t> http_keep_alive_timeout;
    std::optional<size_t> http_keep_alive_max_requests;
    std::optional<size_t> expiration_window_seconds;

    std::optional<bool> use_environment_credentials;
    std::optional<bool> no_sign_request;
    std::optional<bool> use_adaptive_timeouts;
    std::optional<bool> use_insecure_imds_request;
    std::optional<bool> is_virtual_hosted_style;
    std::optional<bool> disable_checksum;
    std::optional<bool> gcs_issue_compose_request;

    bool hasUpdates(const AuthSettings & other) const;
    void updateFrom(const AuthSettings & from);

    bool canBeUsedByUser(const String & user) const { return users.empty() || users.contains(user); }

    static AuthSettings loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const DB::Settings & settings,
        const std::string & setting_name_prefix = "");

    static AuthSettings loadFromSettings(const DB::Settings & settings);

    static AuthSettings loadFromNamedCollection(const NamedCollection & collection);

    void updateFromSettings(const DB::Settings & settings, bool if_changed);

private:
    bool operator==(const AuthSettings & other) const = default;
};

#define REQUEST_SETTINGS(M, ALIAS) \
    M(UInt64, max_single_read_retries, 4, "", 0) \
    M(UInt64, request_timeout_ms, DEFAULT_REQUEST_TIMEOUT_MS, "", 0) \
    M(UInt64, list_object_keys_size, 1000, "", 0) \
    M(Bool, allow_native_copy, true, "", 0) \
    M(Bool, check_objects_after_upload, false, "", 0) \
    M(Bool, throw_on_zero_files_match, false, "", 0) \
    M(UInt64, max_single_operation_copy_size, DEFAULT_MAX_SINGLE_OPERATION_COPY_SIZE, "", 0) \
    M(String, storage_class_name, "", "", 0) \

#define PART_UPLOAD_SETTINGS(M, ALIAS) \
    M(UInt64, strict_upload_part_size, 0, "", 0) \
    M(UInt64, min_upload_part_size, DEFAULT_MIN_UPLOAD_PART_SIZE, "", 0) \
    M(UInt64, max_upload_part_size, DEFAULT_MAX_UPLOAD_PART_SIZE, "", 0) \
    M(UInt64, upload_part_size_multiply_factor, DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_FACTOR, "", 0) \
    M(UInt64, upload_part_size_multiply_parts_count_threshold, DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_PARTS_COUNT_THRESHOLD, "", 0) \
    M(UInt64, max_inflight_parts_for_one_file, DEFAULT_MAX_INFLIGHT_PARTS_FOR_ONE_FILE, "", 0) \
    M(UInt64, max_part_number, DEFAULT_MAX_PART_NUMBER, "", 0) \
    M(UInt64, max_single_part_upload_size, DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE, "", 0) \
    M(UInt64, max_unexpected_write_error_retries, 4, "", 0) \


#define REQUEST_SETTINGS_LIST(M, ALIAS) \
    REQUEST_SETTINGS(M, ALIAS)             \
    PART_UPLOAD_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(RequestSettingsTraits, REQUEST_SETTINGS_LIST)

struct RequestSettings : public BaseSettings<RequestSettingsTraits>
{
    void validateUploadSettings();

    ThrottlerPtr get_request_throttler;
    ThrottlerPtr put_request_throttler;

    static RequestSettings loadFromSettings(const DB::Settings & settings, bool validate_settings = true);
    static RequestSettings loadFromNamedCollection(const NamedCollection & collection, bool validate_settings = true);
    static RequestSettings loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const DB::Settings & settings,
        bool validate_settings = true,
        const std::string & setting_name_prefix = "");

    void updateFromSettings(const DB::Settings & settings, bool if_changed, bool validate_settings = true);
    void updateIfChanged(const RequestSettings & settings);

private:
    void initializeThrottler(const DB::Settings & settings);
};

}
