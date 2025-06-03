#pragma once

#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/S3Defines.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/Throttler_fwd.h>
#include <Common/Throttler.h>
#include <Core/SettingsEnums.h>
#include <Core/BaseSettings.h>
#include <Interpreters/Context.h>
#include <unordered_set>

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/URI.h>
#include <IO/S3/Credentials.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Errors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
}

struct Settings;

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

namespace DB
{
class NamedCollection;
struct ProxyConfigurationResolver;

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
///    If some setting is absent from there, we lool up for it in Users config.

#define AUTH_SETTINGS(M, ALIAS) \
    M(String, access_key_id, "", "", 0) \
    M(String, secret_access_key, "", "", 0) \
    M(String, session_token, "", "", 0) \
    M(String, region, "", "", 0) \
    M(String, server_side_encryption_customer_key_base64, "", "", 0) \

#define CLIENT_SETTINGS(M, ALIAS) \
    M(UInt64, connect_timeout_ms, DEFAULT_CONNECT_TIMEOUT_MS, "", 0) \
    M(UInt64, request_timeout_ms, DEFAULT_REQUEST_TIMEOUT_MS, "", 0) \
    M(UInt64, max_connections, DEFAULT_MAX_CONNECTIONS, "", 0) \
    M(UInt64, http_keep_alive_timeout, DEFAULT_KEEP_ALIVE_TIMEOUT, "", 0) \
    M(UInt64, http_keep_alive_max_requests, DEFAULT_KEEP_ALIVE_MAX_REQUESTS, "", 0) \
    M(UInt64, expiration_window_seconds, DEFAULT_EXPIRATION_WINDOW_SECONDS, "", 0) \
    M(Bool, use_environment_credentials, DEFAULT_USE_ENVIRONMENT_CREDENTIALS, "", 0) \
    M(Bool, no_sign_request, DEFAULT_NO_SIGN_REQUEST, "", 0) \
    M(Bool, use_insecure_imds_request, false, "", 0) \
    M(Bool, use_adaptive_timeouts, DEFAULT_USE_ADAPTIVE_TIMEOUTS, "", 0) \
    M(Bool, is_virtual_hosted_style, false, "", 0) \
    M(Bool, disable_checksum, DEFAULT_DISABLE_CHECKSUM, "", 0) \
    M(Bool, gcs_issue_compose_request, false, "", 0) \

#define REQUEST_SETTINGS(M, ALIAS) \
    M(UInt64, max_single_read_retries, 4, "", 0) \
    M(UInt64, request_timeout_ms, DEFAULT_REQUEST_TIMEOUT_MS, "", 0) \
    M(UInt64, list_object_keys_size, DEFAULT_LIST_OBJECT_KEYS_SIZE, "", 0) \
    M(Bool, allow_native_copy, DEFAULT_ALLOW_NATIVE_COPY, "", 0) \
    M(Bool, check_objects_after_upload, DEFAULT_CHECK_OBJECTS_AFTER_UPLOAD, "", 0) \
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

#define CLIENT_SETTINGS_LIST(M, ALIAS) \
    CLIENT_SETTINGS(M, ALIAS)             \
    AUTH_SETTINGS(M, ALIAS)

#define REQUEST_SETTINGS_LIST(M, ALIAS) \
    REQUEST_SETTINGS(M, ALIAS)             \
    PART_UPLOAD_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(AuthSettingsTraits, CLIENT_SETTINGS_LIST)
DECLARE_SETTINGS_TRAITS(RequestSettingsTraits, REQUEST_SETTINGS_LIST)

struct AuthSettings : public BaseSettings<AuthSettingsTraits>
{
    AuthSettings() = default;

    AuthSettings(
        const Poco::Util::AbstractConfiguration & config,
        const DB::Settings & settings,
        const std::string & config_prefix);

    explicit AuthSettings(const DB::Settings & settings);

    explicit AuthSettings(const DB::NamedCollection & collection);

    void updateFromSettings(const DB::Settings & settings, bool if_changed);
    bool hasUpdates(const AuthSettings & other) const;
    void updateIfChanged(const AuthSettings & settings);
    bool canBeUsedByUser(const String & user) const { return users.empty() || users.contains(user); }

    HTTPHeaderEntries headers;
    std::unordered_set<std::string> users;
    ServerSideEncryptionKMSConfig server_side_encryption_kms_config;
    /// Note: if you add any field, do not forget to update operator ==.
};

bool operator==(const AuthSettings & left, const AuthSettings & right);

struct RequestSettings : public BaseSettings<RequestSettingsTraits>
{
    RequestSettings() = default;

    /// Create request settings from Config.
    RequestSettings(
        const Poco::Util::AbstractConfiguration & config,
        const DB::Settings & settings,
        const std::string & config_prefix,
        const std::string & setting_name_prefix = "",
        bool validate_settings = true);

    /// Create request settings from DB::Settings.
    explicit RequestSettings(const DB::Settings & settings, bool validate_settings = true);

    /// Create request settings from NamedCollection.
    RequestSettings(
        const NamedCollection & collection,
        const DB::Settings & settings,
        bool validate_settings = true);

    void updateFromSettings(const DB::Settings & settings, bool if_changed, bool validate_settings = true);
    void updateIfChanged(const RequestSettings & settings);
    void validateUploadSettings();

    ThrottlerPtr get_request_throttler;
    ThrottlerPtr put_request_throttler;
    std::shared_ptr<ProxyConfigurationResolver> proxy_resolver;

private:
    void finishInit(const DB::Settings & settings, bool validate_settings);
    void normalizeSettings();
};

HTTPHeaderEntries getHTTPHeaders(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config);

ServerSideEncryptionKMSConfig getSSEKMSConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config);

}
}
