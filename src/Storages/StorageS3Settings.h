#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#include <base/types.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Throttler_fwd.h>
#include <Storages/HeaderCollection.h>

#include <IO/S3Common.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

struct Settings;

struct S3Settings
{
    struct RequestSettings
    {
        size_t max_single_read_retries = 0;
        size_t min_upload_part_size = 0;
        size_t max_upload_part_size = 0;
        size_t upload_part_size_multiply_factor = 0;
        size_t upload_part_size_multiply_parts_count_threshold = 0;
        size_t max_part_number = 0;
        size_t max_single_part_upload_size = 0;
        size_t max_single_operation_copy_size = 0;
        size_t max_connections = 0;
        bool check_objects_after_upload = false;
        size_t max_unexpected_write_error_retries = 0;
        ThrottlerPtr get_request_throttler;
        ThrottlerPtr put_request_throttler;

        RequestSettings() = default;
        explicit RequestSettings(const Settings & settings);

        inline bool operator==(const RequestSettings & other) const
        {
            return max_single_read_retries == other.max_single_read_retries
                && min_upload_part_size == other.min_upload_part_size
                && max_upload_part_size == other.max_upload_part_size
                && upload_part_size_multiply_factor == other.upload_part_size_multiply_factor
                && upload_part_size_multiply_parts_count_threshold == other.upload_part_size_multiply_parts_count_threshold
                && max_part_number == other.max_part_number
                && max_single_part_upload_size == other.max_single_part_upload_size
                && max_single_operation_copy_size == other.max_single_operation_copy_size
                && max_connections == other.max_connections
                && check_objects_after_upload == other.check_objects_after_upload
                && max_unexpected_write_error_retries == other.max_unexpected_write_error_retries
                && get_request_throttler == other.get_request_throttler
                && put_request_throttler == other.put_request_throttler;
        }

        static const constexpr UInt64 DEFAULT_SINGLE_READ_RETRIES = 4;
        static const constexpr UInt64 DEFAULT_MIN_UPLOAD_PART_SIZE = 16 * 1024 * 1024;
        static const constexpr UInt64 DEFAULT_MAX_UPLOAD_PART_SIZE = 5ULL * 1024 * 1024 * 1024;
        static const constexpr UInt64 DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_FACTOR = 2;
        static const constexpr UInt64 DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_PARTS_COUNT_THRESHOLD = 500;
        static const constexpr UInt64 DEFAULT_MAX_PART_NUMBER = 10000;
        static const constexpr UInt64 DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE = 32 * 1024 * 1024;
        static const constexpr UInt64 DEFAULT_MAX_SINGLE_OPERATION_COPY_SIZE = 5ULL * 1024 * 1024 * 1024;
        static const constexpr UInt64 DEFAULT_MAX_CONNECTIONS = 1024;
        static const constexpr UInt64 DEFAULT_MAX_UNEXPECTED_WRITE_ERRORS_RETRIES = 4;

        void setEmptyFieldsByDefault();
        void updateFromSettingsIfEmpty(const Settings & settings);
    };

    S3::AuthSettings auth_settings;
    RequestSettings request_settings;

    inline bool operator==(const S3Settings & other) const
    {
        return auth_settings == other.auth_settings && request_settings == other.request_settings;
    }
};

/// Settings for the StorageS3.
class StorageS3Settings
{
public:
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config, const Settings & settings);

    S3Settings getSettings(const String & endpoint) const;

private:
    mutable std::mutex mutex;
    std::map<const String, const S3Settings> s3_settings;
};

inline void S3Settings::RequestSettings::setEmptyFieldsByDefault()
{
    if (!max_single_read_retries)
        max_single_read_retries = DEFAULT_SINGLE_READ_RETRIES;
    if (!min_upload_part_size)
        min_upload_part_size = DEFAULT_MIN_UPLOAD_PART_SIZE;
    if (!max_upload_part_size)
        max_upload_part_size = DEFAULT_MAX_UPLOAD_PART_SIZE;
    if (!upload_part_size_multiply_factor)
        upload_part_size_multiply_factor = DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_FACTOR;
    if (!upload_part_size_multiply_parts_count_threshold)
        upload_part_size_multiply_parts_count_threshold = DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_PARTS_COUNT_THRESHOLD;
    if (!max_part_number)
        max_part_number = DEFAULT_MAX_PART_NUMBER;
    if (!max_single_part_upload_size)
        max_single_part_upload_size = DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE;
    if (!max_single_operation_copy_size)
        max_single_operation_copy_size = DEFAULT_MAX_SINGLE_OPERATION_COPY_SIZE;
    if (!max_connections)
        max_connections = DEFAULT_MAX_CONNECTIONS;
    if (!max_unexpected_write_error_retries)
        max_unexpected_write_error_retries = DEFAULT_MAX_UNEXPECTED_WRITE_ERRORS_RETRIES;
}

}
