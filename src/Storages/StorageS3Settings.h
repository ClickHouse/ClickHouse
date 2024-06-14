#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#include <base/types.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Throttler_fwd.h>

#include <IO/S3Common.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

struct Settings;
class NamedCollection;

struct S3Settings
{
    struct RequestSettings
    {
        struct PartUploadSettings
        {
            size_t strict_upload_part_size = 0;
            size_t min_upload_part_size = 16 * 1024 * 1024;
            size_t max_upload_part_size = 5ULL * 1024 * 1024 * 1024;
            size_t upload_part_size_multiply_factor = 2;
            size_t upload_part_size_multiply_parts_count_threshold = 500;
            size_t max_inflight_parts_for_one_file = 20;
            size_t max_part_number = 10000;
            size_t max_single_part_upload_size = 32 * 1024 * 1024;
            size_t max_single_operation_copy_size = 5ULL * 1024 * 1024 * 1024;
            String storage_class_name;

            void updateFromSettings(const Settings & settings) { updateFromSettingsImpl(settings, true); }
            void validate();

        private:
            PartUploadSettings() = default;
            explicit PartUploadSettings(const Settings & settings);
            explicit PartUploadSettings(const NamedCollection & collection);
            PartUploadSettings(
                const Poco::Util::AbstractConfiguration & config,
                const String & config_prefix,
                const Settings & settings,
                String setting_name_prefix = {});

            void updateFromSettingsImpl(const Settings & settings, bool if_changed);

            friend struct RequestSettings;
        };

    private:
        PartUploadSettings upload_settings = {};

    public:
        size_t max_single_read_retries = 4;
        size_t max_connections = 1024;
        bool check_objects_after_upload = false;
        size_t max_unexpected_write_error_retries = 4;
        size_t list_object_keys_size = 1000;
        ThrottlerPtr get_request_throttler;
        ThrottlerPtr put_request_throttler;
        size_t retry_attempts = 10;
        size_t request_timeout_ms = 30000;
        bool allow_native_copy = true;

        bool throw_on_zero_files_match = false;

        const PartUploadSettings & getUploadSettings() const { return upload_settings; }
        PartUploadSettings & getUploadSettings() { return upload_settings; }

        void setStorageClassName(const String & storage_class_name) { upload_settings.storage_class_name = storage_class_name; }

        RequestSettings() = default;
        explicit RequestSettings(const Settings & settings);
        explicit RequestSettings(const NamedCollection & collection);

        /// What's the setting_name_prefix, and why do we need it?
        /// There are (at least) two config sections where s3 settings can be specified:
        /// * settings for s3 disk (clickhouse/storage_configuration/disks)
        /// * settings for s3 storage (clickhouse/s3), which are also used for backups
        /// Even though settings are the same, in case of s3 disk they are prefixed with "s3_"
        /// ("s3_max_single_part_upload_size"), but in case of s3 storage they are not
        /// (   "max_single_part_upload_size"). Why this happened is a complete mystery to me.
        RequestSettings(
            const Poco::Util::AbstractConfiguration & config,
            const String & config_prefix,
            const Settings & settings,
            String setting_name_prefix = {});

        void updateFromSettings(const Settings & settings);

    private:
        void updateFromSettingsImpl(const Settings & settings, bool if_changed);
    };

    S3::AuthSettings auth_settings;
    RequestSettings request_settings;
};

/// Settings for the StorageS3.
class StorageS3Settings
{
public:
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config, const Settings & settings);

    S3Settings getSettings(const String & endpoint, const String & user, bool ignore_user = false) const;

private:
    mutable std::mutex mutex;
    std::map<const String, const S3Settings> s3_settings;
};

}
