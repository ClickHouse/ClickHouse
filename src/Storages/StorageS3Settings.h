#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#include <base/types.h>
#include <Interpreters/Context_fwd.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{
struct HttpHeader
{
    String name;
    String value;

    inline bool operator==(const HttpHeader & other) const { return name == other.name && value == other.value; }
};

using HeaderCollection = std::vector<HttpHeader>;

struct Settings;

struct S3Settings
{
    struct AuthSettings
    {
        String access_key_id;
        String secret_access_key;
        String region;
        String server_side_encryption_customer_key_base64;

        HeaderCollection headers;

        std::optional<bool> use_environment_credentials;
        std::optional<bool> use_insecure_imds_request;

        inline bool operator==(const AuthSettings & other) const
        {
            return access_key_id == other.access_key_id && secret_access_key == other.secret_access_key
                && region == other.region
                && server_side_encryption_customer_key_base64 == other.server_side_encryption_customer_key_base64
                && headers == other.headers
                && use_environment_credentials == other.use_environment_credentials
                && use_insecure_imds_request == other.use_insecure_imds_request;
        }
    };

    struct ReadWriteSettings
    {
        size_t max_single_read_retries = 0;
        size_t min_upload_part_size = 0;
        size_t upload_part_size_multiply_factor = 0;
        size_t upload_part_size_multiply_parts_count_threshold = 0;
        size_t max_single_part_upload_size = 0;
        size_t max_connections = 0;
    };

    AuthSettings auth_settings;
    ReadWriteSettings rw_settings;
};

/// Settings for the StorageS3.
class StorageS3Settings
{
public:
    StorageS3Settings() = default;
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config, const Settings & settings);

    S3Settings getSettings(const String & endpoint) const;

private:
    mutable std::mutex mutex;
    std::map<const String, const S3Settings> s3_settings;
};

}
