#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#include <base/types.h>

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

struct S3AuthSettings
{
    String access_key_id;
    String secret_access_key;
    String region;
    String server_side_encryption_customer_key_base64;

    HeaderCollection headers;

    std::optional<bool> use_environment_credentials;
    std::optional<bool> use_insecure_imds_request;

    size_t multipart_write_thread_pool_size = 1;

    inline bool operator==(const S3AuthSettings & other) const
    {
        return access_key_id == other.access_key_id && secret_access_key == other.secret_access_key
            && region == other.region
            && server_side_encryption_customer_key_base64 == other.server_side_encryption_customer_key_base64
            && headers == other.headers
            && use_environment_credentials == other.use_environment_credentials
            && use_insecure_imds_request == other.use_insecure_imds_request
            && multipart_write_thread_pool_size == other.multipart_write_thread_pool_size;
    }
};

/// Settings for the StorageS3.
class StorageS3Settings
{
public:
    StorageS3Settings() = default;
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    S3AuthSettings getSettings(const String & endpoint) const;

private:
    mutable std::mutex mutex;
    std::map<const String, const S3AuthSettings> settings;
};

}
