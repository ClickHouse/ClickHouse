#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#include <common/types.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

struct HttpHeader
{
    const String name;
    const String value;
};

using HeaderCollection = std::vector<HttpHeader>;

struct S3AuthSettings
{
    const String access_key_id;
    const String secret_access_key;
    const String server_side_encryption_customer_key_base64;

    const HeaderCollection headers;

    std::optional<bool> use_environment_credentials;
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
