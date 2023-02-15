#include <IO/S3Common.h>

#include <Common/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "config.h"

#if USE_AWS_S3

#    include <Common/quoteString.h>

#    include <IO/WriteBufferFromString.h>
#    include <IO/HTTPHeaderEntries.h>
#    include <Storages/StorageS3Settings.h>

#    include <IO/S3/PocoHTTPClientFactory.h>
#    include <IO/S3/PocoHTTPClient.h>
#    include <IO/S3/Client.h>
#    include <IO/S3/URI.h>
#    include <IO/S3/Requests.h>
#    include <IO/S3/Credentials.h>
#    include <Common/logger_useful.h>

#    include <fstream>

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

AuthSettings AuthSettings::loadFromConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    auto access_key_id = config.getString(config_elem + ".access_key_id", "");
    auto secret_access_key = config.getString(config_elem + ".secret_access_key", "");
    auto region = config.getString(config_elem + ".region", "");
    auto server_side_encryption_customer_key_base64 = config.getString(config_elem + ".server_side_encryption_customer_key_base64", "");

    std::optional<bool> use_environment_credentials;
    if (config.has(config_elem + ".use_environment_credentials"))
        use_environment_credentials = config.getBool(config_elem + ".use_environment_credentials");

    std::optional<bool> use_insecure_imds_request;
    if (config.has(config_elem + ".use_insecure_imds_request"))
        use_insecure_imds_request = config.getBool(config_elem + ".use_insecure_imds_request");

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

    return AuthSettings
    {
        std::move(access_key_id), std::move(secret_access_key),
        std::move(region),
        std::move(server_side_encryption_customer_key_base64),
        std::move(headers),
        use_environment_credentials,
        use_insecure_imds_request
    };
}


void AuthSettings::updateFrom(const AuthSettings & from)
{
    /// Update with check for emptyness only parameters which
    /// can be passed not only from config, but via ast.

    if (!from.access_key_id.empty())
        access_key_id = from.access_key_id;
    if (!from.secret_access_key.empty())
        secret_access_key = from.secret_access_key;

    headers = from.headers;
    region = from.region;
    server_side_encryption_customer_key_base64 = from.server_side_encryption_customer_key_base64;
    use_environment_credentials = from.use_environment_credentials;
    use_insecure_imds_request = from.use_insecure_imds_request;
}

}
}
