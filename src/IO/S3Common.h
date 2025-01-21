#pragma once

#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/HTTPHeaderEntries.h>

#include <string>
#include <optional>
#include <unordered_set>

#include "config.h"

#if USE_AWS_S3

#include <base/types.h>
#include <Common/Exception.h>
#include <Common/Throttler_fwd.h>

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

class RemoteHostFilter;

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
    static AuthSettings loadFromConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config);

    std::string access_key_id;
    std::string secret_access_key;
    std::string session_token;
    std::string region;
    std::string server_side_encryption_customer_key_base64;
    ServerSideEncryptionKMSConfig server_side_encryption_kms_config;

    HTTPHeaderEntries headers;

    std::optional<bool> use_environment_credentials;
    std::optional<bool> use_insecure_imds_request;
    std::optional<uint64_t> expiration_window_seconds;
    std::optional<bool> no_sign_request;

    std::unordered_set<std::string> users;

    bool hasUpdates(const AuthSettings & other) const;
    void updateFrom(const AuthSettings & from);

    bool canBeUsedByUser(const String & user) const;

private:
    bool operator==(const AuthSettings & other) const = default;
};

}
