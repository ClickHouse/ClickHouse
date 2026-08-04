#include <IO/S3Common.h>

#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Core/Settings.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/String.h>

#include "config.h"

#if USE_AWS_S3

#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>


namespace DB
{

bool S3Exception::isRetryableError() const
{
    /// Looks like these list is quite conservative, add more codes if you wish
    static const UnorderedSetWithMemoryTracking<Aws::S3::S3Errors> unretryable_errors = {
        Aws::S3::S3Errors::NO_SUCH_KEY,
        Aws::S3::S3Errors::ACCESS_DENIED,
        Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID,
        Aws::S3::S3Errors::INVALID_SIGNATURE,
        Aws::S3::S3Errors::NO_SUCH_UPLOAD,
        Aws::S3::S3Errors::NO_SUCH_BUCKET,
    };

    return !unretryable_errors.contains(code);
}

bool S3Exception::isAccessTokenExpiredError() const
{
    return code == Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID || code == Aws::S3::S3Errors::ACCESS_DENIED || code == Aws::S3::S3Errors::INVALID_SIGNATURE || code == Aws::S3::S3Errors::UNKNOWN;
}

bool S3Exception::isPreconditionFailed() const
{
    /// See `S3::isPreconditionFailedError`. The thrown exception no longer carries the HTTP status, so
    /// only the name and raw message are available here — fail-safe: matching too broadly maps a hard
    /// error to a retryable re-validate, never a false success.
    return exception_name == "PreconditionFailed"
        || message().find("PreconditionFailed") != std::string::npos;
}

namespace S3
{

/// A synchronous rejection PROVING the request was never applied — matched by the canonical S3 error
/// code STRING (many of these are UNKNOWN in the SDK's modeled enum, mirroring
/// ObjectStorageBackend::finalizeConditionalWrite's own name-first matching) plus the modeled enum
/// value where one exists, belt-and-suspenders.
bool isMalformedRequestError(const S3Exception & e)
{
    const String & name = e.getExceptionName();
    return name == "MalformedXML" || name == "MalformedPOSTRequest" || name == "InvalidArgument"
        || name == "InvalidRequest" || name == "InvalidBucketName" || name == "KeyTooLongError"
        || e.getS3ErrorCode() == Aws::S3::S3Errors::INVALID_PARAMETER_VALUE
        || e.getS3ErrorCode() == Aws::S3::S3Errors::INVALID_REQUEST
        || e.getS3ErrorCode() == Aws::S3::S3Errors::VALIDATION;
}

bool isEntityTooLargeError(const S3Exception & e)
{
    /// No modeled enum value for this error — name-only match, same as PreconditionFailed elsewhere.
    return e.getExceptionName() == "EntityTooLarge";
}

bool isAccessDeniedError(const S3Exception & e)
{
    const String & name = e.getExceptionName();
    return name == "AccessDenied" || name == "InvalidAccessKeyId" || name == "SignatureDoesNotMatch"
        || name == "InvalidToken" || name == "ExpiredToken" || name == "AccountProblem"
        || e.getS3ErrorCode() == Aws::S3::S3Errors::ACCESS_DENIED
        || e.getS3ErrorCode() == Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID
        || e.getS3ErrorCode() == Aws::S3::S3Errors::SIGNATURE_DOES_NOT_MATCH
        || e.getS3ErrorCode() == Aws::S3::S3Errors::INVALID_CLIENT_TOKEN_ID;
}

}

}

#endif

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 s3_max_get_burst;
    extern const SettingsUInt64 s3_max_get_rps;
    extern const SettingsUInt64 s3_max_put_burst;
    extern const SettingsUInt64 s3_max_put_rps;
}

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace S3
{

HTTPHeaderEntries getHTTPHeaders(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config, const std::string header_key)
{
    HTTPHeaderEntries headers;
    Poco::Util::AbstractConfiguration::Keys subconfig_keys;
    config.keys(config_elem, subconfig_keys);
    for (const std::string & subkey : subconfig_keys)
    {
        if (subkey.starts_with(header_key))
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


}

}
