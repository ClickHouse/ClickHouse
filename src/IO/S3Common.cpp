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

#include <cctype>
#include <string_view>

#include "config.h"

#if USE_AWS_S3

#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>


namespace DB
{

String sanitizeS3ErrorMessage(String message)
{
    static constexpr std::string_view arn_prefix = "arn:";
    static constexpr std::string_view replacement = "[REDACTED_AWS_ARN]";

    auto is_arn_char = [](char c)
    {
        return std::isalnum(static_cast<unsigned char>(c)) || c == ':' || c == '/' || c == '_' || c == '-'
            || c == '+' || c == '=' || c == ',' || c == '.' || c == '@';
    };

    size_t pos = 0;
    while ((pos = message.find(arn_prefix, pos)) != String::npos)
    {
        size_t end = pos + arn_prefix.size();
        while (end < message.size() && is_arn_char(message[end]))
            ++end;

        message.replace(pos, end - pos, replacement);
        pos += replacement.size();
    }

    return message;
}

PreformattedMessage sanitizeS3PreformattedMessage(PreformattedMessage msg)
{
    msg.text = sanitizeS3ErrorMessage(std::move(msg.text));
    /// `format_string_args` holds stringified argument values that get
    /// propagated into structured logs / telemetry (e.g.
    /// `Exception::message_format_string_args`,
    /// `system.query_log.exception_format_string_args`). Redact AWS ARNs in
    /// each arg too, otherwise they leak through that channel even after the
    /// human-readable `text` is sanitized. `format_string` is a literal
    /// template (e.g. `"{}"`), so it is left untouched.
    for (auto & arg : msg.format_string_args)
        arg = sanitizeS3ErrorMessage(std::move(arg));
    return msg;
}

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
