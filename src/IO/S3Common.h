#pragma once

#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/Client.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Core/Field.h>
#include <Poco/Util/AbstractConfiguration.h>

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
    S3Exception(Aws::S3::S3Errors code_, FormatStringHelper<Args...> fmt, Args &&... args)
        : Exception(PreformattedMessage{fmt.format(std::forward<Args>(args)...)}, ErrorCodes::S3_ERROR), code(code_)
    {
    }

    S3Exception(const std::string & msg, Aws::S3::S3Errors code_, String exception_name_ = {})
        : Exception(msg, ErrorCodes::S3_ERROR)
        , code(code_)
        , exception_name(std::move(exception_name_))
    {}

    /// Preserves the static format string (system.text_log / system.errors grouping) while also
    /// carrying the canonical S3 error name — build msg with PreformattedMessage::create.
    S3Exception(PreformattedMessage && msg, Aws::S3::S3Errors code_, String exception_name_)
        : Exception(std::move(msg), ErrorCodes::S3_ERROR)
        , code(code_)
        , exception_name(std::move(exception_name_))
    {}

    Aws::S3::S3Errors getS3ErrorCode() const
    {
        return code;
    }

    /// The canonical S3 error code string from the response XML `<Code>` (e.g. "PreconditionFailed",
    /// "NoSuchKey") as reported by `Aws::Client::AWSError::GetExceptionName`. Errors unmodeled by the
    /// SDK (a conditional-PUT 412 is one) have `getS3ErrorCode` == UNKNOWN, so this name is the only
    /// machine-readable discriminator. Empty when the throw site did not attach it.
    /// Not `Exception::name`; this is the AWS `<Code>` string.
    const String & getExceptionName() const
    {
        return exception_name;
    }

    bool isRetryableError() const;
    bool isAccessTokenExpiredError() const;

    /// True for a conditional-request 412 (a lost `If-Match`/`If-None-Match`). The thrown exception
    /// discards the HTTP status, so it matches on the canonical `<Code>` name and the raw message —
    /// see `S3::isPreconditionFailedError` for the full (response-code-aware) policy.
    bool isPreconditionFailed() const;

    S3Exception * clone() const override { return new S3Exception(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(cert-err60-cpp)

private:
    Aws::S3::S3Errors code;
    String exception_name;
};

namespace S3
{

/// One policy for "is this error a conditional-request 412 (`PreconditionFailed`)?", shared by the
/// retry strategy and the CA conditional delete/copy paths. The HTTP status is authoritative — a
/// non-AWS body (e.g. RustFS) leaves the SDK-parsed `ExceptionName` empty — with the canonical `<Code>`
/// name and the raw message as fallbacks. Fail-safe by direction: over-matching only forces a caller
/// re-validate, never a false success.
template <typename ErrorType>
inline bool isPreconditionFailedError(const Aws::Client::AWSError<ErrorType> & error)
{
    return error.GetResponseCode() == Aws::Http::HttpResponseCode::PRECONDITION_FAILED
        || error.GetExceptionName() == "PreconditionFailed"
        || error.GetMessage().find("PreconditionFailed") != std::string::npos;
}

/// Error-name classifiers factored out of the CAS conditional-write outcome mapping
/// (`CasRequestControl.cpp`), so the name lists live next to the other S3 error classifiers here
/// and are available for reuse.
bool isMalformedRequestError(const S3Exception & e);
bool isEntityTooLargeError(const S3Exception & e);
bool isAccessDeniedError(const S3Exception & e);

}

}

#endif

namespace Poco::Util
{
    class AbstractConfiguration;
};

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct ProxyConfigurationResolver;

namespace S3
{

HTTPHeaderEntries getHTTPHeaders(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config, std::string header_key = "header");
ServerSideEncryptionKMSConfig getSSEKMSConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config);

template <typename SettingFieldRef>
bool setValueFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path,
    SettingFieldRef & field)
{
    if (!config.has(path))
        return false;

    auto which = field.getValue().getType();
    if (which == Field::Types::String)
        field.setValue(config.getString(path));
    else if (which == Field::Types::Bool)
        field.setValue(config.getBool(path));
    else if (isInt64OrUInt64FieldType(which))
    {
        const auto type_name = field.getTypeName();
        if (type_name == "UInt64" || type_name == "Int64")
            field.setValue(config.getUInt64(path));
        else
            field.setValue(Field(config.getString(path)));
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type: {}", field.getTypeName());

    return true;
}

}
}
