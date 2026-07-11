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

    S3Exception(const std::string & msg, Aws::S3::S3Errors code_)
        : Exception(msg, ErrorCodes::S3_ERROR)
        , code(code_)
    {}

    Aws::S3::S3Errors getS3ErrorCode() const
    {
        return code;
    }

    bool isRetryableError() const;
    bool isAccessTokenExpiredError() const;

    S3Exception * clone() const override { return new S3Exception(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(cert-err60-cpp)

private:
    Aws::S3::S3Errors code;
};
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
