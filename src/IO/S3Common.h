#pragma once

#include <IO/S3/Client.h>
#include <IO/HTTPHeaderEntries.h>
#include <base/types.h>
#include <Common/Exception.h>

#include <unordered_set>

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

namespace DB
{
struct ProxyConfigurationResolver;

namespace S3
{

HTTPHeaderEntries getHTTPHeaders(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config, std::string header_key = "header");
ServerSideEncryptionKMSConfig getSSEKMSConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config);

}
}
