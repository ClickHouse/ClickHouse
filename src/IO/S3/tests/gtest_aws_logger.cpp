#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <sstream>

#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

#include <Poco/AutoPtr.h>
#include <Poco/StreamChannel.h>

#include <Common/logger_useful.h>

#include <IO/Expect404ResponseScope.h>
#include <IO/S3/AWSLogger.h>

namespace
{

/// Captures log lines written to the shared `AWSClient` logger used by `DB::S3::AWSLogger`.
struct ScopedAWSClientLoggerRecording
{
    LoggerPtr logger = getLogger("AWSClient");
    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::StreamChannel> channel = new Poco::StreamChannel(stream);
    Poco::Channel * old_channel = nullptr;
    int old_level = 0;

    ScopedAWSClientLoggerRecording()
    {
        old_channel = logger->getChannel();
        old_level = logger->getLevel();
        logger->setChannel(channel.get());
        logger->setLevel("trace");
    }

    ~ScopedAWSClientLoggerRecording()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    std::string captured() const { return stream.str(); }
};

}

/// Stream starts with HTTP response code: 400 and contains x-amz-bucket-region
/// Deliberately mixed-case for checking case insensitivity
TEST(IOTestAwsLogger, LogStreamMutesWrongSigningRegion400)
{
    ScopedAWSClientLoggerRecording recording;
    DB::S3::AWSLogger aws_logger(false);

    Aws::OStringStream oss;
    oss << "HTTP response code: 400 ... X-Amz-Bucket-Region: us-east-1";

    aws_logger.LogStream(Aws::Utils::Logging::LogLevel::Info, "AWSClient", oss);
    EXPECT_TRUE(recording.captured().empty());
}

/// 400 without bucket-region hint, output expected
TEST(IOTestAwsLogger, LogStreamDoesNotMute400WithoutBucketRegionHeader)
{
    ScopedAWSClientLoggerRecording recording;
    DB::S3::AWSLogger aws_logger(false);

    Aws::OStringStream oss;
    oss << "HTTP response code: 400 some other error";

    aws_logger.LogStream(Aws::Utils::Logging::LogLevel::Info, "AWSClient", oss);
    EXPECT_FALSE(recording.captured().empty());
    EXPECT_NE(recording.captured().find("HTTP response code: 400"), std::string::npos);
}

/// Scope + 404 -> no output
TEST(IOTestAwsLogger, LogStreamMutesExpected404InsideScope)
{
    ScopedAWSClientLoggerRecording recording;
    DB::S3::AWSLogger aws_logger(false);

    {
        DB::Expect404ResponseScope expect_404;
        Aws::OStringStream oss;
        oss << "HTTP response code: 404";
        aws_logger.LogStream(Aws::Utils::Logging::LogLevel::Info, "AWSClient", oss);
    }
    EXPECT_TRUE(recording.captured().empty());
}

/// No scope + 404 -> outputs
TEST(IOTestAwsLogger, LogStreamDoesNotMute404OutsideScope)
{
    ScopedAWSClientLoggerRecording recording;
    DB::S3::AWSLogger aws_logger(false);

    Aws::OStringStream oss;
    oss << "HTTP response code: 404";
    aws_logger.LogStream(Aws::Utils::Logging::LogLevel::Info, "AWSClient", oss);
    EXPECT_FALSE(recording.captured().empty());
    EXPECT_NE(recording.captured().find("HTTP response code: 404"), std::string::npos);
}

/// Scope + 403 -> outputs
TEST(IOTestAwsLogger, LogStreamDoesNotMuteNon404InsideExpect404Scope)
{
    ScopedAWSClientLoggerRecording recording;
    DB::S3::AWSLogger aws_logger(false);

    {
        DB::Expect404ResponseScope expect_404;
        Aws::OStringStream oss;
        oss << "HTTP response code: 403";
        aws_logger.LogStream(Aws::Utils::Logging::LogLevel::Info, "AWSClient", oss);
    }
    EXPECT_FALSE(recording.captured().empty());
    EXPECT_NE(recording.captured().find("HTTP response code: 403"), std::string::npos);
}

#endif
