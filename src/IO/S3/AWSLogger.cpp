#include <IO/S3/AWSLogger.h>
#include <IO/ReadHelpers.h>
#include <IO/Expect404ResponseScope.h>
#include <Poco/Net/HTTPResponse.h>

#include <string_view>

#if USE_AWS_S3

#include <Core/SettingsEnums.h>
#include <Common/logger_useful.h>
#include <boost/algorithm/string/predicate.hpp>
#include <aws/core/utils/logging/LogLevel.h>
#include <Poco/Logger.h>

namespace
{

const char * S3_LOGGER_TAG_NAMES[][2] = {
    {"AWSClient", "AWSClient"},
    {"AWSAuthV4Signer", "AWSClient (AWSAuthV4Signer)"},
};

const std::pair<DB::LogsLevel, Poco::Message::Priority> & convertLogLevel(Aws::Utils::Logging::LogLevel log_level)
{
    /// We map levels to our own logger 1 to 1 except INFO+ levels. In most cases we fail over such errors with retries
    /// and don't want to see them as Errors in our logs.
    static const std::unordered_map<Aws::Utils::Logging::LogLevel, std::pair<DB::LogsLevel, Poco::Message::Priority>> mapping =
    {
        {Aws::Utils::Logging::LogLevel::Off, {DB::LogsLevel::none, Poco::Message::PRIO_INFORMATION}},
        {Aws::Utils::Logging::LogLevel::Fatal, {DB::LogsLevel::information, Poco::Message::PRIO_INFORMATION}},
        {Aws::Utils::Logging::LogLevel::Error, {DB::LogsLevel::debug, Poco::Message::PRIO_DEBUG}},
        {Aws::Utils::Logging::LogLevel::Warn, {DB::LogsLevel::debug, Poco::Message::PRIO_DEBUG}},
        {Aws::Utils::Logging::LogLevel::Info, {DB::LogsLevel::debug, Poco::Message::PRIO_DEBUG}},
        {Aws::Utils::Logging::LogLevel::Debug, {DB::LogsLevel::debug, Poco::Message::PRIO_TEST}},
        {Aws::Utils::Logging::LogLevel::Trace, {DB::LogsLevel::trace, Poco::Message::PRIO_TEST}},
    };
    return mapping.at(log_level);
}

}

namespace DB::S3
{

AWSLogger::AWSLogger(bool enable_s3_requests_logging_)
    : enable_s3_requests_logging(enable_s3_requests_logging_)
{
    for (auto [tag, name] : S3_LOGGER_TAG_NAMES)
        tag_loggers[tag] = getLogger(name);

    default_logger = tag_loggers[S3_LOGGER_TAG_NAMES[0][0]];
}

Aws::Utils::Logging::LogLevel AWSLogger::GetLogLevel() const
{
    if (enable_s3_requests_logging)
        return Aws::Utils::Logging::LogLevel::Trace;
    return Aws::Utils::Logging::LogLevel::Info;
}

namespace
{

/// Mute logs from `AWSXMLClient::BuildAWSError` for S3 wrong-signing-region 400
bool isWrongSigningRegionMuted(const std::string_view message)
{
    static constexpr std::string_view  prefix = "HTTP response code: 400";
    if (!message.starts_with(prefix))
        return false;

    return boost::icontains(message, "x-amz-bucket-region");
}

    /// This is the way, how to mute scary logs from `AWSXMLClient::BuildAWSError`
    /// about 404 when 404 is the expected response
bool is404Muted(std::string_view message)
{
    if (!Expect404ResponseScope::is404Expected())
        return false;

    static constexpr std::string_view prefix = "HTTP response code: ";
    if (!message.starts_with(prefix))
        return false;

    if (message.size() < prefix.size() + 3)
        return false;

    UInt64 code = 0;
    if (!tryParse<UInt64>(code, message.data() + prefix.size(), 3))
        return false;

    return code == Poco::Net::HTTPResponse::HTTP_NOT_FOUND;
}

bool isExpectedAwsHttpResponseMuted(const std::string_view message)
{
    return isWrongSigningRegionMuted(message) || is404Muted(message);
}
}

void AWSLogger::Log(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, ...) // NOLINT
{
    if (format_str && isExpectedAwsHttpResponseMuted(format_str))
        return;
    callLogImpl(log_level, tag, format_str); /// FIXME. Variadic arguments?
}

void AWSLogger::LogStream(Aws::Utils::Logging::LogLevel log_level, const char * tag, const Aws::OStringStream & message_stream)
{
    const auto str = message_stream.str();
    if (isExpectedAwsHttpResponseMuted(str))
        return;
    callLogImpl(log_level, tag, str.c_str());
}

void AWSLogger::callLogImpl(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * message)
{
    const auto & [level, prio] = convertLogLevel(log_level);
    if (tag_loggers.contains(tag))
        LOG_IMPL(tag_loggers[tag], level, prio, fmt::runtime(message));
    else
        LOG_IMPL(default_logger, level, prio, "{}: {}", tag, message);
}

void AWSLogger::vaLog(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, va_list)
{
    if (format_str && isExpectedAwsHttpResponseMuted(format_str))
        return;
    callLogImpl(log_level, tag, format_str); /// FIXME. Variadic arguments?
}

}

#endif
