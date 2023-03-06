#pragma once

#include "config.h"

#if USE_AWS_S3
#include <aws/core/utils/logging/LogSystemInterface.h>

#include <Common/logger_useful.h>

namespace DB::S3
{
class AWSLogger final : public Aws::Utils::Logging::LogSystemInterface
{
public:
    explicit AWSLogger(bool enable_s3_requests_logging_);

    ~AWSLogger() final = default;

    Aws::Utils::Logging::LogLevel GetLogLevel() const final;

    void Log(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, ...) final; // NOLINT

    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char * tag, const Aws::OStringStream & message_stream) final;

    void callLogImpl(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * message);

    void Flush() final {}

private:
    Poco::Logger * default_logger;
    bool enable_s3_requests_logging;
    std::unordered_map<String, Poco::Logger *> tag_loggers;
};

}

#endif
