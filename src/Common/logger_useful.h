#pragma once

/// Macros for convenient usage of Poco logger.

#include <fmt/format.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/LoggingFormatStringHelpers.h>

namespace Poco { class Logger; }


#define LogToStr(x, y) std::make_unique<LogToStrImpl>(x, y)
#define LogFrequencyLimiter(x, y) std::make_unique<LogFrequencyLimiterIml>(x, y)

using LogSeriesLimiterPtr = std::shared_ptr<LogSeriesLimiter>;

namespace
{
    [[maybe_unused]] const ::Poco::Logger * getLogger(const ::Poco::Logger * logger) { return logger; }
    [[maybe_unused]] const ::Poco::Logger * getLogger(const std::atomic<::Poco::Logger *> & logger) { return logger.load(); }
    [[maybe_unused]] std::unique_ptr<LogToStrImpl> getLogger(std::unique_ptr<LogToStrImpl> && logger) { return logger; }
    [[maybe_unused]] std::unique_ptr<LogFrequencyLimiterIml> getLogger(std::unique_ptr<LogFrequencyLimiterIml> && logger) { return logger; }
    [[maybe_unused]] LogSeriesLimiterPtr getLogger(LogSeriesLimiterPtr & logger) { return logger; }
}

#define LOG_IMPL_FIRST_ARG(X, ...) X

/// Logs a message to a specified logger with that level.
/// If more than one argument is provided,
///  the first argument is interpreted as a template with {}-substitutions
///  and the latter arguments are treated as values to substitute.
/// If only one argument is provided, it is treated as a message without substitutions.

#define LOG_IMPL(logger, priority, PRIORITY, ...) do                              \
{                                                                                 \
    auto _logger = ::getLogger(logger);                                           \
    const bool _is_clients_log = (DB::CurrentThread::getGroup() != nullptr) &&    \
        (DB::CurrentThread::get().getClientLogsLevel() >= (priority));            \
    if (_is_clients_log || _logger->is((PRIORITY)))                               \
    {                                                                             \
        std::string formatted_message = numArgs(__VA_ARGS__) > 1 ? fmt::format(__VA_ARGS__) : firstArg(__VA_ARGS__); \
        formatStringCheckArgsNum(__VA_ARGS__);                                    \
        if (auto _channel = _logger->getChannel())                                \
        {                                                                         \
            std::string file_function;                                            \
            file_function += __FILE__;                                            \
            file_function += "; ";                                                \
            file_function += __PRETTY_FUNCTION__;                                 \
            Poco::Message poco_message(_logger->name(), formatted_message,        \
                (PRIORITY), file_function.c_str(), __LINE__, tryGetStaticFormatString(LOG_IMPL_FIRST_ARG(__VA_ARGS__))); \
            _channel->log(poco_message);                                          \
        }                                                                         \
        ProfileEvents::incrementForLogMessage(PRIORITY);                          \
    }                                                                             \
} while (false)


#define LOG_TEST(logger, ...)    LOG_IMPL(logger, DB::LogsLevel::test, Poco::Message::PRIO_TEST, __VA_ARGS__)
#define LOG_TRACE(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::trace, Poco::Message::PRIO_TRACE, __VA_ARGS__)
#define LOG_DEBUG(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::debug, Poco::Message::PRIO_DEBUG, __VA_ARGS__)
#define LOG_INFO(logger, ...)    LOG_IMPL(logger, DB::LogsLevel::information, Poco::Message::PRIO_INFORMATION, __VA_ARGS__)
#define LOG_WARNING(logger, ...) LOG_IMPL(logger, DB::LogsLevel::warning, Poco::Message::PRIO_WARNING, __VA_ARGS__)
#define LOG_ERROR(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::error, Poco::Message::PRIO_ERROR, __VA_ARGS__)
#define LOG_FATAL(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::error, Poco::Message::PRIO_FATAL, __VA_ARGS__)
