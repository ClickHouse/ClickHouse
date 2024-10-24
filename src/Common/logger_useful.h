#pragma once

/// Macros for convenient usage of Poco logger.
#include <unistd.h>
#include <fmt/args.h>
#include <fmt/format.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Common/AtomicLogger.h>
#include <Common/CurrentThreadHelpers.h>
#include <Common/Logger.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/ProfileEvents.h>


#define LogToStr(x, y) std::make_unique<LogToStrImpl>(x, y)
#define LogFrequencyLimiter(x, y) std::make_unique<LogFrequencyLimiterImpl>(x, y)

using LogSeriesLimiterPtr = std::shared_ptr<LogSeriesLimiter>;

namespace impl
{
    [[maybe_unused]] inline LoggerPtr getLoggerHelper(const LoggerPtr & logger) { return logger; }
    [[maybe_unused]] inline LoggerPtr getLoggerHelper(const DB::AtomicLogger & logger) { return logger.load(); }
    [[maybe_unused]] inline const ::Poco::Logger * getLoggerHelper(const ::Poco::Logger * logger) { return logger; }
    [[maybe_unused]] inline std::unique_ptr<LogToStrImpl> getLoggerHelper(std::unique_ptr<LogToStrImpl> && logger) { return logger; }
    [[maybe_unused]] inline std::unique_ptr<LogFrequencyLimiterImpl> getLoggerHelper(std::unique_ptr<LogFrequencyLimiterImpl> && logger) { return logger; }
    [[maybe_unused]] inline LogSeriesLimiterPtr getLoggerHelper(LogSeriesLimiterPtr & logger) { return logger; }
}

#define LOG_IMPL_FIRST_ARG(X, ...) X

/// Copy-paste from contrib/libpq/include/c.h
/// There's no easy way to count the number of arguments without evaluating these arguments...
#define CH_VA_ARGS_NARGS(...) \
    CH_VA_ARGS_NARGS_(__VA_ARGS__, \
                   63,62,61,60,                   \
                   59,58,57,56,55,54,53,52,51,50, \
                   49,48,47,46,45,44,43,42,41,40, \
                   39,38,37,36,35,34,33,32,31,30, \
                   29,28,27,26,25,24,23,22,21,20, \
                   19,18,17,16,15,14,13,12,11,10, \
                   9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define CH_VA_ARGS_NARGS_( \
    _01,_02,_03,_04,_05,_06,_07,_08,_09,_10, \
    _11,_12,_13,_14,_15,_16,_17,_18,_19,_20, \
    _21,_22,_23,_24,_25,_26,_27,_28,_29,_30, \
    _31,_32,_33,_34,_35,_36,_37,_38,_39,_40, \
    _41,_42,_43,_44,_45,_46,_47,_48,_49,_50, \
    _51,_52,_53,_54,_55,_56,_57,_58,_59,_60, \
    _61,_62,_63, N, ...) \
    (N)

#define LINE_NUM_AS_STRING_IMPL2(x) #x
#define LINE_NUM_AS_STRING_IMPL(x) LINE_NUM_AS_STRING_IMPL2(x)
#define LINE_NUM_AS_STRING LINE_NUM_AS_STRING_IMPL(__LINE__)
#define MESSAGE_FOR_EXCEPTION_ON_LOGGING "Failed to write a log message: " __FILE__ ":" LINE_NUM_AS_STRING "\n"

/// Logs a message to a specified logger with that level.
/// If more than one argument is provided,
///  the first argument is interpreted as a template with {}-substitutions
///  and the latter arguments are treated as values to substitute.
/// If only one argument is provided, it is treated as a message without substitutions.

#define LOG_IMPL(logger, priority, PRIORITY, ...) do                                                                \
{                                                                                                                   \
    auto _logger = ::impl::getLoggerHelper(logger);                                                                 \
    const bool _is_clients_log = DB::currentThreadHasGroup() && DB::currentThreadLogsLevel() >= (priority);         \
    if (!_is_clients_log && !_logger->is((PRIORITY)))                                                               \
        break;                                                                                                      \
                                                                                                                    \
    try                                                                                                             \
    {                                                                                                               \
        ProfileEvents::incrementForLogMessage(PRIORITY);                                                            \
        auto _channel = _logger->getChannel();                                                                      \
        if (!_channel)                                                                                              \
            break;                                                                                                  \
                                                                                                                    \
        constexpr size_t _nargs = CH_VA_ARGS_NARGS(__VA_ARGS__);                                                    \
        using LogTypeInfo = FormatStringTypeInfo<std::decay_t<decltype(LOG_IMPL_FIRST_ARG(__VA_ARGS__))>>;          \
                                                                                                                    \
        std::string_view _format_string;                                                                            \
        std::string _formatted_message;                                                                             \
        std::vector<std::string> _format_string_args;                                                               \
                                                                                                                    \
        if constexpr (LogTypeInfo::is_static)                                                                       \
        {                                                                                                           \
            formatStringCheckArgsNum(LOG_IMPL_FIRST_ARG(__VA_ARGS__), _nargs - 1);                                  \
            _format_string = ConstexprIfsAreNotIfdefs<LogTypeInfo::is_static>::getStaticFormatString(LOG_IMPL_FIRST_ARG(__VA_ARGS__)); \
        }                                                                                                           \
                                                                                                                    \
        constexpr bool is_preformatted_message = !LogTypeInfo::is_static && LogTypeInfo::has_format;                \
        if constexpr (is_preformatted_message)                                                                      \
        {                                                                                                           \
            static_assert(_nargs == 1 || !is_preformatted_message);                                                 \
            ConstexprIfsAreNotIfdefs<is_preformatted_message>::getPreformatted(LOG_IMPL_FIRST_ARG(__VA_ARGS__)).apply(_formatted_message, _format_string, _format_string_args);  \
        }                                                                                                           \
        else                                                                                                        \
        {                                                                                                           \
             _formatted_message = _nargs == 1 ? firstArg(__VA_ARGS__) : ConstexprIfsAreNotIfdefs<!is_preformatted_message>::getArgsAndFormat(_format_string_args, __VA_ARGS__); \
        }                                                                                                           \
                                                                                                                    \
        std::string _file_function = __FILE__ "; ";                                                                 \
        _file_function += __PRETTY_FUNCTION__;                                                                      \
        Poco::Message _poco_message(_logger->name(), std::move(_formatted_message),                                 \
            (PRIORITY), _file_function.c_str(), __LINE__, _format_string, _format_string_args);                     \
        _channel->log(_poco_message);                                                                               \
    }                                                                                                               \
    catch (const Poco::Exception & logger_exception)                                                                \
    {                                                                                                               \
        ::write(STDERR_FILENO, static_cast<const void *>(MESSAGE_FOR_EXCEPTION_ON_LOGGING), sizeof(MESSAGE_FOR_EXCEPTION_ON_LOGGING)); \
        const std::string & logger_exception_message = logger_exception.message();                                  \
        ::write(STDERR_FILENO, static_cast<const void *>(logger_exception_message.data()), logger_exception_message.size()); \
    }                                                                                                               \
    catch (const std::exception & logger_exception)                                                                 \
    {                                                                                                               \
        ::write(STDERR_FILENO, static_cast<const void *>(MESSAGE_FOR_EXCEPTION_ON_LOGGING), sizeof(MESSAGE_FOR_EXCEPTION_ON_LOGGING)); \
        const char * logger_exception_message = logger_exception.what();                                            \
        ::write(STDERR_FILENO, static_cast<const void *>(logger_exception_message), strlen(logger_exception_message)); \
    }                                                                                                               \
    catch (...)                                                                                                     \
    {                                                                                                               \
        ::write(STDERR_FILENO, static_cast<const void *>(MESSAGE_FOR_EXCEPTION_ON_LOGGING), sizeof(MESSAGE_FOR_EXCEPTION_ON_LOGGING)); \
    }                                                                                                               \
} while (false)


#define LOG_TEST(logger, ...)    LOG_IMPL(logger, DB::LogsLevel::test, Poco::Message::PRIO_TEST, __VA_ARGS__)
#define LOG_TRACE(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::trace, Poco::Message::PRIO_TRACE, __VA_ARGS__)
#define LOG_DEBUG(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::debug, Poco::Message::PRIO_DEBUG, __VA_ARGS__)
#define LOG_INFO(logger, ...)    LOG_IMPL(logger, DB::LogsLevel::information, Poco::Message::PRIO_INFORMATION, __VA_ARGS__)
#define LOG_WARNING(logger, ...) LOG_IMPL(logger, DB::LogsLevel::warning, Poco::Message::PRIO_WARNING, __VA_ARGS__)
#define LOG_ERROR(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::error, Poco::Message::PRIO_ERROR, __VA_ARGS__)
#define LOG_FATAL(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::error, Poco::Message::PRIO_FATAL, __VA_ARGS__)
