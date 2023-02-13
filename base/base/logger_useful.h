#pragma once

/// Macros for convenient usage of Poco logger.

#include <fmt/format.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Common/CurrentThread.h>


namespace
{
    template <typename... Ts> constexpr size_t numArgs(Ts &&...) { return sizeof...(Ts); }
    template <typename T, typename... Ts> constexpr auto firstArg(T && x, Ts &&...) { return std::forward<T>(x); }
    /// For implicit conversion of fmt::basic_runtime<> to char* for std::string ctor
    template <typename T, typename... Ts> constexpr auto firstArg(fmt::basic_runtime<T> && data, Ts &&...) { return data.str.data(); }
}


/// Logs a message to a specified logger with that level.
/// If more than one argument is provided,
///  the first argument is interpreted as template with {}-substitutions
///  and the latter arguments treat as values to substitute.
/// If only one argument is provided, it is threat as message without substitutions.

#define LOG_IMPL(logger, priority, PRIORITY, ...) do                              \
{                                                                                 \
    const bool is_clients_log = (DB::CurrentThread::getGroup() != nullptr) &&     \
        (DB::CurrentThread::getGroup()->client_logs_level >= (priority));         \
    if ((logger)->is((PRIORITY)) || is_clients_log)                               \
    {                                                                             \
        std::string formatted_message = numArgs(__VA_ARGS__) > 1 ? fmt::format(__VA_ARGS__) : firstArg(__VA_ARGS__); \
        if (auto channel = (logger)->getChannel())                                \
        {                                                                         \
            std::string file_function;                                            \
            file_function += __FILE__;                                            \
            file_function += "; ";                                                \
            file_function += __PRETTY_FUNCTION__;                                 \
            Poco::Message poco_message((logger)->name(), formatted_message,       \
                                 (PRIORITY), file_function.c_str(), __LINE__);    \
            channel->log(poco_message);                                           \
        }                                                                         \
    }                                                                             \
} while (false)


#define LOG_TEST(logger, ...)    LOG_IMPL(logger, DB::LogsLevel::test, Poco::Message::PRIO_TEST, __VA_ARGS__)
#define LOG_TRACE(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::trace, Poco::Message::PRIO_TRACE, __VA_ARGS__)
#define LOG_DEBUG(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::debug, Poco::Message::PRIO_DEBUG, __VA_ARGS__)
#define LOG_INFO(logger, ...)    LOG_IMPL(logger, DB::LogsLevel::information, Poco::Message::PRIO_INFORMATION, __VA_ARGS__)
#define LOG_WARNING(logger, ...) LOG_IMPL(logger, DB::LogsLevel::warning, Poco::Message::PRIO_WARNING, __VA_ARGS__)
#define LOG_ERROR(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::error, Poco::Message::PRIO_ERROR, __VA_ARGS__)
#define LOG_FATAL(logger, ...)   LOG_IMPL(logger, DB::LogsLevel::error, Poco::Message::PRIO_FATAL, __VA_ARGS__)
