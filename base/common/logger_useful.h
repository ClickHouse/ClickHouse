#pragma once

/// Macros for convenient usage of Poco logger.

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Common/CurrentThread.h>


/// TODO Remove this.
using Poco::Logger;
using Poco::Message;
using DB::LogsLevel;
using DB::CurrentThread;

/// Logs a message to a specified logger with that level.

#define LOG_IMPL(logger, priority, PRIORITY, ...) do                    \
{                                                                                 \
    const bool is_clients_log = (CurrentThread::getGroup() != nullptr) &&         \
            (CurrentThread::getGroup()->client_logs_level >= (priority));         \
    if ((logger)->is((PRIORITY)) || is_clients_log)                               \
    {                                                                             \
        std::string formatted_message = fmt::format(__VA_ARGS__);                 \
        if (auto channel = (logger)->getChannel())                                \
        {                                                                         \
            std::string file_function;                                            \
            file_function += __FILE__;                                            \
            file_function += "; ";                                                \
            file_function += __PRETTY_FUNCTION__;                                 \
            Message poco_message((logger)->name(), formatted_message,             \
                                 (PRIORITY), file_function.c_str(), __LINE__);    \
            channel->log(poco_message);                                           \
        }                                                                         \
    }                                                                             \
} while (false)


#define LOG_TRACE(logger, ...)   LOG_IMPL(logger, LogsLevel::trace, Message::PRIO_TRACE, __VA_ARGS__)
#define LOG_DEBUG(logger, ...)   LOG_IMPL(logger, LogsLevel::debug, Message::PRIO_DEBUG, __VA_ARGS__)
#define LOG_INFO(logger, ...)    LOG_IMPL(logger, LogsLevel::information, Message::PRIO_INFORMATION, __VA_ARGS__)
#define LOG_WARNING(logger, ...) LOG_IMPL(logger, LogsLevel::warning, Message::PRIO_WARNING, __VA_ARGS__)
#define LOG_ERROR(logger, ...)   LOG_IMPL(logger, LogsLevel::error, Message::PRIO_ERROR, __VA_ARGS__)
#define LOG_FATAL(logger, ...)   LOG_IMPL(logger, LogsLevel::error, Message::PRIO_FATAL, __VA_ARGS__)
