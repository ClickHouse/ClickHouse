#pragma once

/// Macros for convenient usage of Poco logger.

#include <sstream>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Poco/Version.h>
#include <Common/CurrentThread.h>

#ifndef QUERY_PREVIEW_LENGTH
#define QUERY_PREVIEW_LENGTH 160
#endif

using Poco::Logger;
using Poco::Message;
using DB::LogsLevel;
using DB::CurrentThread;

/// Logs a message to a specified logger with that level.

#define LOG_SIMPLE(logger, message, priority, PRIORITY) do                        \
{                                                                                 \
    const bool is_clients_log = (CurrentThread::getGroup() != nullptr) &&         \
            (CurrentThread::getGroup()->client_logs_level >= (priority));         \
    if ((logger)->is((PRIORITY)) || is_clients_log)                               \
    {                                                                             \
        std::stringstream oss_internal_rare;                                      \
        oss_internal_rare << message;                                             \
        if (auto channel = (logger)->getChannel())                                \
        {                                                                         \
            std::string file_function;                                            \
            file_function += __FILE__;                                            \
            file_function += "; ";                                                \
            file_function += __PRETTY_FUNCTION__;                                 \
            Message poco_message((logger)->name(), oss_internal_rare.str(),       \
                                 (PRIORITY), file_function.c_str(), __LINE__);    \
            channel->log(poco_message);                                           \
        }                                                                         \
    }                                                                             \
} while (false)


#define LOG_TRACE(logger, message)   LOG_SIMPLE(logger, message, LogsLevel::trace, Message::PRIO_TRACE)
#define LOG_DEBUG(logger, message)   LOG_SIMPLE(logger, message, LogsLevel::debug, Message::PRIO_DEBUG)
#define LOG_INFO(logger, message)    LOG_SIMPLE(logger, message, LogsLevel::information, Message::PRIO_INFORMATION)
#define LOG_WARNING(logger, message) LOG_SIMPLE(logger, message, LogsLevel::warning, Message::PRIO_WARNING)
#define LOG_ERROR(logger, message)   LOG_SIMPLE(logger, message, LogsLevel::error, Message::PRIO_ERROR)
#define LOG_FATAL(logger, message)   LOG_SIMPLE(logger, message, LogsLevel::error, Message::PRIO_FATAL)

