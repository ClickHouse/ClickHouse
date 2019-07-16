#pragma once

/// Macros for convenient usage of Poco logger.

#include <sstream>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Poco/Version.h>
#include <Core/SettingsCommon.h>
#include <Common/CurrentThread.h>

#ifndef QUERY_PREVIEW_LENGTH
#define QUERY_PREVIEW_LENGTH 160
#endif

using Poco::Logger;
using Poco::Message;
using DB::LogsLevel;
using DB::CurrentThread;

/// Logs a message to a specified logger with that level.

#define LOG_SIMPLE(logger, message, priority, PRIORITY)                                         \
    const bool is_clients_log = (CurrentThread::getGroup() != nullptr) &&                       \
            (CurrentThread::getGroup()->client_logs_level >= (priority));                       \
    if ((logger)->is((PRIORITY)) || is_clients_log) {                                           \
        std::stringstream oss_internal_rare;                                                    \
        oss_internal_rare << message;                                                           \
        auto channel = (logger)->getChannel();                                                  \
        channel->log(Message("", oss_internal_rare.str(), (PRIORITY)));                         \
    }                                                                                           \

#define LOG_WARNING(logger, message) do { LOG_SIMPLE(logger, message, LogsLevel::warning, Message::PRIO_WARNING ) } while(false)
#define LOG_TRACE(logger, message)   do { LOG_SIMPLE(logger, message, LogsLevel::trace, Message::PRIO_TRACE) } while(false)
#define LOG_DEBUG(logger, message)   do { LOG_SIMPLE(logger, message, LogsLevel::debug, Message::PRIO_DEBUG) } while(false)
#define LOG_INFO(logger, message)    do { LOG_SIMPLE(logger, message, LogsLevel::information, Message::PRIO_INFORMATION) } while(false)
#define LOG_ERROR(logger, message)   do { LOG_SIMPLE(logger, message, LogsLevel::error, Message::PRIO_ERROR) } while(false)


