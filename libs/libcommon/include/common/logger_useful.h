#pragma once

/// Macros for convenient usage of Poco logger.

#include <sstream>
#include <atomic>
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

#if defined(POCO_CLICKHOUSE_PATCH)

#define LOG_TRACE(logger, message) do { \
    const bool is_clients_log = (CurrentThread::getGroup() != nullptr) && (CurrentThread::getGroup()->client_logs_level >= LogsLevel::trace); \
    if ((logger)->trace() || is_clients_log) {\
    std::stringstream oss_internal_rare; \
    oss_internal_rare << message; \
    if (is_clients_log) {\
        (logger)->force_log(oss_internal_rare.str(), Message::PRIO_TRACE); \
    } else { \
        (logger)->trace(oss_internal_rare.str()); \
    }}} while(false)

#define LOG_DEBUG(logger, message) do { \
    const bool is_clients_log = (CurrentThread::getGroup() != nullptr) && (CurrentThread::getGroup()->client_logs_level >= LogsLevel::debug); \
    if ((logger)->debug() || is_clients_log) {\
    std::stringstream oss_internal_rare; \
    oss_internal_rare << message; \
    if (is_clients_log) {\
        (logger)->force_log(oss_internal_rare.str(), Message::PRIO_DEBUG); \
    } else { \
        (logger)->debug(oss_internal_rare.str()); \
    }}} while(false)

#define LOG_INFO(logger, message) do { \
    const bool is_clients_log = (CurrentThread::getGroup() != nullptr) && (CurrentThread::getGroup()->client_logs_level >= LogsLevel::information); \
    if ((logger)->information() || is_clients_log) {\
    std::stringstream oss_internal_rare; \
    oss_internal_rare << message; \
    if (is_clients_log) {\
        (logger)->force_log(oss_internal_rare.str(), Message::PRIO_INFORMATION); \
    } else { \
        (logger)->information(oss_internal_rare.str()); \
    }}} while(false)

#define LOG_WARNING(logger, message) do { \
    const bool is_clients_log = (CurrentThread::getGroup() != nullptr) && (CurrentThread::getGroup()->client_logs_level >= LogsLevel::warning); \
    if ((logger)->warning() || is_clients_log) {\
    std::stringstream oss_internal_rare; \
    oss_internal_rare << message; \
    if (is_clients_log) {\
        (logger)->force_log(oss_internal_rare.str(), Message::PRIO_WARNING); \
    } else { \
        (logger)->warning(oss_internal_rare.str()); \
    }}} while(false)

#define LOG_ERROR(logger, message) do { \
    const bool is_clients_log = (CurrentThread::getGroup() != nullptr) && (CurrentThread::getGroup()->client_logs_level >= LogsLevel::error); \
    if ((logger)->error() || is_clients_log) {\
    std::stringstream oss_internal_rare; \
    oss_internal_rare << message; \
    if (is_clients_log) {\
        (logger)->force_log(oss_internal_rare.str(), Message::PRIO_ERROR); \
    } else { \
        (logger)->error(oss_internal_rare.str()); \
    }}} while(false)

#else

#define LOG_TRACE(logger, message) do { \
    if ((logger)->trace()) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->trace(oss_internal_rare.str());}} while(false)

#define LOG_DEBUG(logger, message) do { \
    if ((logger)->debug()) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->debug(oss_internal_rare.str());}} while(false)

#define LOG_INFO(logger, message) do { \
    if ((logger)->information()) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->information(oss_internal_rare.str());}} while(false)

#define LOG_WARNING(logger, message) do { \
    if ((logger)->warning()) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->warning(oss_internal_rare.str());}} while(false)

#define LOG_ERROR(logger, message) do { \
    if ((logger)->error()) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->error(oss_internal_rare.str());}} while(false)

#endif

