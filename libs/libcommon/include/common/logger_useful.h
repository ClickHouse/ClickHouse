#pragma once

/// Macros for convenient usage of Poco logger.

#include <sstream>
#include <atomic>
#include <Poco/Logger.h>
#include <Core/SettingsCommon.h>
#include <Common/CurrentThread.h>

#ifndef QUERY_PREVIEW_LENGTH
#define QUERY_PREVIEW_LENGTH 160
#endif

using Poco::Logger;
using DB::LogsLevel;
using DB::CurrentThread;


/// Logs a message to a specified logger with that level.

#define LOG_TRACE(logger, message) do { \
    if ((logger)->trace() || CurrentThread::getGroup()->client_logs_level >= LogsLevel::trace) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->trace(oss_internal_rare.str());}} while(false)

#define LOG_DEBUG(logger, message) do { \
    if ((logger)->debug() || CurrentThread::getGroup()->client_logs_level >= LogsLevel::debug) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->debug(oss_internal_rare.str());}} while(false)

#define LOG_INFO(logger, message) do { \
    if ((logger)->information() || CurrentThread::getGroup()->client_logs_level >= LogsLevel::information) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->information(oss_internal_rare.str());}} while(false)

#define LOG_WARNING(logger, message) do { \
    if ((logger)->warning() || CurrentThread::getGroup()->client_logs_level >= LogsLevel::warning) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->warning(oss_internal_rare.str());}} while(false)

#define LOG_ERROR(logger, message) do { \
    if ((logger)->error() || CurrentThread::getGroup()->client_logs_level >= LogsLevel::error) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->error(oss_internal_rare.str());}} while(false)
