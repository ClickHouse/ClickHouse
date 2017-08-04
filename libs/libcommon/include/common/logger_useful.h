#pragma once

/// Macros for convenient usage of Poco logger.

#include <sstream>
#include <Poco/Logger.h>

#ifndef QUERY_PREVIEW_LENGTH
#define QUERY_PREVIEW_LENGTH 160
#endif

using Poco::Logger;

/// Logs a message to a specified logger with that level.

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

#define LOG_NOTICE(logger, message) do { \
    if ((logger)->notice()) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->notice(oss_internal_rare.str());}} while(false)

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

#define LOG_CRITICAL(logger, message) do { \
    if ((logger)->critical()) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->critical(oss_internal_rare.str());}} while(false)

#define LOG_FATAL(logger, message) do { \
    if ((logger)->fatal()) {\
    std::stringstream oss_internal_rare;    \
    oss_internal_rare << message; \
    (logger)->fatal(oss_internal_rare.str());}} while(false)
