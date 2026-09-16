/*
 * SPDX-FileCopyrightText: Copyright (c) 2025, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <rapids_logger/log_levels.h>

// Default to info level if not specified.
#if !defined(KVIKIO_LOG_ACTIVE_LEVEL)
#define KVIKIO_LOG_ACTIVE_LEVEL RAPIDS_LOGGER_LOG_LEVEL_INFO
#endif

// Macros for easier logging, similar to spdlog.
#define KVIKIO_LOGGER_CALL(logger, level, ...) (logger).log(level, __VA_ARGS__)

#if KVIKIO_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_TRACE
#define KVIKIO_LOG_TRACE(...) \
  KVIKIO_LOGGER_CALL(kvikio::default_logger(), rapids_logger::level_enum::trace, __VA_ARGS__)
#else
#define KVIKIO_LOG_TRACE(...) (void)0
#endif

#if KVIKIO_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_DEBUG
#define KVIKIO_LOG_DEBUG(...) \
  KVIKIO_LOGGER_CALL(kvikio::default_logger(), rapids_logger::level_enum::debug, __VA_ARGS__)
#else
#define KVIKIO_LOG_DEBUG(...) (void)0
#endif

#if KVIKIO_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_INFO
#define KVIKIO_LOG_INFO(...) KVIKIO_LOGGER_CALL(kvikio::default_logger(), rapids_logger::level_enum::info, __VA_ARGS__)
#else
#define KVIKIO_LOG_INFO(...) (void)0
#endif

#if KVIKIO_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_WARN
#define KVIKIO_LOG_WARN(...) KVIKIO_LOGGER_CALL(kvikio::default_logger(), rapids_logger::level_enum::warn, __VA_ARGS__)
#else
#define KVIKIO_LOG_WARN(...) (void)0
#endif

#if KVIKIO_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_ERROR
#define KVIKIO_LOG_ERROR(...) \
  KVIKIO_LOGGER_CALL(kvikio::default_logger(), rapids_logger::level_enum::error, __VA_ARGS__)
#else
#define KVIKIO_LOG_ERROR(...) (void)0
#endif

#if KVIKIO_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_CRITICAL
#define KVIKIO_LOG_CRITICAL(...) \
  KVIKIO_LOGGER_CALL(kvikio::default_logger(), rapids_logger::level_enum::critical, __VA_ARGS__)
#else
#define KVIKIO_LOG_CRITICAL(...) (void)0
#endif
