/*
 * SPDX-FileCopyrightText: Copyright (c) 2025, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <rapids_logger/log_levels.h>

// Default to info level if not specified.
#if !defined(CUDF_LOG_ACTIVE_LEVEL)
#define CUDF_LOG_ACTIVE_LEVEL RAPIDS_LOGGER_LOG_LEVEL_INFO
#endif

// Macros for easier logging, similar to spdlog.
#define CUDF_LOGGER_CALL(logger, level, ...) (logger).log(level, __VA_ARGS__)

#if CUDF_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_TRACE
#define CUDF_LOG_TRACE(...) \
  CUDF_LOGGER_CALL(cudf::default_logger(), rapids_logger::level_enum::trace, __VA_ARGS__)
#else
#define CUDF_LOG_TRACE(...) (void)0
#endif

#if CUDF_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_DEBUG
#define CUDF_LOG_DEBUG(...) \
  CUDF_LOGGER_CALL(cudf::default_logger(), rapids_logger::level_enum::debug, __VA_ARGS__)
#else
#define CUDF_LOG_DEBUG(...) (void)0
#endif

#if CUDF_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_INFO
#define CUDF_LOG_INFO(...) CUDF_LOGGER_CALL(cudf::default_logger(), rapids_logger::level_enum::info, __VA_ARGS__)
#else
#define CUDF_LOG_INFO(...) (void)0
#endif

#if CUDF_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_WARN
#define CUDF_LOG_WARN(...) CUDF_LOGGER_CALL(cudf::default_logger(), rapids_logger::level_enum::warn, __VA_ARGS__)
#else
#define CUDF_LOG_WARN(...) (void)0
#endif

#if CUDF_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_ERROR
#define CUDF_LOG_ERROR(...) \
  CUDF_LOGGER_CALL(cudf::default_logger(), rapids_logger::level_enum::error, __VA_ARGS__)
#else
#define CUDF_LOG_ERROR(...) (void)0
#endif

#if CUDF_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_CRITICAL
#define CUDF_LOG_CRITICAL(...) \
  CUDF_LOGGER_CALL(cudf::default_logger(), rapids_logger::level_enum::critical, __VA_ARGS__)
#else
#define CUDF_LOG_CRITICAL(...) (void)0
#endif
