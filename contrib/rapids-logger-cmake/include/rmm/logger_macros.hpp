/*
 * SPDX-FileCopyrightText: Copyright (c) 2025, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <rapids_logger/log_levels.h>

// Default to info level if not specified.
#if !defined(RMM_LOG_ACTIVE_LEVEL)
#define RMM_LOG_ACTIVE_LEVEL RAPIDS_LOGGER_LOG_LEVEL_INFO
#endif

// Macros for easier logging, similar to spdlog.
#define RMM_LOGGER_CALL(logger, level, ...) (logger).log(level, __VA_ARGS__)

#if RMM_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_TRACE
#define RMM_LOG_TRACE(...) \
  RMM_LOGGER_CALL(rmm::default_logger(), rapids_logger::level_enum::trace, __VA_ARGS__)
#else
#define RMM_LOG_TRACE(...) (void)0
#endif

#if RMM_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_DEBUG
#define RMM_LOG_DEBUG(...) \
  RMM_LOGGER_CALL(rmm::default_logger(), rapids_logger::level_enum::debug, __VA_ARGS__)
#else
#define RMM_LOG_DEBUG(...) (void)0
#endif

#if RMM_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_INFO
#define RMM_LOG_INFO(...) RMM_LOGGER_CALL(rmm::default_logger(), rapids_logger::level_enum::info, __VA_ARGS__)
#else
#define RMM_LOG_INFO(...) (void)0
#endif

#if RMM_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_WARN
#define RMM_LOG_WARN(...) RMM_LOGGER_CALL(rmm::default_logger(), rapids_logger::level_enum::warn, __VA_ARGS__)
#else
#define RMM_LOG_WARN(...) (void)0
#endif

#if RMM_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_ERROR
#define RMM_LOG_ERROR(...) \
  RMM_LOGGER_CALL(rmm::default_logger(), rapids_logger::level_enum::error, __VA_ARGS__)
#else
#define RMM_LOG_ERROR(...) (void)0
#endif

#if RMM_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_CRITICAL
#define RMM_LOG_CRITICAL(...) \
  RMM_LOGGER_CALL(rmm::default_logger(), rapids_logger::level_enum::critical, __VA_ARGS__)
#else
#define RMM_LOG_CRITICAL(...) (void)0
#endif
