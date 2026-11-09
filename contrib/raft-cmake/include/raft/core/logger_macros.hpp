/*
 * SPDX-FileCopyrightText: Copyright (c) 2025, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <rapids_logger/log_levels.h>

// Default to info level if not specified.
#if !defined(RAFT_LOG_ACTIVE_LEVEL)
#define RAFT_LOG_ACTIVE_LEVEL RAPIDS_LOGGER_LOG_LEVEL_INFO
#endif

// Macros for easier logging, similar to spdlog.
#define RAFT_LOGGER_CALL(logger, level, ...) (logger).log(level, __VA_ARGS__)

#if RAFT_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_TRACE
#define RAFT_LOG_TRACE(...) \
  RAFT_LOGGER_CALL(raft::default_logger(), rapids_logger::level_enum::trace, __VA_ARGS__)
#else
#define RAFT_LOG_TRACE(...) (void)0
#endif

#if RAFT_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_DEBUG
#define RAFT_LOG_DEBUG(...) \
  RAFT_LOGGER_CALL(raft::default_logger(), rapids_logger::level_enum::debug, __VA_ARGS__)
#else
#define RAFT_LOG_DEBUG(...) (void)0
#endif

#if RAFT_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_INFO
#define RAFT_LOG_INFO(...) RAFT_LOGGER_CALL(raft::default_logger(), rapids_logger::level_enum::info, __VA_ARGS__)
#else
#define RAFT_LOG_INFO(...) (void)0
#endif

#if RAFT_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_WARN
#define RAFT_LOG_WARN(...) RAFT_LOGGER_CALL(raft::default_logger(), rapids_logger::level_enum::warn, __VA_ARGS__)
#else
#define RAFT_LOG_WARN(...) (void)0
#endif

#if RAFT_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_ERROR
#define RAFT_LOG_ERROR(...) \
  RAFT_LOGGER_CALL(raft::default_logger(), rapids_logger::level_enum::error, __VA_ARGS__)
#else
#define RAFT_LOG_ERROR(...) (void)0
#endif

#if RAFT_LOG_ACTIVE_LEVEL <= RAPIDS_LOGGER_LOG_LEVEL_CRITICAL
#define RAFT_LOG_CRITICAL(...) \
  RAFT_LOGGER_CALL(raft::default_logger(), rapids_logger::level_enum::critical, __VA_ARGS__)
#else
#define RAFT_LOG_CRITICAL(...) (void)0
#endif
