#pragma once

// Only for debugging purposes.
// This is logging for the scheduler components, normally disabled.
// Available modes (uncomment one to enable):
//#define SCHEDULER_DEBUG_COUT    // Log to stdout with colors
#define SCHEDULER_DEBUG_TRACE   // Log using LOG_TRACE // TODO(serxa): DO NOT MERGE, this is enabled to debug the scheduler issues in CI for this PR only.

#if defined(SCHEDULER_DEBUG_COUT)

#include <iostream>
#include <base/getThreadId.h>
#define SCHED_DBG(...) std::cout << fmt::format("\033[01;3{}m[{}] {} {} {}\033[00m {}:{}\n", 1 + getThreadId() % 8, getThreadId(), reinterpret_cast<void*>(this), fmt::format(__VA_ARGS__), __PRETTY_FUNCTION__, __FILE__, __LINE__)

#elif defined(SCHEDULER_DEBUG_TRACE)

#include <Common/logger_useful.h>
#define SCHED_DBG(...) LOG_TRACE(getLogger("Scheduler"), "{} {} {}:{}", fmt::format(__VA_ARGS__), __PRETTY_FUNCTION__, __FILE__, __LINE__)

#else

#include <base/defines.h>
#define SCHED_DBG(...) UNUSED(__VA_ARGS__)

#endif
