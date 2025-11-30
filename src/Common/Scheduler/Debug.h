#pragma once

// Only for debugging purposes.
// This is logging for the scheduler components, normally disabled.

//#define SCHEDULER_DEBUG

#ifdef SCHEDULER_DEBUG
#include <iostream>
#include <base/getThreadId.h>
#define SCHED_DBG(...) std::cout << fmt::format("\033[01;3{}m[{}] {} {} {}\033[00m {}:{}\n", 1 + getThreadId() % 8, getThreadId(), reinterpret_cast<void*>(this), fmt::format(__VA_ARGS__), __PRETTY_FUNCTION__, __FILE__, __LINE__)
#else
#include <base/defines.h>
#define SCHED_DBG(...) UNUSED(__VA_ARGS__)
#endif
