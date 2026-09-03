#pragma once

#include <Common/Logger.h>

/// Sleeps for random duration between 0 and a specified number of milliseconds, optionally outputs a logging message about that.
/// This function can be used to add random delays in tests.
void randomDelayForMaxMilliseconds(uint64_t milliseconds, LoggerPtr log = nullptr, const char * start_of_message = nullptr);
void randomDelayForMaxSeconds(uint64_t seconds, LoggerPtr log = nullptr, const char * start_of_message = nullptr);
