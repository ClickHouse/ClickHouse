#pragma once
#include <cstdint>

/// Obtain thread id from OS. The value is cached in thread local variable.
uint64_t getThreadId();
