#pragma once

#include <cstdint>

#if defined(MEMORY_ACCESS_TRACING)

/// Get the number of memory accesses since application start or last reset
uint64_t getMemoryAccessCount();

/// Reset the memory access counter
void resetMemoryAccessCount();

#endif
