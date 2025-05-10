#pragma once

#include <cstdint>

#if defined(MEMORY_ACCESS_TRACING)

extern int ENABLE_TRACE;

extern int FAULT;

uint64_t getMemoryAccessCount();

void resetMemoryAccessCount();

void enableMemoryAccessesCoverage();

void disableMemoryAccessesCoverage();

#endif
