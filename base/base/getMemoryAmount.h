#pragma once

#include <cstdint>

/// Returns the size in bytes of physical memory (RAM) available to the process. The value can
/// be smaller than the total available RAM available to the system due to cgroups settings.
/// Returns 0 on unsupported platform or if it cannot determine the size of physical memory.
uint64_t getMemoryAmountOrZero();

/// Throws exception if it cannot determine the size of physical memory.
uint64_t getMemoryAmount();
