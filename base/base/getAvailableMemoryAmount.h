#pragma once

#include <cstdint>

/** Returns the size of currently available physical memory (RAM) in bytes.
  * Returns 0 on unsupported platform or if it cannot determine the size of physical memory.
  */
uint64_t getAvailableMemoryAmountOrZero();

/** Throws exception if it cannot determine the size of physical memory.
  */
uint64_t getAvailableMemoryAmount();
