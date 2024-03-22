#pragma once

#include <cstdint>

/** Returns the size of physical memory (RAM) in bytes.
  * Returns 0 on unsupported platform or if it cannot determine the size of physical memory.
  */
uint64_t getMemoryAmountOrZero();

/** Throws exception if it cannot determine the size of physical memory.
  */
uint64_t getMemoryAmount();
