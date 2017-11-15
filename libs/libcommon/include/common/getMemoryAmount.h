#pragma once

#include <cstdint>

/**
* Returns the size of physical memory (RAM) in bytes.
* Returns 0 on unsupported platform
*/
uint64_t getMemoryAmount();
