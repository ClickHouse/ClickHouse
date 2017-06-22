#pragma once
#include <cstdint>

namespace DB
{
/**
* Returns the size of physical memory (RAM) in bytes.
* Returns 0 on unsupported platform
*/
uint64_t getMemoryAmount();
}
