#pragma once
#include <cstddef>

namespace MemoryAllocationTracker
{

void enable_allocation_tracker();
void disable_allocation_tracker();

void track_alloc(void * ptr, std::size_t size);
void track_free(void * ptr, std::size_t size);

}
