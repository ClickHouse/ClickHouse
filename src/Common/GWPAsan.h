#pragma once

#include "config.h"

#if USE_GWP_ASAN

#include <gwp_asan/guarded_pool_allocator.h>

namespace Memory
{

extern gwp_asan::GuardedPoolAllocator GuardedAlloc;

bool isGWPAsanError(uintptr_t fault_address);

void printGWPAsanReport(uintptr_t fault_address);

}

#endif
