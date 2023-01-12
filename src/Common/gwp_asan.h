#pragma once

#include <base/defines.h>

#include <gwp_asan/guarded_pool_allocator.h>
#include <gwp_asan/optional/options_parser.h>

namespace Memory
{

static gwp_asan::GuardedPoolAllocator GuardedAlloc;

inline ALWAYS_INLINE void initGWPAsan()
{
    gwp_asan::options::initOptions();
    gwp_asan::options::Options &opts = gwp_asan::options::getOptions();
    GuardedAlloc.init(opts);
}

}
