#pragma once

#include "config.h"

#include <base/defines.h>

#if USE_GWP_ASAN
#include <gwp_asan/guarded_pool_allocator.h>
#include <gwp_asan/optional/options_parser.h>
#endif

namespace Memory
{

#if USE_GWP_ASAN
static gwp_asan::GuardedPoolAllocator GuardedAlloc;

static struct InitGwpAsan
{
    InitGwpAsan()
    {
         gwp_asan::options::initOptions();
         gwp_asan::options::Options &opts = gwp_asan::options::getOptions();
         GuardedAlloc.init(opts);
    }

    static bool isInit()
    {
        return GuardedAlloc.getAllocatorState()->GuardedPagePoolEnd != 0;
    }
} init_gwp_asan;
#endif

}
