#include <gwp_asan/guarded_pool_allocator.h>
#include <Common/memory.h>

#if USE_GWP_ASAN
namespace Memory
{
gwp_asan::GuardedPoolAllocator GuardedAlloc;
static bool guarded_alloc_initialized = []
{
     gwp_asan::options::initOptions();
     gwp_asan::options::Options &opts = gwp_asan::options::getOptions();
     opts.MaxSimultaneousAllocations = 256;
     GuardedAlloc.init(opts);

     ///std::cerr << "GwpAsan is initialized, the options are { Enabled: " << opts.Enabled
     ///          << ", MaxSimultaneousAllocations: " << opts.MaxSimultaneousAllocations
     ///          << ", SampleRate: " << opts.SampleRate << " }\n";

     return true;
}();
}
#endif
