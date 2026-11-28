#include <allocator/ExtentCache.h>

namespace jemalloc
{

bool ExtentCache::init(ThreadState * /*tsdn*/, ExtentState state_, unsigned idx_, bool delay_coalesce_)
{
    if (mutex.init("extents", MutexRank::EXTENTS, MutexLockOrder::RankExclusive))
        return true;
    state = state_;
    idx = idx_;
    delay_coalesce = delay_coalesce_;
    extent_set.init(state_);
    guarded_extent_set.init(state_);
    return false;
}

}
