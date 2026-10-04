#include <allocator/ExtentCache.h>

namespace jemalloc
{

bool ExtentCache::init(ThreadState * /*tsdn*/, ExtentState state_, unsigned ind_, bool delay_coalesce_)
{
    if (mtx.init("extents", MutexRank::EXTENTS, MutexLockOrder::RankExclusive))
        return true;
    state = state_;
    ind = ind_;
    delay_coalesce = delay_coalesce_;
    eset.init(state_);
    guarded_eset.init(state_);
    return false;
}

}
