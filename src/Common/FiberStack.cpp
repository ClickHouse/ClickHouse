#include "FiberStack.h"
#include <atomic>
#include <absl/container/flat_hash_set.h>
#include <base/defines.h>
#include <base/scope_guard.h>
#include <boost/core/noncopyable.hpp>
#include <Core/ServerSettings.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Interpreters/Context.h>

namespace DB::ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

namespace ProfileEvents
{
    extern const Event FiberStackAllocationCount;
    extern const Event FiberStackAllocationMicroseconds;
    extern const Event FiberStackDeallocationCount;
    extern const Event FiberStackDeallocationMicroseconds;
    extern const Event FiberStackCacheHits;
    extern const Event FiberStackCacheMisses;
}

namespace DB::ServerSetting
{
    extern const ServerSettingsUInt32 num_fiber_stack_cache;
    extern const ServerSettingsUInt64 max_fiber_stack_cache_size_bytes;
}

namespace CurrentMetrics
{
    extern const Metric FiberStackCacheBytes;
    extern const Metric FiberStackCacheActive;
    extern const Metric FiberStackActive;
}

boost::context::stack_context FiberStack::allocate() const
{
    ProfileEvents::increment(ProfileEvents::FiberStackAllocationCount);
    Stopwatch watch;
    SCOPE_EXIT(
        ProfileEvents::increment(ProfileEvents::FiberStackAllocationMicroseconds, watch.elapsedMicroseconds());
    );

    void * vp = ::mmap(nullptr, num_bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (MAP_FAILED == vp)
        throw DB::ErrnoException(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "FiberStack: Cannot mmap {}.", ReadableSize(num_bytes));

    /// TODO: make reports on illegal guard page access more clear.
    /// Currently we will see segfault and almost random stacktrace.
    if (-1 == ::mprotect(vp, page_size, PROT_NONE))
    {
        ::munmap(vp, num_bytes);
        throw DB::ErrnoException(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "FiberStack: cannot protect guard page");
    }

    /// Do not count guard page in memory usage.
    auto trace = CurrentMemoryTracker::alloc(num_bytes - page_size);
    trace.onAlloc(vp, num_bytes - page_size);

    boost::context::stack_context sctx;
    sctx.size = num_bytes;
    sctx.sp = static_cast< char * >(vp) + sctx.size;
#if defined(BOOST_USE_VALGRIND)
    sctx.valgrind_stack_id = VALGRIND_STACK_REGISTER(sctx.sp, vp);
#endif
    return sctx;
}

void FiberStack::deallocate(boost::context::stack_context & sctx) const
{
    ProfileEvents::increment(ProfileEvents::FiberStackDeallocationCount);
    Stopwatch watch;
    SCOPE_EXIT(
        ProfileEvents::increment(ProfileEvents::FiberStackDeallocationMicroseconds, watch.elapsedMicroseconds());
    );

#if defined(BOOST_USE_VALGRIND)
    VALGRIND_STACK_DEREGISTER(sctx.valgrind_stack_id);
#endif
    void * vp = static_cast< char * >(sctx.sp) - sctx.size;
    ::munmap(vp, sctx.size);

    /// Do not count guard page in memory usage.
    auto trace = CurrentMemoryTracker::free(sctx.size - page_size);
    trace.onFree(vp, sctx.size - page_size);
}

/// A pool of fixed size fiber stacks, thread safe.
/// Other threads can borrow stacks from this pool and return them once done.
class FixedSizeFiberStackCache
{
private:
    std::mutex mutex;
    size_t max_cached_bytes; /// Maximum number of cached bytes.
    FiberStack * allocator TSA_GUARDED_BY(mutex); /// Base allocator for new stacks.
    std::list<boost::context::stack_context> free_stacks TSA_GUARDED_BY(mutex); /// List of free stacks.
    absl::flat_hash_set<void *> allocated_stacks TSA_GUARDED_BY(mutex); /// Map of allocated pointer -> allocated_stack.
    size_t cached_bytes TSA_GUARDED_BY(mutex);
#ifndef NDEBUG
    size_t page_size;
#endif

public:
    explicit FixedSizeFiberStackCache(FiberStack * base_allocator_, size_t max_cached_bytes_)
        : max_cached_bytes(max_cached_bytes_), allocator(base_allocator_), cached_bytes(0)
    {
#ifndef NDEBUG
        page_size = getPageSize();
#endif
    }
    /// Borrow a stack from the pool.
    /// If no free stacks are available, allocate a new one.
    /// If the maximum number of cached stacks is reached, return nullopt.
    std::optional<boost::context::stack_context> borrowStack()
    {
        std::lock_guard lock(mutex);
        if (free_stacks.empty())
        {
            if (cached_bytes + FiberStack::default_stack_size >= max_cached_bytes)
                return std::nullopt;
            free_stacks.push_front(allocator->allocate());
            CurrentMetrics::add(CurrentMetrics::FiberStackCacheBytes, free_stacks.front().size);
        }
        auto stack = free_stacks.front();
        free_stacks.pop_front();
        allocated_stacks.insert(stack.sp);

    /// TODO: do we need to zero out the stack in all cases since all fiber is in same process?
    /// Folly::GuardPageAllocator, boost::context::pooled_fixedsize_stack, boost::coroutines::standard_stack_allocator
    /// don't zero out the stack. glibc pthread zero out the thread local storage section of the stack.
#ifndef NDEBUG
        auto * vp = static_cast<char *>(stack.sp) - stack.size;
        ::memset(vp + page_size, 0, stack.size - page_size);
#endif
        CurrentMetrics::add(CurrentMetrics::FiberStackCacheActive);
        return stack;
    }

    /// Return a stack to the pool.
    /// If the stack is not allocated by this pool, do nothing.
    bool returnStack(boost::context::stack_context & sctx)
    {
        std::lock_guard lock(mutex);
        if (auto it = allocated_stacks.find(sctx.sp); it != allocated_stacks.end())
        {
            free_stacks.push_front(sctx);
            allocated_stacks.erase(it);
            CurrentMetrics::sub(CurrentMetrics::FiberStackCacheActive);
            return true;
        }
        return false;
    }
};

/// A manager of fiber stack caches.
class FiberStackCacheManager : public boost::noncopyable
{
public:

    static FiberStackCacheManager & instance()
    {
        static FiberStackCacheManager manager;
        return manager;
    }

    FixedSizeFiberStackCache & getCacheForCurrentThread()
    {
        /// Using round-robin to distribute load is better than binding thread to cache
        /// because single thread can create many fibers at the same time. Using thread-local
        /// cache limits the number of fibers that can use cached stacks.
        return *caches[idx.fetch_add(1, std::memory_order_relaxed) % caches.size()];
    }

    FiberStack & getBaseAllocator()
    {
        return base_allocator;
    }

private:

    FiberStackCacheManager()
    {
        auto num_caches = DB::Context::getGlobalContextInstance()->getServerSettings()[DB::ServerSetting::num_fiber_stack_cache];
        auto max_cached_bytes = DB::Context::getGlobalContextInstance()->getServerSettings()[DB::ServerSetting::max_fiber_stack_cache_size_bytes];
        /// We create multiple caches to reduce the lock contentions
        caches.resize(num_caches);
        for (auto & cache : caches)
            cache = std::make_unique<FixedSizeFiberStackCache>(&base_allocator, std::max(static_cast<size_t>(max_cached_bytes / num_caches), 32 * FiberStack::default_stack_size));
    }

    FiberStack base_allocator;
    std::vector<std::unique_ptr<FixedSizeFiberStackCache>> caches;
    std::atomic_uint16_t idx = 0;
};

FixedSizeFiberStackWithCache::FixedSizeFiberStackWithCache(size_t /*stack_size_*/)
    : cache(&FiberStackCacheManager::instance().getCacheForCurrentThread())
    , fallback_allocator(&FiberStackCacheManager::instance().getBaseAllocator())
{
    CurrentMetrics::add(CurrentMetrics::FiberStackActive);
}

FixedSizeFiberStackWithCache::FixedSizeFiberStackWithCache(const FixedSizeFiberStackWithCache & other)
    : cache(other.cache)
    , fallback_allocator(other.fallback_allocator)
{
    CurrentMetrics::add(CurrentMetrics::FiberStackActive);
}

FixedSizeFiberStackWithCache::~FixedSizeFiberStackWithCache()
{
    CurrentMetrics::sub(CurrentMetrics::FiberStackActive);
}

boost::context::stack_context FixedSizeFiberStackWithCache::allocate()
{
    assert(cache);
    assert(fallback_allocator);
    auto stack = cache->borrowStack();
    if (stack)
    {
        ProfileEvents::increment(ProfileEvents::FiberStackCacheHits);
        return *stack;
    }
    ProfileEvents::increment(ProfileEvents::FiberStackCacheMisses);
    return fallback_allocator->allocate();
}

void FixedSizeFiberStackWithCache::deallocate(boost::context::stack_context & sctx)
{
    assert(cache);
    assert(fallback_allocator);
    if (!cache->returnStack(sctx))
        fallback_allocator->deallocate(sctx);
}


