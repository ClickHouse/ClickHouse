#include <base/defines.h>
#include <base/MemorySanitizer.h>
#include <Common/formatReadable.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/memory.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/getPageSize.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/mman.h>
#include <Common/CoroutineStack.h>

#if defined(BOOST_USE_VALGRIND)
#include <valgrind/valgrind.h>
#endif

/// Required for older Darwin builds, that lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

namespace
{
constexpr bool guardPagesEnabled()
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    return true;
#else
    return false;
#endif
}
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

namespace ProfileEvents
{
    extern const Event FiberStackAllocs;
    extern const Event FiberStackAllocBytes;
    extern const Event FiberStackAllocNanoseconds;
    extern const Event FiberStackFreeNanoseconds;
}

namespace CurrentMetrics
{
    extern const Metric FiberStacks;
    extern const Metric FiberStackBytes;
}


CoroutineStack::CoroutineStack(size_t stack_size_)
    : stack_size(stack_size_)
    , page_size(getPageSize())
{
}

boost::context::stack_context CoroutineStack::allocate() const
{
    Stopwatch watch;

    size_t num_pages = 1 + (stack_size - 1) / page_size;

    if constexpr (guardPagesEnabled())
        /// Add one page at bottom that will be used as guard-page
        num_pages += 1;

    size_t num_bytes = num_pages * page_size;
    void * data = ::aligned_alloc(page_size, num_bytes);

    if (!data)
        throw DB::ErrnoException(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate CoroutineStack");

    if constexpr (guardPagesEnabled())
    {
        /// TODO: make reports on illegal guard page access more clear.
        /// Currently we will see segfault and almost random stacktrace.
        try
        {
            memoryGuardInstall(data, page_size);
        }
        catch (...)
        {
            ::free(data);
            throw;
        }
    }

    boost::context::stack_context sctx;
    sctx.size = num_bytes;
    sctx.sp = static_cast< char * >(data) + sctx.size;
#if defined(BOOST_USE_VALGRIND)
    sctx.valgrind_stack_id = VALGRIND_STACK_REGISTER(sctx.sp, data);
#endif

    ProfileEvents::increment(ProfileEvents::FiberStackAllocs);
    ProfileEvents::increment(ProfileEvents::FiberStackAllocBytes, num_bytes);
    ProfileEvents::increment(ProfileEvents::FiberStackAllocNanoseconds, watch.elapsedNanoseconds());
    CurrentMetrics::add(CurrentMetrics::FiberStacks);
    CurrentMetrics::add(CurrentMetrics::FiberStackBytes, num_bytes);

    return sctx;
}

void CoroutineStack::deallocate(boost::context::stack_context & sctx) const
{
    Stopwatch watch;

#if defined(BOOST_USE_VALGRIND)
    VALGRIND_STACK_DEREGISTER(sctx.valgrind_stack_id);
#endif
    void * data = static_cast< char * >(sctx.sp) - sctx.size;

    if constexpr (guardPagesEnabled())
        memoryGuardRemove(data, page_size);

    ::free(data);

    CurrentMetrics::sub(CurrentMetrics::FiberStacks);
    CurrentMetrics::sub(CurrentMetrics::FiberStackBytes, sctx.size);
    ProfileEvents::increment(ProfileEvents::FiberStackFreeNanoseconds, watch.elapsedNanoseconds());
}
