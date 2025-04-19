#include <base/defines.h>
#include <Common/formatReadable.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/memory.h>
#include <base/getPageSize.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/mman.h>
#include <Common/FiberStack.h>

#if defined(BOOST_USE_VALGRIND)
#include <valgrind/valgrind.h>
#endif

/// Required for older Darwin builds, that lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif


namespace DB::ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}


FiberStack::FiberStack(size_t stack_size_)
    : stack_size(stack_size_)
    , page_size(getPageSize())
{
}

boost::context::stack_context FiberStack::allocate() const
{
    size_t num_pages = 1 + (stack_size - 1) / page_size;
    size_t num_bytes = (num_pages + 1) * page_size; /// Add one page at bottom that will be used as guard-page

    void * vp = ::mmap(nullptr, num_bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (MAP_FAILED == vp)
        throw DB::ErrnoException(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "FiberStack: Cannot mmap {}.", ReadableSize(num_bytes));

    /// TODO: make reports on illegal guard page access more clear.
    /// Currently we will see segfault and almost random stacktrace.
    try
    {
        memoryGuardInstall(vp, page_size);
    }
    catch (...)
    {
        ::munmap(vp, num_bytes);
        throw;
    }

    /// Do not count guard page in memory usage.
    auto trace = CurrentMemoryTracker::alloc(num_pages * page_size);
    trace.onAlloc(vp, num_pages * page_size);

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
#if defined(BOOST_USE_VALGRIND)
    VALGRIND_STACK_DEREGISTER(sctx.valgrind_stack_id);
#endif
    void * vp = static_cast< char * >(sctx.sp) - sctx.size;
    ::munmap(vp, sctx.size);

    /// Do not count guard page in memory usage.
    auto trace = CurrentMemoryTracker::free(sctx.size - page_size);
    trace.onFree(vp, sctx.size - page_size);
}
