#pragma once
#include <base/defines.h>
#include <boost/context/stack_context.hpp>
#include <Common/formatReadable.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <base/getPageSize.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/mman.h>

#if defined(BOOST_USE_VALGRIND)
#include <valgrind/valgrind.h>
#endif

namespace DB::ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

/// This is an implementation of allocator for fiber stack.
/// The reference implementation is protected_fixedsize_stack from boost::context.
/// This implementation additionally track memory usage. It is the main reason why it is needed.
class FiberStack
{
private:
    size_t stack_size;
    size_t page_size = 0;
public:
    /// NOTE: If you see random segfaults in CI and stack starts from boost::context::...fiber...
    /// probably it worth to try to increase stack size for coroutines.
    ///
    /// Current value is just enough for all tests in our CI. It's not selected in some special
    /// way. We will have 40 pages with 4KB page size.
    static constexpr size_t default_stack_size = 192 * 1024; /// 64KB was not enough for tests

    explicit FiberStack(size_t stack_size_ = default_stack_size) : stack_size(stack_size_)
    {
        page_size = getPageSize();
    }

    boost::context::stack_context allocate() const
    {
        size_t num_pages = 1 + (stack_size - 1) / page_size;
        size_t num_bytes = (num_pages + 1) * page_size; /// Add one page at bottom that will be used as guard-page

        void * vp = ::mmap(nullptr, num_bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (MAP_FAILED == vp)
            DB::throwFromErrno(fmt::format("FiberStack: Cannot mmap {}.", ReadableSize(num_bytes)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        /// TODO: make reports on illegal guard page access more clear.
        /// Currently we will see segfault and almost random stacktrace.
        if (-1 == ::mprotect(vp, page_size, PROT_NONE))
        {
            ::munmap(vp, num_bytes);
            DB::throwFromErrno("FiberStack: cannot protect guard page", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }

        /// Do not count guard page in memory usage.
        CurrentMemoryTracker::alloc(num_pages * page_size);

        boost::context::stack_context sctx;
        sctx.size = num_bytes;
        sctx.sp = static_cast< char * >(vp) + sctx.size;
#if defined(BOOST_USE_VALGRIND)
        sctx.valgrind_stack_id = VALGRIND_STACK_REGISTER(sctx.sp, vp);
#endif
        return sctx;
    }

    void deallocate(boost::context::stack_context & sctx) const
    {
#if defined(BOOST_USE_VALGRIND)
        VALGRIND_STACK_DEREGISTER(sctx.valgrind_stack_id);
#endif
        void * vp = static_cast< char * >(sctx.sp) - sctx.size;
        ::munmap(vp, sctx.size);

        /// Do not count guard page in memory usage.
        CurrentMemoryTracker::free(sctx.size - page_size);
    }
};
