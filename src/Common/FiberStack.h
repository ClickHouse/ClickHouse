#pragma once
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <base/defines.h>
#include <boost/context/stack_context.hpp>
#include <Common/formatReadable.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <base/getPageSize.h>
#include <boost/core/noncopyable.hpp>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/mman.h>

#if defined(BOOST_USE_VALGRIND)
#include <valgrind/valgrind.h>
#endif

/// Required for older Darwin builds, that lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

/// This is an implementation of allocator for fiber stack.
/// The reference implementation is protected_fixedsize_stack from boost::context.
/// This implementation additionally track memory usage. It is the main reason why it is needed.
class FiberStack
{
private:
    size_t stack_size;
    size_t page_size = 0;
    
    /// The real number of pages and bytes allocated for the stack.
    size_t num_pages = 0;
    size_t num_bytes = 0;
public:
    /// NOTE: If you see random segfaults in CI and stack starts from boost::context::...fiber...
    /// probably it worth to try to increase stack size for coroutines.
    ///
    /// Current value is just enough for all tests in our CI. It's not selected in some special
    /// way. We will have 80 pages with 4KB page size.
    static constexpr size_t default_stack_size = 320 * 1024; /// 64KB was not enough for tests

    explicit FiberStack(size_t stack_size_ = default_stack_size) : stack_size(stack_size_)
    {
        page_size = getPageSize();
        num_pages = 1 + (stack_size - 1) / page_size;
        num_bytes = (num_pages + 1) * page_size; /// Add one page at bottom that will be used as guard-page
    }

    boost::context::stack_context allocate() const;

    void deallocate(boost::context::stack_context & sctx) const;
};

class FixedSizeFiberStackCache;

/// Fiber stack allocator with cached pages.
/// It is used to reduce the number of mmap / munmap / mprotect / sysconf syscalls.
class FixedSizeFiberStackWithCache
{
public:
    explicit FixedSizeFiberStackWithCache(size_t stack_size_ = FiberStack::default_stack_size);
    FixedSizeFiberStackWithCache(const FixedSizeFiberStackWithCache &);
    ~FixedSizeFiberStackWithCache();

    boost::context::stack_context allocate();
    void deallocate(boost::context::stack_context & sctx);
private:
    /// Cache for fiber stacks, the stack in the cached is allocated by mmap with guard page.
    FixedSizeFiberStackCache * cache;

    /// Fallback allocator is used when the cache is full
    /// TODO: can we use standard allocator with ::malloc here? It's being used in boost::context::pooled_fixedsize_stack
    /// The only reason to use mmap is to have guard page, but since we're using fibers to run specific tasks, I think
    /// we may have SOME stack without guard page (if stack overflow happens, cached stacks with guard page will catch it)
    FiberStack * fallback_allocator;
};

