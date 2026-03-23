#pragma once
#include <boost/context/stack_context.hpp>
#include <base/sanitizer_defs.h>
#include <cstddef>

#if __has_include(<sanitizer/asan_interface.h>) && defined(ADDRESS_SANITIZER)
#   include <sanitizer/asan_interface.h>
#endif

/// This is an implementation of allocator for fiber stack.
/// The reference implementation is protected_fixedsize_stack from boost::context.
/// This implementation additionally track memory usage. It is the main reason why it is needed.
class FiberStack
{
public:
    /// NOTE: If you see random segfaults in CI and stack starts from boost::context::...fiber...
    /// probably it worth to try to increase stack size for coroutines.
    ///
    /// Current value is just enough for all tests in our CI. It's not selected in some special
    /// way. We will have 80 pages with 4KB page size.
    static constexpr size_t default_stack_size = 320 * 1024; /// 64KB was not enough for tests

    explicit FiberStack(size_t stack_size_ = default_stack_size);

    boost::context::stack_context allocate();
    void deallocate(boost::context::stack_context & sctx) const;

    /// Unpoison the fiber stack memory for ASan.
    ///
    /// ASan's use-after-scope detection poisons stack memory when local variables
    /// go out of scope. On fiber stacks, this poisoning persists across context
    /// switches (yield/resume), causing false positives when the same stack addresses
    /// are reused by new frames in subsequent fiber resumes.
    ///
    /// This must be called before each fiber resume to clear stale scope poisoning.
    /// See https://github.com/google/sanitizers/issues/189
    void beforeResume() const
    {
#if defined(ADDRESS_SANITIZER)
        if (stack_base)
            ASAN_UNPOISON_MEMORY_REGION(stack_base, stack_allocation_size);
#endif
    }

private:
    const size_t stack_size;
    const size_t page_size;

    /// Tracked allocation for ASan unpoisoning.
    void * stack_base = nullptr;
    size_t stack_allocation_size = 0;
};
