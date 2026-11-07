#pragma once

/// Not implemented on Windows (see `CoroutineStack.cpp`): it rests on `getrlimit` for the stack
/// size and on mmap guard pages, and its only consumer, `AsyncTaskExecutor`, is not built there
/// either. `boost::context` itself does support Windows - see the PE assembly variants selected in
/// contrib/boost-cmake - so this is about the stack allocator, not about fibers being impossible.
///
/// The declaration stays visible on Windows, because `StackfulCoroutine` names it, and
/// `StackfulCoroutine` is built everywhere: `checkStackSize` asks it for the current coroutine.
/// Nothing constructs a `CoroutineStack` there.
#include <boost/context/stack_context.hpp>

/// This is an implementation of allocator for coroutine stack.
/// The reference implementation is protected_fixedsize_stack from boost::context.
/// This implementation additionally track memory usage. It is the main reason why it is needed.
class CoroutineStack
{
public:
    /// NOTE: If you see random segfaults in CI and stack starts from boost::context::...coroutine...
    /// probably it worth to try to increase stack size for coroutines.
    ///
    /// Current value is just enough for all tests in our CI. It's not selected in some special
    /// way. We will have 80 pages with 4KB page size.
    static constexpr size_t default_stack_size = 320 * 1024; /// 64KB was not enough for tests

    /// Address range a coroutine's frames may occupy: the allocation minus the guard page.
    struct Bounds
    {
        const char * lowest = nullptr;
        size_t size = 0;
    };

    explicit CoroutineStack(size_t stack_size_ = default_stack_size);

    boost::context::stack_context allocate();
    void deallocate(boost::context::stack_context & sctx) const;

    /// Empty before the first `allocate`.
    Bounds lastAllocatedBounds() const { return last_allocated_bounds; }

private:
    const size_t stack_size;
    const size_t page_size;
    Bounds last_allocated_bounds;
};
