#pragma once
#include <boost/context/stack_context.hpp>

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

    boost::context::stack_context allocate() const;
    void deallocate(boost::context::stack_context & sctx) const;

private:
    const size_t stack_size;
    const size_t page_size;
};
