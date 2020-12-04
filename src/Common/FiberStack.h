#pragma once
#include <boost/context/stack_context.hpp>
#include <Common/Allocator.h>

#if defined(BOOST_USE_VALGRIND)
#include <valgrind/valgrind.h>
#endif

/// This is an implementation of allocator for fiber stack.
/// It uses internal allocator, so we track memory usage. It is the main reason why this class is needed.
/// The reference implementations are pooled_fixedsize_stack and protected_fixedsize_stack from boost::context.
template <typename TAllocator = Allocator<false, false>>
class FiberStack
{
private:
    size_t stack_size;
public:
    /// 8MB of memory per fiber stack may seem too expensive. It is indeed.
    /// The reason is that current (patched) libunwind needs > 4MB of stack memory to unwind stack.
    /// If we allocate less memory, any thrown exception inside fiber will cause segfault.
    static constexpr size_t default_stack_size = 8 * 1024 * 1024;

    explicit FiberStack(size_t stack_size_ = default_stack_size) : stack_size(stack_size_) {}

    boost::context::stack_context allocate()
    {
        void * vp = TAllocator().alloc(stack_size);

        boost::context::stack_context sctx;
        sctx.size = stack_size;
        sctx.sp = static_cast< char * >(vp) + sctx.size;
#if defined(BOOST_USE_VALGRIND)
        sctx.valgrind_stack_id = VALGRIND_STACK_REGISTER(sctx.sp, vp);
#endif
        return sctx;
    }

    void deallocate(boost::context::stack_context & sctx)
    {
#if defined(BOOST_USE_VALGRIND)
        VALGRIND_STACK_DEREGISTER( sctx.valgrind_stack_id);
#endif
        void * vp = static_cast< char * >(sctx.sp) - sctx.size;
        TAllocator().free(vp, stack_size);
    }
};
