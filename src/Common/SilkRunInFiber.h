#pragma once

#if defined(OS_LINUX)

#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <type_traits>
#include <utility>

namespace Silk
{

namespace detail
{

struct FiberLocalStorage
{
    DB::ThreadStatus * saved_current_thread = nullptr;
};

/// Inheritance is required,
/// so we can cast void * received from silk::FiberScheduler::getFiberParameters to FiberLocalStorage *.
template <typename F>
struct FiberContext : FiberLocalStorage
{
    F func;

    static int main(FiberContext * self) noexcept
    {
        return self->func();
    }
};

}

void initializeFiberScheduler();
void destroyFiberScheduler();

template <typename F>
[[nodiscard]] int RunInFiber(F && func, silk::FiberFuture & future)
{
    using Func = std::decay_t<F>;
    return silk::FiberScheduler::run(
        &detail::FiberContext<Func>::main,
        detail::FiberContext<Func>
        {
            .func = std::forward<F>(func),
        },
        &future);
}

template <typename F>
[[nodiscard]] int RunInFiber(F && func)
{
    silk::FiberFuture future;
    int r = RunInFiber(std::forward<F>(func), future);
    return r ? r : future.wait();
}

}

#endif
