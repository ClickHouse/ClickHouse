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

template <typename F>
struct FiberParams
{
    DB::ThreadGroupPtr thread_group;
    F func;
};

template <typename F>
int fiberMain(FiberParams<F> * params) noexcept
{
    DB::ThreadStatus thread_status{DB::ThreadStatus::NO_OS_THREAD};
    DB::ThreadGroupSwitcher switcher(params->thread_group);
    params->func();
    return 0;
}

}

template <typename F>
[[nodiscard]] int RunInFiber(F && func, DB::ThreadGroupPtr thread_group, silk::FiberFuture & future)
{
    using Func = std::decay_t<F>;
    return silk::FiberScheduler::run(
        &detail::fiberMain<Func>,
        detail::FiberParams<Func>
        {
            .thread_group = std::move(thread_group),
            .func = std::forward<F>(func),
        },
        &future);
}

template <typename F>
[[nodiscard]] int RunInFiber(F && func, DB::ThreadGroupPtr thread_group)
{
    silk::FiberFuture future;
    int r = RunInFiber(std::forward<F>(func), std::move(thread_group), future);
    return r ? r : future.wait();
}

}

#endif
