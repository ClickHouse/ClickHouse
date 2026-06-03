#pragma once

#if defined(OS_LINUX)

#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <mutex>
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

    DB::ThreadStatus * saved_current_thread;
};

template <typename F>
int fiberMain(FiberParams<F> * params) noexcept
{
    DB::ThreadStatus thread_status{DB::ThreadStatus::NO_OS_THREAD};
    DB::ThreadGroupSwitcher switcher(params->thread_group);
    params->func();
    return 0;
}

template <typename F>
inline void onFiberResumeSuspend(silk::Fiber * fiber) noexcept
{
    auto params = static_cast<FiberParams<F> *>(silk::FiberScheduler::getFiberParameters(fiber));
    std::swap(params->saved_current_thread, DB::current_thread);
}

}

template <typename F>
[[nodiscard]] int RunInFiber(F && func, DB::ThreadGroupPtr thread_group, silk::FiberFuture & future)
{
    using Func = std::decay_t<F>;
    static silk::FiberScheduler::Options options =
    {
        .fiberResume = &onFiberResumeSuspend<F>,
        .fiberSuspend = &onFiberResumeSuspend<F>,
    };

    return silk::FiberScheduler::run(
        &detail::fiberMain<Func>,
        detail::FiberParams<Func>
        {
            .thread_group = std::move(thread_group),
            .func = std::forward<F>(func),
        },
        &future,
        options
    );
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
