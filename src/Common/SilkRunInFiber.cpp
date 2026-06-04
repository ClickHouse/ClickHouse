#if defined(OS_LINUX)

#include <Common/SilkRunInFiber.h>

#include <Common/CurrentThread.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <functional>
#include <utility>

namespace Silk
{


struct FiberContext
{
    DB::ThreadStatus * saved_current_thread;
    std::function<int()> task;

    static int main(FiberContext * self) noexcept
    {
        return self->task();
    }
};

void onFiberResumeSuspend(silk::Fiber * fiber) noexcept
{
    auto * context = static_cast<FiberContext *>(silk::FiberScheduler::getFiberParameters(fiber));
    std::swap(context->saved_current_thread, DB::current_thread);
}

void initializeFiberScheduler()
{
    const silk::FiberScheduler::Options options =
    {
        .fiberSuspend = &onFiberResumeSuspend,
        .fiberResume = &onFiberResumeSuspend,
    };
    silk::FiberScheduler::initialize(&options);
}

void destroyFiberScheduler()
{
    silk::FiberScheduler::destroy();
}

int RunInFiber(std::function<int()> task, silk::FiberFuture & future)
{
    return silk::FiberScheduler::run(
        &FiberContext::main,
        FiberContext{ .saved_current_thread = nullptr, .task = std::move(task) },
        &future);
}

int RunInFiber(std::function<int()> task)
{
    silk::FiberFuture future;
    int r = RunInFiber(std::move(task), future);
    return r ? r : future.wait();
}

}

#endif
