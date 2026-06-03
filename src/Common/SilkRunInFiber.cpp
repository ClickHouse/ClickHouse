#if defined(OS_LINUX)

#include <Common/SilkRunInFiber.h>

#include <Common/CurrentThread.h>

#include <silk/fibers/fiber.h>

#include <utility>

namespace Silk
{

namespace
{

void onFiberResumeSuspend(silk::Fiber * fiber) noexcept
{
    auto * storage = static_cast<detail::FiberLocalStorage *>(silk::FiberScheduler::getFiberParameters(fiber));
    std::swap(storage->saved_current_thread, DB::current_thread);
}

}

void initializeFiberScheduler()
{
    silk::FiberScheduler::Options options;
    options.fiberResume = &onFiberResumeSuspend;
    options.fiberSuspend = &onFiberResumeSuspend;
    silk::FiberScheduler::initialize(&options);
}

void destroyFiberScheduler()
{
    silk::FiberScheduler::destroy();
}

}

#endif
