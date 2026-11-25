#include <Common/Silk/FiberSemaphore.h>

#if USE_SILK

#include <Common/Silk/FiberScheduler.h>
#include <base/defines.h>

#include <mutex>

namespace Silk
{

bool FiberSemaphore::hasFreePermit() const noexcept
{
    const size_t current_limit = getLimit();
    return current_limit == 0 || held < current_limit;
}

void FiberSemaphore::acquire() noexcept
{
    chassert(isInsideFiber());

    std::unique_lock lock(mutex);
    while (!hasFreePermit())
        permit_released.wait(lock);
    ++held;

    /// A release wakes one fiber. When the limit was raised, more permits may be free: wake the next fiber too.
    if (hasFreePermit())
        permit_released.notify_one();
}

std::optional<FiberSemaphore::Permit> FiberSemaphore::tryTakePermit() noexcept
{
    chassert(isInsideFiber());

    std::lock_guard lock(mutex);
    if (!hasFreePermit())
        return {};
    ++held;
    return Permit(*this, Permit::Taken{});
}

void FiberSemaphore::release() noexcept
{
    chassert(isInsideFiber());

    std::lock_guard lock(mutex);
    chassert(held);
    --held;
    permit_released.notify_one();
}

}

#endif
