#include <Common/Silk/FiberSemaphore.h>

#if USE_SILK

#include <Common/Silk/FiberScheduler.h>
#include <base/defines.h>

#include <mutex>

namespace Silk
{

void FiberSemaphore::acquire() noexcept
{
    chassert(isInsideFiber());

    std::unique_lock lock(mutex);
    auto has_free_permit = [this]
    {
        const size_t current_limit = getLimit();
        return current_limit == 0 || held < current_limit;
    };

    while (!has_free_permit())
        permit_released.wait(lock);
    ++held;

    /// A release wakes one fiber. When the limit was raised, more permits may be free: wake the next fiber too.
    if (has_free_permit())
        permit_released.notify_one();
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
