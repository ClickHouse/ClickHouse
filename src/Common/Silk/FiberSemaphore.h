#pragma once

#include "config.h"

#if USE_SILK

#include <silk/fibers/condvar.h>
#include <silk/fibers/mutex.h>

#include <atomic>
#include <cstddef>
#include <optional>
#include <utility>

namespace Silk
{

/// Limits how many fibers hold a `Permit` at once: taking one suspends the calling fiber while `limit` permits are held.
/// A limit of 0 means no limit. The limit can be changed at any time from any thread: a lowered limit applies to the
/// permits taken after it, and the fibers that wait take the permits of a raised limit from the next release on.
/// Permits are taken and released in fibers.
class FiberSemaphore
{
public:
    /// Holds one permit until it is destroyed or moved from.
    class Permit
    {
    public:
        /// Waits for a permit.
        explicit Permit(FiberSemaphore & semaphore_) : semaphore(&semaphore_) { semaphore->acquire(); }
        Permit(Permit && other) noexcept : semaphore(std::exchange(other.semaphore, nullptr)) {}
        Permit(const Permit &) = delete;
        Permit & operator=(const Permit &) = delete;
        Permit & operator=(Permit &&) = delete;
        ~Permit()
        {
            if (semaphore)
                semaphore->release();
        }

    private:
        friend class FiberSemaphore;
        struct Taken {};
        Permit(FiberSemaphore & semaphore_, Taken) : semaphore(&semaphore_) {}

        FiberSemaphore * semaphore;
    };

    /// Takes a permit if one is free, without waiting.
    std::optional<Permit> tryTakePermit() noexcept;

    void setLimit(size_t limit_) noexcept { limit.store(limit_, std::memory_order_relaxed); }
    size_t getLimit() const noexcept { return limit.load(std::memory_order_relaxed); }

private:
    void acquire() noexcept;
    void release() noexcept;
    bool hasFreePermit() const noexcept;

    std::atomic<size_t> limit{0};
    silk::FiberMutex mutex;
    silk::FiberCondVar permit_released;
    size_t held = 0;
};

}

#endif
