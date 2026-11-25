#pragma once

#include "config.h"

#if USE_SILK

#include <boost/noncopyable.hpp>

#include <silk/fibers/condvar.h>
#include <silk/fibers/mutex.h>

#include <atomic>
#include <cstddef>

namespace Silk
{

/// Limits how many fibers hold a `Permit` at once: taking one suspends the calling fiber while `limit` permits are held.
/// A limit of 0 means no limit. The limit can be changed at any time from any thread: a lowered limit applies to the
/// permits taken after it, and the fibers that wait take the permits of a raised limit from the next release on.
/// Permits are taken and released in fibers.
class FiberSemaphore
{
public:
    /// Holds one permit from construction to destruction.
    class Permit : private boost::noncopyable
    {
    public:
        explicit Permit(FiberSemaphore & semaphore_) : semaphore(semaphore_) { semaphore.acquire(); }
        ~Permit() { semaphore.release(); }

    private:
        FiberSemaphore & semaphore;
    };

    void setLimit(size_t limit_) noexcept { limit.store(limit_, std::memory_order_relaxed); }
    size_t getLimit() const noexcept { return limit.load(std::memory_order_relaxed); }

private:
    void acquire() noexcept;
    void release() noexcept;

    std::atomic<size_t> limit{0};
    silk::FiberMutex mutex;
    silk::FiberCondVar permit_released;
    size_t held = 0;
};

}

#endif
