#pragma once

#include "config.h"

#if USE_SILK

#include <silk/fibers/event.h>

#include <atomic>
#include <cstddef>

namespace SilkTests
{

/// Suspends each arriving fiber until the last one arrives.
class FiberBarrier
{
public:
    explicit FiberBarrier(size_t count) : remaining(count) {}

    void arriveAndWait()
    {
        if (--remaining == 0)
            event.set();
        else
            event.wait();
    }

    /// For participants that never started, so that the started ones are not left waiting.
    void dropParticipants(size_t count)
    {
        if (remaining.fetch_sub(count) == count)
            event.set();
    }

private:
    std::atomic<size_t> remaining;
    silk::FiberEvent event;
};

}

#endif
