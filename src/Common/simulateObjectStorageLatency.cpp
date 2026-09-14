#include <Common/simulateObjectStorageLatency.h>

#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <base/defines.h>
#include <base/sleep.h>

namespace ProfileEvents
{
    extern const Event SimulatedObjectStorageLatencyMicroseconds;
}

namespace DB
{

/// NOINLINE (also LTO-proof) keeps the injected wait as its own profiler frame, not smeared over the caller.
NOINLINE void simulateObjectStorageLatency(UInt64 sleep_ms)
{
    auto timer = CurrentThread::getProfileEvents().timer(ProfileEvents::SimulatedObjectStorageLatencyMicroseconds);
    sleepForMilliseconds(sleep_ms);
}

}
