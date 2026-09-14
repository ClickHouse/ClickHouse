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

/// NOINLINE also stops (Thin)LTO from folding the sleep into its caller, which would
/// smear the injected wait over the caller's own frame in profiler stacks.
NOINLINE void simulateObjectStorageLatency(UInt64 sleep_ms)
{
    auto timer = CurrentThread::getProfileEvents().timer(ProfileEvents::SimulatedObjectStorageLatencyMicroseconds);
    sleepForMilliseconds(sleep_ms);
}

}
