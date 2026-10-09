#include <Processors/Executors/Runtime/V2/Engine/WorkerSlot.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace Runtime::V2
{

static constexpr UInt64 check_period_ns = 1'000'000;

WorkerSlot::WorkerSlot(AcquiredSlotPtr slot_, MemoryReservation * reservation_, MemoryTracker * tracker_)
    : slot(std::move(slot_))
    , lease(dynamic_cast<ISlotLease *>(slot.get()))
    , reservation(reservation_)
    , tracker(tracker_)
{
    if (lease)
        lease->startConsumption();
}

bool WorkerSlot::keepRunning()
{
    if (!lease && !reservation)
        return true;

    const UInt64 now_ns = clock_gettime_ns();
    if (now_ns - last_check_ns < check_period_ns)
        return true;
    last_check_ns = now_ns;

    if (lease && !lease->renew())
        return false;

    if (reservation && tracker)
        reservation->syncWithMemoryTracker(tracker);

    return true;
}

}

}
