#include <Processors/Executors/Runtime/Engine/WorkerSlot.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace
{

constexpr UInt64 renew_period_ns = 1'000'000;

}

WorkerSlot::WorkerSlot(AcquiredSlotPtr slot_, MemoryReservation * reservation_, MemoryTracker * tracker_)
    : slot(std::move(slot_))
    , lease(dynamic_cast<ISlotLease *>(slot.get()))
    , reservation(reservation_)
    , tracker(tracker_)
{
    if (lease)
        lease->startConsumption();
}

bool WorkerSlot::keepGoing()
{
    if (lease)
    {
        const UInt64 now_ns = clock_gettime_ns();
        if (now_ns - last_renew_ns >= renew_period_ns)
        {
            last_renew_ns = now_ns;
            if (!lease->renew())
                return false;
        }
    }

    if (reservation && tracker)
        reservation->syncWithMemoryTracker(tracker);

    return true;
}

}
