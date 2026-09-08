#include <Processors/Executors/Runtime/Engine/WorkerSlot.h>
#include <Processors/Executors/Runtime/Engine/WorkerPool.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace
{

constexpr UInt64 renew_period_ns = 1'000'000;

}

WorkerSlot::WorkerSlot(AcquiredSlotPtr slot_, WorkerPool & pool_, MemoryReservation * reservation_, MemoryTracker * tracker_)
    : slot(std::move(slot_))
    , pool(pool_)
    , reservation(reservation_)
    , tracker(tracker_)
    , lease(dynamic_cast<ISlotLease *>(slot.get()))
{
    if (lease)
        lease->startConsumption();
}

bool WorkerSlot::keepGoing()
{
    pool.grow();

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
