#include <Processors/ISpillable.h>

#include <Common/CurrentThread.h>
#include <Common/MemorySpillScheduler.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/ThreadStatus.h>

namespace DB
{

void ISpillable::unregisterProcessor()
{
    const size_t previous = active_processors.fetch_sub(1, std::memory_order_acq_rel);
    chassert(previous > 0);
    if (previous != 1)
        return;

    if (spill_accounting.reservation)
        spill_accounting.reservation->removeReclaimable(this);
    if (auto group = CurrentThread::getGroup())
        group->memory_spill_scheduler->remove(this);
}

}
