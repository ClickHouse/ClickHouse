#pragma once

#include <Common/ISlotControl.h>

class MemoryTracker;

namespace DB
{

class WorkerPool;
struct MemoryReservation;

class WorkerSlot
{
public:
    WorkerSlot(AcquiredSlotPtr slot_, WorkerPool & pool_, MemoryReservation * reservation_, MemoryTracker * tracker_);

    bool keepGoing();

private:
    AcquiredSlotPtr slot;
    WorkerPool & pool;

    /// Memory synchronization
    MemoryReservation * reservation;
    MemoryTracker * tracker;

    /// Lease workloads
    ISlotLease * lease;
    UInt64 last_renew_ns = 0;
};

}
