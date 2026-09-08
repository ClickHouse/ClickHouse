#pragma once

#include <Common/ISlotControl.h>

class MemoryTracker;

namespace DB
{

struct MemoryReservation;

class WorkerSlot
{
public:
    WorkerSlot(AcquiredSlotPtr slot_, MemoryReservation * reservation_, MemoryTracker * tracker_);

    bool keepGoing();

private:
    AcquiredSlotPtr slot;

    /// Lease workloads
    ISlotLease * lease;
    UInt64 last_renew_ns = 0;

    /// Memory synchronization
    MemoryReservation * reservation;
    MemoryTracker * tracker;
};

}
