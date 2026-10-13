#pragma once

#include <Common/ISlotControl.h>

class MemoryTracker;

namespace DB
{

struct MemoryReservation;

namespace Runtime::V2
{

class WorkerSlot
{
public:
    WorkerSlot(AcquiredSlotPtr slot_, MemoryReservation * reservation_, MemoryTracker * tracker_);

    bool keepRunning();

private:
    AcquiredSlotPtr slot;

    /// Lease workloads
    ISlotLease * lease;
    UInt64 last_check_ns = 0;

    /// Memory synchronization
    MemoryReservation * reservation;
    MemoryTracker * tracker;
};

}

}
