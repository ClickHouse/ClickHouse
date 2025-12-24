#pragma once
#include <atomic>
#include <mutex>
#include <Core/Field.h>
#include <Common/SharedMutex.h>

/// Field + mutex looks a little heavy, but profiling has not showed anything concerning.
/// It should be possible to use std::atomic<size_t> for the threshold because we only
/// support numeric equivalent types. But that will require type specific comparison
/// operators (e.g for Int32, for Date / DateTime, for DecimalXX etc).
///
/// Field keeps the door open for using this class for ORDER BY <string> (if needed)
namespace DB
{

struct TopKThresholdTracker
{
    explicit TopKThresholdTracker(int direction_) : direction(direction_) {}

    void testAndSet(const Field & value)
    {
        std::lock_guard lock(mutex);
        const auto cur = active_idx.load(std::__1::memory_order::acquire);
        const auto next = 1 - cur;

        if (!is_set.load(std::memory_order_acquire))
        {
            thresholds[next] = value;
            active_idx.store(next, std::memory_order_release);
            is_set.store(true, std::memory_order_release);
            return;
        }

        const auto & threshold = thresholds[cur];
        if ((direction == 1 /* ASC */ && value < threshold) || (direction == -1 /* DESC */ && value > threshold))
        {
            thresholds[next] = value;
            active_idx.store(next, std::memory_order_release);
        }
    }

    bool isValueInsideThreshold(const Field & value) const
    {
        if (!is_set.load(std::memory_order_acquire))
            return true;

        const auto idx = active_idx.load(std::memory_order_acquire);
        const auto & threshold = thresholds[idx];

        return ((direction == 1 /* ASC */ && value < threshold) || (direction == -1 /* DESC */ && value > threshold));
    }

    Field getValue() const
    {
        const auto idx = active_idx.load(std::memory_order_acquire);
        return thresholds[idx];
    }

    bool isSet() const { return is_set.load(std::memory_order_acquire); }

    int getDirection() const { return direction; }

private:
    /// The current threshold is double buffered: one value at `active_idx` is "published", which threads can
    /// freely read without locking, while writers must lock, write to the other index, then swap `active_idx`.
    /// This avoids shared lock contention -- cache line ping-ponging -- for concurrent readers.
    Field thresholds[2];
    mutable SharedMutex mutex;
    std::atomic<bool> is_set{false};
    std::atomic<uint8_t> active_idx{0};
    int direction{0};
};

using TopKThresholdTrackerPtr = std::shared_ptr<TopKThresholdTracker>;

}
