#pragma once
#include <shared_mutex>
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
        std::unique_lock lock(mutex);
        if (!is_set)
        {
            threshold = value;
            is_set = true;
            ++version;
            return;
        }
        if (direction == 1) /// ASC
        {
            if (value < threshold)
            {
                threshold = value;
                ++version;
            }
        }
        else if (direction == -1) /// DESC
        {
            if (value > threshold)
            {
                threshold = value;
                ++version;
            }
        }
    }

    bool isValueInsideThreshold(const Field & value) const
    {
        if (!is_set)
            return true;

        std::shared_lock lock(mutex);
        if (direction == 1 && value >= threshold) /// ASC
            return false;
        else if (direction == -1 && value <= threshold) /// DESC
            return false;

        return true;
    }

    Field getValue() const
    {
        std::shared_lock lock(mutex);
        auto ret = threshold;
        return ret;
    }

    bool isSet() const { return is_set; } /// unlocked read is fine

    int getDirection() const { return direction; }

    /// Monotonically increasing counter, bumped on every threshold change.
    /// Allows consumers to cheaply detect whether a rebuild is needed.
    uint64_t getVersion() const { return version.load(std::memory_order_acquire); }

private:
    Field threshold;
    mutable SharedMutex mutex;
    std::atomic<bool> is_set{false};
    std::atomic<uint64_t> version{0};
    int direction{0};
};

using TopKThresholdTrackerPtr = std::shared_ptr<TopKThresholdTracker>;

}
