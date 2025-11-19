#pragma once
#include <shared_mutex>
#include <Core/Field.h>
#include <Common/SharedMutex.h>

/// Field + mutex looks a little heavy, but profiling has not showed anything concerning.
/// It should be possible to use std::atomic<size_t> for the threshold because we only
/// support numeric equivalent types. But that will require type specific comparision
/// operators (e.g for Int32, for Date / DateTime etc).
namespace DB
{

struct TopNThresholdTracker
{
    explicit TopNThresholdTracker(int direction_) : direction(direction_) {}

    void testAndSet(const Field & value)
    {
        std::unique_lock lock(mutex);
        if (!is_set)
        {
            threshold = value;
            is_set = true;
            return;
        }
        if (direction == 1) /// ASC
        {
            if (value < threshold)
            {
                threshold = value;
            }
        }
        else if (direction == -1) /// DESC
        {
            if (value > threshold)
            {
                threshold = value;
            }
        }
    }

    bool isValueInsideThreshold(const Field & value) const
    {
        bool result = true;
        if (!is_set)
            return result;

        std::shared_lock lock(mutex);
        if (direction == 1) /// ASC
        {
            if (value >= threshold)
            {
                result = false;
            }
        }
        else if (direction == -1) /// DESC
        {
            if (value <= threshold)
            {
                result = false;
            }
        }
        return result;
    }

    Field getValue() const
    {
        std::shared_lock lock(mutex);
        auto ret = threshold;
        return ret;
    }

    bool isSet() const { return is_set; } /// unlocked read is fine

    int getDirection() const { return direction; }

private:
    Field threshold;
    mutable SharedMutex mutex;
    bool is_set{false};
    int direction{0};
};

using TopNThresholdTrackerPtr = std::shared_ptr<TopNThresholdTracker>;

}
