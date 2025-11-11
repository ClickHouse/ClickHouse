#pragma once
#include <Core/Field.h>
#include <mutex>
#include <shared_mutex>

/// TODO : Field is "heavy", just use atomic<size_t> for Int32/Int64/UInt32/UInt64/Date/DateTime/Float/etc.
/// Anthing better than Field + shared_mutex. Also there is a shared_ptr<> for the get / set.
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
                is_set = true;
            }
        }
        else if (direction == -1) /// DESC
        {
            if (value > threshold)
            {
                threshold = value;
                is_set = true;
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
    mutable std::shared_mutex mutex;
    bool is_set{false};
    int direction{0};
};

using TopNThresholdTrackerPtr = std::shared_ptr<TopNThresholdTracker>;

}
