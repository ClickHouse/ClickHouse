#pragma once
#include <shared_mutex>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <Common/SharedMutex.h>

namespace DB
{

struct TopKThresholdTracker
{
    explicit TopKThresholdTracker(const SortColumnDescription & sort_desc_)
        : sort_desc(sort_desc_)
    {
    }

    void testAndSet(const Field & value)
    {
        std::unique_lock lock(mutex);
        if (!is_set)
        {
            threshold = value;
            is_set = true;
            return;
        }
        int cmp = compareFields(value, threshold);
        if (sort_desc.direction == 1 && cmp < 0)
            threshold = value;
        else if (sort_desc.direction == -1 && cmp > 0)
            threshold = value;
    }

    bool isValueInsideThreshold(const Field & value) const
    {
        if (!is_set)
            return true;

        std::shared_lock lock(mutex);
        int cmp = compareFields(value, threshold);
        if (sort_desc.direction == 1 && cmp > 0)
            return false;
        if (sort_desc.direction == -1 && cmp < 0)
            return false;

        return true;
    }

    Field getValue() const
    {
        std::shared_lock lock(mutex);
        auto ret = threshold;
        return ret;
    }

    bool isSet() const { return is_set; }

    int getDirection() const { return sort_desc.direction; }
    int getNullsDirection() const { return sort_desc.nulls_direction; }
    const std::shared_ptr<Collator> & getCollator() const { return sort_desc.collator; }

private:
    /// Compare two Field values respecting NULL ordering and collation
    /// from the stored SortColumnDescription.
    int compareFields(const Field & lhs, const Field & rhs) const;

    Field threshold;
    mutable SharedMutex mutex;
    std::atomic<bool> is_set{false};
    SortColumnDescription sort_desc;
};

using TopKThresholdTrackerPtr = std::shared_ptr<TopKThresholdTracker>;

}
