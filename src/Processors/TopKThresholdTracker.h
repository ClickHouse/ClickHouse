#pragma once
#include <atomic>
#include <limits>
#include <Core/CompareHelper.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <Common/NaNUtils.h>
#include <Common/SharedMutex.h>

namespace DB
{

class IDataType;

class ITopKThresholdTracker
{
public:
    explicit ITopKThresholdTracker(const SortColumnDescription & sort_desc_) : sort_desc(sort_desc_) {}
    virtual ~ITopKThresholdTracker() = default;

    virtual void testAndSet(const Field & value) = 0;
    virtual bool isValueInsideThreshold(const Field & value) const = 0;
    virtual Field getValue() const = 0;
    virtual bool isSet() const = 0;

    int getDirection() const { return sort_desc.direction; }
    int getNullsDirection() const { return sort_desc.nulls_direction; }
    const std::shared_ptr<Collator> & getCollator() const { return sort_desc.collator; }

protected:
    SortColumnDescription sort_desc;
};

/// Lock-free tracker for types whose values are represented in a `Field` by a plain
/// numeric type that fits into `std::atomic` (T is one of UInt64, Int64, Float64).
template <typename T>
class TopKThresholdTrackerNumeric : public ITopKThresholdTracker
{
public:
    explicit TopKThresholdTrackerNumeric(const SortColumnDescription & sort_desc_)
        : ITopKThresholdTracker(sort_desc_)
        , threshold(sentinel(sort_desc_.direction))
    {
    }

    void testAndSet(const Field & value) override
    {
        T candidate = value.safeGet<T>();

        /// A NaN boundary must never become the threshold
        if constexpr (std::is_floating_point_v<T>)
        {
            if (isNaN(candidate))
                return;
        }

        T current = threshold.load(std::memory_order_relaxed);

        if (sort_desc.direction == 1)
        {
            while (CompareHelper<T>::less(candidate, current, sort_desc.nulls_direction)
                && !threshold.compare_exchange_weak(current, candidate, std::memory_order_relaxed))
            {
            }
        }
        else
        {
            while (CompareHelper<T>::greater(candidate, current, sort_desc.nulls_direction)
                && !threshold.compare_exchange_weak(current, candidate, std::memory_order_relaxed))
            {
            }
        }

        is_set.store(true, std::memory_order_release);
    }

    bool isValueInsideThreshold(const Field & value) const override
    {
        if (!is_set.load(std::memory_order_acquire))
            return true;

        T candidate = value.safeGet<T>();
        T current = threshold.load(std::memory_order_relaxed);

        if (sort_desc.direction == 1)
            return !CompareHelper<T>::greater(candidate, current, sort_desc.nulls_direction);

        return !CompareHelper<T>::less(candidate, current, sort_desc.nulls_direction);
    }

    /// Returns the sentinel if no value was published yet; callers must check `isSet` first.
    Field getValue() const override { return threshold.load(std::memory_order_relaxed); }
    bool isSet() const override { return is_set.load(std::memory_order_acquire); }

private:
    static constexpr T sentinel(int direction)
    {
        /// For floating point types the maximum finite value is not enough:
        /// a published threshold of +inf must not exclude +inf values.
        if constexpr (std::is_floating_point_v<T>)
            return direction == 1 ? std::numeric_limits<T>::infinity() : -std::numeric_limits<T>::infinity();
        else
            return direction == 1 ? std::numeric_limits<T>::max() : std::numeric_limits<T>::min();
    }

    std::atomic<T> threshold;
    std::atomic<bool> is_set{false};
};

class TopKThresholdTrackerGeneric : public ITopKThresholdTracker
{
public:
    explicit TopKThresholdTrackerGeneric(const SortColumnDescription & sort_desc_) : ITopKThresholdTracker(sort_desc_) {}

    void testAndSet(const Field & value) override;
    bool isValueInsideThreshold(const Field & value) const override;
    Field getValue() const override;
    bool isSet() const override { return is_set; }

private:
    /// Compare two Field values respecting NULL ordering and collation
    /// from the stored SortColumnDescription.
    int compareFields(const Field & lhs, const Field & rhs) const;

    Field threshold;
    mutable SharedMutex mutex;
    std::atomic<bool> is_set{false};
};

using TopKThresholdTrackerPtr = std::shared_ptr<ITopKThresholdTracker>;
TopKThresholdTrackerPtr createTopKThresholdTracker(const SortColumnDescription & sort_desc, const IDataType & data_type);

}
