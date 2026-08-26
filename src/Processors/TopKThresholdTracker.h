#pragma once
#include <atomic>
#include <Core/Field.h>
#include <Core/SortDescription.h>
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
    explicit TopKThresholdTrackerNumeric(const SortColumnDescription & sort_desc_);

    void testAndSet(const Field & value) override;
    bool isValueInsideThreshold(const Field & value) const override;

    /// Returns the sentinel if no value was published yet; callers must check `isSet` first.
    Field getValue() const override { return threshold.load(std::memory_order_relaxed);}
    bool isSet() const override { return is_set.load(std::memory_order_acquire);}

private:
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
