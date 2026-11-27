#pragma once
#include <atomic>
#include <Columns/IColumn_fwd.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <Common/SharedMutex.h>

namespace DB
{

class IDataType;

class ITopKThresholdTracker
{
public:
    ITopKThresholdTracker(const SortColumnDescription & sort_desc_, DataTypePtr data_type_)
        : sort_desc(sort_desc_), data_type(std::move(data_type_)) {}
    virtual ~ITopKThresholdTracker() = default;

    virtual void testAndSet(const Field & value) = 0;
    virtual bool isValueInsideThreshold(const Field & value) const = 0;
    virtual Field getValue() const = 0;
    virtual bool isSet() const = 0;

    int getDirection() const { return sort_desc.direction; }
    int getNullsDirection() const { return sort_desc.nulls_direction; }
    const std::shared_ptr<Collator> & getCollator() const { return sort_desc.collator; }
    /// The type of the sort column the published values are taken from.
    const DataTypePtr & getDataType() const { return data_type; }

    /// Changes every time a new threshold value is stored.
    UInt64 getVersion() const { return version.load(std::memory_order_relaxed); }

    /// Set when the reading step is cloned: then several sorts may publish to this tracker.
    void setSharedByPlanCopies() { shared_by_plan_copies.store(true, std::memory_order_relaxed); }
    bool isSharedByPlanCopies() const { return shared_by_plan_copies.load(std::memory_order_relaxed); }

protected:
    SortColumnDescription sort_desc;
    DataTypePtr data_type;
    std::atomic<UInt64> version{0};
    std::atomic<bool> shared_by_plan_copies{false};
};

/// Lock-free tracker for types whose values are represented in a `Field` by a plain
/// numeric type that fits into `std::atomic` (T is one of UInt64, Int64, Float64).
template <typename T>
class TopKThresholdTrackerNumeric : public ITopKThresholdTracker
{
public:
    TopKThresholdTrackerNumeric(const SortColumnDescription & sort_desc_, DataTypePtr data_type_);

    void testAndSet(const Field & value) override;
    bool isValueInsideThreshold(const Field & value) const override;

    /// Returns the sentinel if no value was published yet; callers must check `isSet` first.
    Field getValue() const override { return threshold.load(std::memory_order_relaxed);}
    bool isSet() const override;

private:
    std::atomic<T> threshold;
};

class TopKThresholdTrackerGeneric : public ITopKThresholdTracker
{
public:
    TopKThresholdTrackerGeneric(const SortColumnDescription & sort_desc_, DataTypePtr data_type_);

    void testAndSet(const Field & value) override;
    bool isValueInsideThreshold(const Field & value) const override;
    Field getValue() const override;
    bool isSet() const override { return is_set; }

private:
    /// Compare two Field values in the order the sort puts them, at every nesting level
    /// (NULL ordering and collation from the stored SortColumnDescription).
    int compareFields(const Field & lhs, const Field & rhs) const;

    /// Set when the `Field` order can differ from the sort's: a `Nullable` or a float anywhere in the type, or a collation.
    ColumnPtr empty_column;

    Field threshold;
    mutable SharedMutex mutex;
    std::atomic<bool> is_set{false};
};

using TopKThresholdTrackerPtr = std::shared_ptr<ITopKThresholdTracker>;
TopKThresholdTrackerPtr createTopKThresholdTracker(const SortColumnDescription & sort_desc, const IDataType & data_type);

}
