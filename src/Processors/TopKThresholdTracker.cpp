#include <mutex>
#include <shared_mutex>
#include <Processors/TopKThresholdTracker.h>
#include <Columns/Collator.h>
#include <DataTypes/IDataType.h>

namespace DB
{

void TopKThresholdTrackerGeneric::testAndSet(const Field & value)
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

bool TopKThresholdTrackerGeneric::isValueInsideThreshold(const Field & value) const
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

Field TopKThresholdTrackerGeneric::getValue() const
{
    std::shared_lock lock(mutex);
    auto ret = threshold;
    return ret;
}

int TopKThresholdTrackerGeneric::compareFields(const Field & lhs, const Field & rhs) const
{
    bool lhs_null = lhs.isNull();
    bool rhs_null = rhs.isNull();

    if (lhs_null && rhs_null)
        return 0;
    if (lhs_null)
        return sort_desc.nulls_direction;
    if (rhs_null)
        return -sort_desc.nulls_direction;

    if (sort_desc.collator && lhs.getType() == Field::Types::String)
    {
        const auto & l = lhs.safeGet<String>();
        const auto & r = rhs.safeGet<String>();
        return sort_desc.collator->compare(l.data(), l.size(), r.data(), r.size());
    }

    if (lhs < rhs)
        return -1;
    if (rhs < lhs)
        return 1;
    return 0;
}

TopKThresholdTrackerPtr createTopKThresholdTracker(const SortColumnDescription & sort_desc, const IDataType & data_type)
{
    /// Use the lock-free implementation when values of the type
    /// are represented in a `Field` by a plain 64-bit numeric type.
    if (data_type.isValueRepresentedByNumber())
    {
        switch (data_type.getDefault().getType())
        {
            case Field::Types::UInt64:
                return std::make_shared<TopKThresholdTrackerNumeric<UInt64>>(sort_desc);
            case Field::Types::Int64:
                return std::make_shared<TopKThresholdTrackerNumeric<Int64>>(sort_desc);
            case Field::Types::Float64:
                return std::make_shared<TopKThresholdTrackerNumeric<Float64>>(sort_desc);
            default:
                break;
        }
    }

    return std::make_shared<TopKThresholdTrackerGeneric>(sort_desc);
}

}
