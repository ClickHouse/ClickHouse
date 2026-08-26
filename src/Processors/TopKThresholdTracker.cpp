#include <mutex>
#include <shared_mutex>
#include <Processors/TopKThresholdTracker.h>
#include <Columns/Collator.h>
#include <Core/CompareHelper.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace
{

/// Initial threshold that lets every value pass. For floating point types the maximum
/// finite value is not enough: a published threshold of +inf must not exclude +inf values.
template <typename T>
constexpr T sentinel(int direction)
{
    if constexpr (std::is_floating_point_v<T>)
        return direction == 1 ? std::numeric_limits<T>::infinity() : -std::numeric_limits<T>::infinity();
    else
        return direction == 1 ? std::numeric_limits<T>::max() : std::numeric_limits<T>::min();
}

}

template <typename T>
TopKThresholdTrackerNumeric<T>::TopKThresholdTrackerNumeric(const SortColumnDescription & sort_desc_)
    : ITopKThresholdTracker(sort_desc_)
    , threshold(sentinel<T>(sort_desc_.direction))
{
}

template <typename T>
void TopKThresholdTrackerNumeric<T>::testAndSet(const Field & value)
{
    T candidate = value.safeGet<T>();

    /// A NaN boundary must never become the threshold.
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

template <typename T>
bool TopKThresholdTrackerNumeric<T>::isValueInsideThreshold(const Field & value) const
{
    if (!is_set.load(std::memory_order_acquire))
        return true;

    T candidate = value.safeGet<T>();
    T current = threshold.load(std::memory_order_relaxed);

    if (sort_desc.direction == 1)
        return !CompareHelper<T>::greater(candidate, current, sort_desc.nulls_direction);

    return !CompareHelper<T>::less(candidate, current, sort_desc.nulls_direction);
}

template class TopKThresholdTrackerNumeric<UInt64>;
template class TopKThresholdTrackerNumeric<Int64>;
template class TopKThresholdTrackerNumeric<Float64>;

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
    return threshold;
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
