#pragma once

#include <utility>

#include <base/defines.h>

#include <Common/DequeWithMemoryTracking.h>


namespace DB
{

/// Maintains the running combine ("sum") of per-bucket `SummaryType` values over a sliding window. Each value is
/// added with the largest timestamp it covers and dropped once that timestamp leaves the window; values must be
/// added in non-decreasing timestamp order. `getCurrentSum` returns the combine of the in-window values.
///
/// `SummaryType` is a monoid: a default-constructed value is the identity,
/// and `SummaryType::merge(const SummaryType &)` combines two of them (associative).
///
/// The removal strategy is chosen automatically:
///  - if `SummaryType` is *invertible* - it has `void unmerge(const SummaryType & leaving, const SummaryType * new_first)`
///    - a single running sum is kept, `add` merges into it and `removeBefore` unmerges the values leaving the window.
///  - otherwise the value cannot be subtracted, so the in-window values are kept in a deque and their sum is
///    recomputed per grid point (memoized until the window changes).
template <typename TimestampType, typename SummaryType>
class AggregateFunctionTimeseriesSlidingSum
{
public:
    /// `SummaryType` is invertible when it can subtract a previously merged value.
    static constexpr bool is_invertible = requires (SummaryType summary, const SummaryType & value)
        { summary.unmerge(value, &value); };

    AggregateFunctionTimeseriesSlidingSum()
        : current_sum_valid(!is_invertible)
    {
    }

    void add(SummaryType && value, TimestampType timestamp)
    {
        if constexpr (is_invertible)
        {
            current_sum.merge(value);
            window.emplace_back(timestamp, std::move(value));
        }
        else
        {
            window.emplace_back(timestamp, std::move(value));
            current_sum_valid = false;
        }
    }

    void removeBefore(TimestampType cut_off)
    {
        if constexpr (is_invertible)
        {
            while (!window.empty() && window.front().first <= cut_off)
            {
                const SummaryType leaving = std::move(window.front().second);
                window.pop_front();
                current_sum.unmerge(leaving, window.empty() ? nullptr : &window.front().second);
            }
        }
        else
        {
            bool removed = false;
            while (!window.empty() && window.front().first <= cut_off)
            {
                window.pop_front();
                removed = true;
            }
            if (removed)
                current_sum_valid = false;
        }
    }

    SummaryType getCurrentSum() const
    {
        if constexpr (is_invertible)
        {
            return current_sum;
        }
        else
        {
            /// Recompute path: recalculate the window's sum only when it changed (`current_sum_valid` is false).
            if (!current_sum_valid)
            {
                current_sum = SummaryType{};
                for (const auto & timestamp_and_value : window)
                    current_sum.merge(timestamp_and_value.second);
                current_sum_valid = true;
            }
            return current_sum;
        }
    }

private:
    mutable SummaryType current_sum;
    mutable bool current_sum_valid;
    DequeWithMemoryTracking<std::pair<TimestampType, SummaryType>> window;  /// in-window values in time order
};

}
