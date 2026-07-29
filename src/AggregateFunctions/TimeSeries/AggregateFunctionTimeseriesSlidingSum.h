#pragma once

#include <utility>

#include <base/defines.h>

#include <Common/DequeWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>


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
///  - otherwise the value cannot be subtracted, so:
///     - if `stack_size > 0`: a two-stack monoid queue (the "Two-Stacks" sliding-window algorithm). Values entering at
///       the right edge are pushed onto the back stack, values leaving at the left edge are popped from the front
///       stack. This path combines values in a different order than they arrive, so it additionally requires
///       `merge` to be commutative (the recompute and invertible paths combine in time order and do not).
///     - if `stack_size == 0`: recompute - keep the in-window values in a deque and recalculate their sum per each grid point,
///       this algorithm is faster when the window holds only few buckets
template <typename TimestampType, typename SummaryType>
class AggregateFunctionTimeseriesSlidingSum
{
public:
    /// `SummaryType` is invertible when it can subtract a previously merged value.
    static constexpr bool is_invertible = requires (SummaryType summary, const SummaryType & value)
        { summary.unmerge(value, &value); };

    /// Default constructor: no two-stack queue (the invertible running-sum or recompute strategy).
    AggregateFunctionTimeseriesSlidingSum() : AggregateFunctionTimeseriesSlidingSum(0) {}

    explicit AggregateFunctionTimeseriesSlidingSum(size_t stack_size)
        : use_two_stacks(!is_invertible && stack_size != 0)
        , current_sum_valid(!is_invertible)
    {
        /// `stack_size` only selects the removal strategy for non-invertible types.
        chassert(!is_invertible || stack_size == 0);
        if (use_two_stacks)
        {
            back_stack.reserve(stack_size);
            front_stack.reserve(stack_size);
        }
    }

    void add(SummaryType && value, TimestampType timestamp)
    {
        if constexpr (is_invertible)
        {
            current_sum.merge(value);
            window.emplace_back(timestamp, std::move(value));
        }
        else if (use_two_stacks)
        {
            SummaryType combined = value;
            if (!back_stack.empty())
                combined.merge(back_stack.back().combined);
            back_stack.push_back({timestamp, std::move(value), std::move(combined)});
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
        else if (use_two_stacks)
        {
            while (true)
            {
                if (front_stack.empty())
                {
                    /// Flush the back stack into the front stack, reversing it so the oldest value ends up on top.
                    while (!back_stack.empty())
                    {
                        auto & back = back_stack.back();
                        SummaryType combined = back.single;
                        if (!front_stack.empty())
                            combined.merge(front_stack.back().combined);
                        /// `back` is popped right after, so move its single value into the front entry instead of copying it.
                        front_stack.push_back({back.last_timestamp, std::move(back.single), std::move(combined)});
                        back_stack.pop_back();
                    }
                }

                if (front_stack.empty() || front_stack.back().last_timestamp > cut_off)
                    break;
                front_stack.pop_back();
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
        else if (use_two_stacks)
        {
            SummaryType combined;
            if (!front_stack.empty())
                combined.merge(front_stack.back().combined);
            if (!back_stack.empty())
                combined.merge(back_stack.back().combined);
            return combined;
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
    struct StackEntry
    {
        TimestampType last_timestamp;
        SummaryType single;     /// this value alone
        SummaryType combined;   /// running combine over this stack up to this entry
    };

    bool use_two_stacks;
    mutable SummaryType current_sum;
    mutable bool current_sum_valid;
    VectorWithMemoryTracking<StackEntry> back_stack;   /// two-stacks: newer values; pushed here
    VectorWithMemoryTracking<StackEntry> front_stack;  /// two-stacks: older values; popped here
    DequeWithMemoryTracking<std::pair<TimestampType, SummaryType>> window;  /// invertible/recompute: in-window values in time order
};

}
