#include <Common/IntervalTree.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <base/Decimal.h>
#include <Core/Types_fwd.h>


namespace DB
{

template <typename Interval, typename Value>
void IntervalTree<Interval, Value>::buildTree()
{
    std::vector<IntervalStorageType> temporary_points_storage;
    temporary_points_storage.reserve(sorted_intervals.size() * 2);

    std::vector<IntervalWithValue> left_intervals;
    std::vector<IntervalWithValue> right_intervals;
    std::vector<IntervalWithValue> intervals_sorted_by_left_asc;
    std::vector<IntervalWithValue> intervals_sorted_by_right_desc;

    struct StackFrame
    {
        size_t index;
        std::vector<IntervalWithValue> intervals;
    };

    std::vector<StackFrame> stack;
    stack.emplace_back(StackFrame{0, std::move(sorted_intervals)});
    sorted_intervals.clear();

    while (!stack.empty())
    {
        auto frame = std::move(stack.back());
        stack.pop_back();

        size_t current_index = frame.index;
        auto & current_intervals = frame.intervals;

        if (current_intervals.empty())
            continue;

        if (current_index >= nodes.size())
            nodes.resize(current_index + 1);

        temporary_points_storage.clear();
        intervalsToPoints(current_intervals, temporary_points_storage);
        auto median = pointsMedian(temporary_points_storage);

        left_intervals.clear();
        right_intervals.clear();
        intervals_sorted_by_left_asc.clear();
        intervals_sorted_by_right_desc.clear();

        for (const auto & interval_with_value : current_intervals)
        {
            auto & interval = getInterval(interval_with_value);

            if (interval.right < median)
            {
                left_intervals.emplace_back(interval_with_value);
            }
            else if (interval.left > median)
            {
                right_intervals.emplace_back(interval_with_value);
            }
            else
            {
                intervals_sorted_by_left_asc.emplace_back(interval_with_value);
                intervals_sorted_by_right_desc.emplace_back(interval_with_value);
            }
        }

        ::sort(intervals_sorted_by_left_asc.begin(), intervals_sorted_by_left_asc.end(), [](auto & lhs, auto & rhs)
        {
            auto & lhs_interval = getInterval(lhs);
            auto & rhs_interval = getInterval(rhs);
            return lhs_interval.left < rhs_interval.left;
        });

        ::sort(intervals_sorted_by_right_desc.begin(), intervals_sorted_by_right_desc.end(), [](auto & lhs, auto & rhs)
        {
            auto & lhs_interval = getInterval(lhs);
            auto & rhs_interval = getInterval(rhs);
            return lhs_interval.right > rhs_interval.right;
        });

        size_t sorted_intervals_range_start_index = sorted_intervals.size();

        for (auto && interval_sorted_by_left_asc : intervals_sorted_by_left_asc)
            sorted_intervals.emplace_back(std::move(interval_sorted_by_left_asc));

        for (auto && interval_sorted_by_right_desc : intervals_sorted_by_right_desc)
            sorted_intervals.emplace_back(std::move(interval_sorted_by_right_desc));

        auto & node = nodes[current_index];
        node.middle_element = median;
        node.sorted_intervals_range_start_index = sorted_intervals_range_start_index;
        node.sorted_intervals_range_size = intervals_sorted_by_left_asc.size();

        size_t left_child_index = current_index * 2 + 1;
        stack.emplace_back(StackFrame{left_child_index, std::move(left_intervals)});

        size_t right_child_index = current_index * 2 + 2;
        stack.emplace_back(StackFrame{right_child_index, std::move(right_intervals)});
    }
}


// Explicit template instantiations for Interval<T>

template struct Interval<UInt8>;
template struct Interval<UInt16>;
template struct Interval<UInt32>;
template struct Interval<UInt64>;
template struct Interval<UInt128>;
template struct Interval<UInt256>;
template struct Interval<Int8>;
template struct Interval<Int16>;
template struct Interval<Int32>;
template struct Interval<Int64>;
template struct Interval<Int128>;
template struct Interval<Int256>;
template struct Interval<Decimal32>;
template struct Interval<Decimal64>;
template struct Interval<Decimal128>;
template struct Interval<Decimal256>;
template struct Interval<DateTime64>;
template struct Interval<Time64>;
template struct Interval<Float32>;
template struct Interval<Float64>;

// Explicit template instantiations for IntervalTree with void value (IntervalSet)

template class IntervalTree<Interval<UInt8>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<UInt16>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<UInt32>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<UInt64>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<UInt128>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<UInt256>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Int8>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Int16>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Int32>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Int64>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Int128>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Int256>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Decimal32>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Decimal64>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Decimal128>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Decimal256>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<DateTime64>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Time64>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Float32>, IntervalTreeVoidValue>;
template class IntervalTree<Interval<Float64>, IntervalTreeVoidValue>;

// Explicit template instantiations for IntervalTree with size_t value (IntervalMap)

template class IntervalTree<Interval<UInt8>, size_t>;
template class IntervalTree<Interval<UInt16>, size_t>;
template class IntervalTree<Interval<UInt32>, size_t>;
template class IntervalTree<Interval<UInt64>, size_t>;
template class IntervalTree<Interval<UInt128>, size_t>;
template class IntervalTree<Interval<UInt256>, size_t>;
template class IntervalTree<Interval<Int8>, size_t>;
template class IntervalTree<Interval<Int16>, size_t>;
template class IntervalTree<Interval<Int32>, size_t>;
template class IntervalTree<Interval<Int64>, size_t>;
template class IntervalTree<Interval<Int128>, size_t>;
template class IntervalTree<Interval<Int256>, size_t>;
template class IntervalTree<Interval<Decimal32>, size_t>;
template class IntervalTree<Interval<Decimal64>, size_t>;
template class IntervalTree<Interval<Decimal128>, size_t>;
template class IntervalTree<Interval<Decimal256>, size_t>;
template class IntervalTree<Interval<DateTime64>, size_t>;
template class IntervalTree<Interval<Time64>, size_t>;
template class IntervalTree<Interval<Float32>, size_t>;
template class IntervalTree<Interval<Float64>, size_t>;

// Explicit template instantiation for test usage with std::string

template class IntervalTree<Interval<Int64>, std::string>;

}
