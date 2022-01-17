#pragma once

#include <base/defines.h>

#include <vector>
#include <utility>


namespace DB
{

template <typename TIntervalStorageType>
struct Interval
{
    using IntervalStorageType = TIntervalStorageType;
    IntervalStorageType left;
    IntervalStorageType right;

    Interval(IntervalStorageType left_, IntervalStorageType right_) : left(left_), right(right_) { }

    inline bool contains(IntervalStorageType point) const { return left <= point && point <= right; }
};

template <typename IntervalStorageType>
bool operator<(const Interval<IntervalStorageType> & lhs, const Interval<IntervalStorageType> & rhs)
{
    return std::tie(lhs.left, lhs.right) < std::tie(rhs.left, rhs.right);
}

template <typename IntervalStorageType>
bool operator<=(const Interval<IntervalStorageType> & lhs, const Interval<IntervalStorageType> & rhs)
{
    return std::tie(lhs.left, lhs.right) <= std::tie(rhs.left, rhs.right);
}

template <typename IntervalStorageType>
bool operator==(const Interval<IntervalStorageType> & lhs, const Interval<IntervalStorageType> & rhs)
{
    return std::tie(lhs.left, lhs.right) == std::tie(rhs.left, rhs.right);
}

template <typename IntervalStorageType>
bool operator!=(const Interval<IntervalStorageType> & lhs, const Interval<IntervalStorageType> & rhs)
{
    return std::tie(lhs.left, lhs.right) != std::tie(rhs.left, rhs.right);
}

template <typename IntervalStorageType>
bool operator>(const Interval<IntervalStorageType> & lhs, const Interval<IntervalStorageType> & rhs)
{
    return std::tie(lhs.left, lhs.right) > std::tie(rhs.left, rhs.right);
}

template <typename IntervalStorageType>
bool operator>=(const Interval<IntervalStorageType> & lhs, const Interval<IntervalStorageType> & rhs)
{
    return std::tie(lhs.left, lhs.right) >= std::tie(rhs.left, rhs.right);
}

struct IntervalTreeVoidValue
{
};

template <typename Interval, typename Value>
class IntervalTree
{
public:
    using IntervalStorageType = typename Interval::IntervalStorageType;

    static constexpr bool is_empty_value = std::is_same_v<Value, IntervalTreeVoidValue>;

    IntervalTree() { nodes.resize(1); }

    template <typename TValue = Value, std::enable_if_t<std::is_same_v<TValue, IntervalTreeVoidValue>, bool> = true>
    void emplace(Interval interval)
    {
        assert(!tree_constructed);
        sorted_intervals.emplace_back(interval);
        increaseIntervalsSize();
    }

    template <typename TValue = Value, std::enable_if_t<!std::is_same_v<TValue, IntervalTreeVoidValue>, bool> = true, typename... Args>
    void emplace(Interval interval, Args &&... args)
    {
        assert(!tree_constructed);
        sorted_intervals.emplace_back(
            std::piecewise_construct, std::forward_as_tuple(interval), std::forward_as_tuple(std::forward<Args>(args)...));
        increaseIntervalsSize();
    }

    template <typename TValue = Value, std::enable_if_t<std::is_same_v<TValue, IntervalTreeVoidValue>, bool> = true>
    void insert(Interval interval)
    {
        assert(!tree_constructed);
        sorted_intervals.emplace_back(interval);
        increaseIntervalsSize();
    }

    template <typename TValue = Value, std::enable_if_t<!std::is_same_v<TValue, IntervalTreeVoidValue>, bool> = true>
    void insert(Interval interval, const Value & value)
    {
        assert(!tree_constructed);
        sorted_intervals.emplace_back(std::piecewise_construct, interval, value);
        increaseIntervalsSize();
    }

    template <typename TValue = Value, std::enable_if_t<!std::is_same_v<TValue, IntervalTreeVoidValue>, bool> = true>
    void insert(Interval interval, Value && value)
    {
        assert(!tree_constructed);
        sorted_intervals.emplace_back(std::piecewise_construct, interval, std::move(value));
        increaseIntervalsSize();
    }

    void construct()
    {
        assert(!tree_constructed);
        nodes.clear();
        nodes.reserve(sorted_intervals.size());
        buildTree();
        tree_constructed = true;
    }

    template <typename IntervalCallback>
    void find(IntervalStorageType point, IntervalCallback && callback) const
    {
        if (unlikely(!tree_constructed))
        {
            findIntervalsNonConstructedImpl(point, callback);
            return;
        }

        findIntervalsImpl(point, callback);
    }

    bool has(IntervalStorageType point) const
    {
        bool has_intervals = false;

        if constexpr (is_empty_value)
        {
            find(point, [&](auto &) {
                has_intervals = true;
                return false;
            });
        }
        else
        {
            find(point, [&](auto &, auto &) {
                has_intervals = true;
                return false;
            });
        }

        return has_intervals;
    }

    class Iterator;
    using iterator = Iterator;
    using const_iterator = Iterator;

    iterator begin()
    {
        size_t start_index = findFirstIteratorNodeIndex();
        return Iterator(start_index, 0, this);
    }

    iterator end()
    {
        size_t end_index = findLastIteratorNodeIndex();
        size_t last_interval_index = end_index < nodes.size() ? nodes[end_index].sorted_intervals_range_size : 0;
        return Iterator(end_index, last_interval_index, this);
    }

    const_iterator begin() const
    {
        size_t start_index = findFirstIteratorNodeIndex();
        return Iterator(start_index, 0, this);
    }

    const_iterator end() const
    {
        size_t end_index = findLastIteratorNodeIndex();
        size_t last_interval_index = end_index < nodes.size() ? nodes[end_index].sorted_intervals_range_size : 0;

        return Iterator(end_index, last_interval_index, this);
    }

    const_iterator cbegin() const { return begin(); }

    const_iterator cend() const { return end(); }

    size_t getIntervalsSize() const { return intervals_size; }

private:
    struct Node
    {
        size_t sorted_intervals_range_start_index;
        size_t sorted_intervals_range_size;

        IntervalStorageType middle_element;

        inline bool hasValue() const { return sorted_intervals_range_size != 0; }
    };

    using IntervalWithEmptyValue = Interval;
    using IntervalWithNonEmptyValue = std::pair<Interval, Value>;

    using IntervalWithValue = std::conditional_t<is_empty_value, IntervalWithEmptyValue, IntervalWithNonEmptyValue>;

public:
    class Iterator
    {
    public:
        bool operator==(const Iterator & rhs) const
        {
            return node_index == rhs.node_index && current_interval_index == rhs.current_interval_index && tree == rhs.tree;
        }

        bool operator!=(const Iterator & rhs) const { return !(*this == rhs); }

        const IntervalWithValue & operator*() { return getCurrentValue(); }

        const IntervalWithValue & operator*() const { return getCurrentValue(); }

        const IntervalWithValue * operator->() { return &getCurrentValue(); }

        const IntervalWithValue * operator->() const { return &getCurrentValue(); }

        Iterator & operator++()
        {
            iterateToNext();
            return *this;
        }

        Iterator operator++(int) // NOLINT
        {
            Iterator copy(*this);
            iterateToNext();
            return copy;
        }

        Iterator & operator--()
        {
            iterateToPrevious();
            return *this;
        }

        Iterator operator--(int) // NOLINT
        {
            Iterator copy(*this);
            iterateToPrevious();
            return copy;
        }

    private:
        friend class IntervalTree;

        Iterator(size_t node_index_, size_t current_interval_index_, const IntervalTree * tree_)
            : node_index(node_index_), current_interval_index(current_interval_index_), tree(tree_)
        {
        }

        size_t node_index;
        size_t current_interval_index;
        const IntervalTree * tree;

        void iterateToNext()
        {
            size_t nodes_size = tree->nodes.size();
            auto & current_node = tree->nodes[node_index];

            ++current_interval_index;

            if (current_interval_index < current_node.sorted_intervals_range_size)
                return;

            size_t node_index_copy = node_index + 1;
            for (; node_index_copy < nodes_size; ++node_index_copy)
            {
                auto & node = tree->nodes[node_index_copy];

                if (node.hasValue())
                {
                    node_index = node_index_copy;
                    current_interval_index = 0;
                    break;
                }
            }
        }

        void iterateToPrevious()
        {
            if (current_interval_index > 0)
            {
                --current_interval_index;
                return;
            }

            while (node_index > 0)
            {
                auto & node = tree->nodes[node_index - 1];
                if (node.hasValue())
                {
                    current_interval_index = node.sorted_intervals_range_size - 1;
                    break;
                }

                --node_index;
            }
        }

        const IntervalWithValue & getCurrentValue() const
        {
            auto & current_node = tree->nodes[node_index];
            size_t interval_index = current_node.sorted_intervals_range_start_index + current_interval_index;
            return tree->sorted_intervals[interval_index];
        }
    };

private:
    void buildTree()
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

            std::sort(intervals_sorted_by_left_asc.begin(), intervals_sorted_by_left_asc.end(), [](auto & lhs, auto & rhs) {
                auto & lhs_interval = getInterval(lhs);
                auto & rhs_interval = getInterval(rhs);
                return lhs_interval.left < rhs_interval.left;
            });

            std::sort(intervals_sorted_by_right_desc.begin(), intervals_sorted_by_right_desc.end(), [](auto & lhs, auto & rhs) {
                auto & lhs_interval = getInterval(lhs);
                auto & rhs_interval = getInterval(rhs);
                return lhs_interval.right > rhs_interval.right;
            });

            size_t sorted_intervals_range_start_index = sorted_intervals.size();

            sorted_intervals.insert(sorted_intervals.end(), intervals_sorted_by_left_asc.begin(), intervals_sorted_by_left_asc.end());
            sorted_intervals.insert(sorted_intervals.end(), intervals_sorted_by_right_desc.begin(), intervals_sorted_by_right_desc.end());

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

    template <typename IntervalCallback>
    void findIntervalsImpl(IntervalStorageType point, IntervalCallback && callback) const
    {
        size_t current_index = 0;

        while (true)
        {
            if (current_index >= nodes.size())
                break;

            auto & node = nodes[current_index];
            if (!node.hasValue())
                break;

            auto middle_element = node.middle_element;

            if (point < middle_element)
            {
                size_t start = node.sorted_intervals_range_start_index;
                size_t end = start + node.sorted_intervals_range_size;

                for (; start != end; ++start)
                {
                    auto & interval_with_value_left_sorted_asc = sorted_intervals[start];
                    auto & interval_left_sorted_asc = getInterval(interval_with_value_left_sorted_asc);
                    if (interval_left_sorted_asc.left > point)
                        break;

                    bool should_continue = callCallback(interval_with_value_left_sorted_asc, callback);
                    if (unlikely(!should_continue))
                        return;
                }

                size_t left_child_index = current_index * 2 + 1;
                current_index = left_child_index;
            }
            else
            {
                size_t start = node.sorted_intervals_range_start_index + node.sorted_intervals_range_size;
                size_t end = start + node.sorted_intervals_range_size;

                for (; start != end; ++start)
                {
                    auto & interval_with_value_right_sorted_desc = sorted_intervals[start];
                    auto & interval_right_sorted_desc = getInterval(interval_with_value_right_sorted_desc);
                    if (interval_right_sorted_desc.right < point)
                        break;

                    bool should_continue = callCallback(interval_with_value_right_sorted_desc, callback);
                    if (unlikely(!should_continue))
                        return;
                }

                if (likely(point > middle_element))
                {
                    size_t right_child_index = current_index * 2 + 2;
                    current_index = right_child_index;
                }
                else
                {
                    break;
                }
            }
        }
    }

    template <typename IntervalCallback>
    void findIntervalsNonConstructedImpl(IntervalStorageType point, IntervalCallback && callback) const
    {
        for (auto & interval_with_value : sorted_intervals)
        {
            auto & interval = getInterval(interval_with_value);

            if (interval.contains(point))
                callCallback(interval_with_value, callback);
        }
    }

    inline size_t findFirstIteratorNodeIndex() const
    {
        size_t nodes_size = nodes.size();
        size_t result_index = 0;

        for (; result_index < nodes_size; ++result_index)
        {
            if (nodes[result_index].hasValue())
                break;
        }

        if (unlikely(result_index == nodes_size))
            result_index = 0;

        return result_index;
    }

    inline size_t findLastIteratorNodeIndex() const
    {
        if (unlikely(nodes.empty()))
            return 0;

        size_t nodes_size = nodes.size();
        size_t result_index = nodes_size - 1;
        for (; result_index != 0; --result_index)
        {
            if (nodes[result_index].hasValue())
                break;
        }

        return result_index;
    }

    inline void increaseIntervalsSize()
    {
        ++intervals_size;
        nodes[0].sorted_intervals_range_size = intervals_size;
    }

    std::vector<Node> nodes;
    std::vector<IntervalWithValue> sorted_intervals;
    size_t intervals_size = 0;
    bool tree_constructed = false;

    static inline const Interval & getInterval(const IntervalWithValue & interval_with_value)
    {
        if constexpr (is_empty_value)
            return interval_with_value;
        else
            return interval_with_value.first;
    }

    template <typename IntervalCallback>
    static inline bool callCallback(const IntervalWithValue & interval, IntervalCallback && callback)
    {
        if constexpr (is_empty_value)
            return callback(interval);
        else
            return callback(interval.first, interval.second);
    }

    static inline void
    intervalsToPoints(const std::vector<IntervalWithValue> & intervals, std::vector<IntervalStorageType> & temporary_points_storage)
    {
        for (const auto & interval_with_value : intervals)
        {
            auto & interval = getInterval(interval_with_value);
            temporary_points_storage.emplace_back(interval.left);
            temporary_points_storage.emplace_back(interval.right);
        }
    }

    static inline IntervalStorageType pointsMedian(std::vector<IntervalStorageType> & points)
    {
        size_t size = points.size();
        size_t middle_element_index = size / 2;

        std::nth_element(points.begin(), points.begin() + middle_element_index, points.end());

        /** We should not get median as average of middle_element_index and middle_element_index - 1
          * because we want point in node to intersect some interval.
          * Example: Intervals [1, 1], [3, 3]. If we choose 2 as average point, it does not intersect any interval.
          */
        return points[middle_element_index];
    }
};

template <typename IntervalType>
using IntervalSet = IntervalTree<IntervalType, IntervalTreeVoidValue>;

template <typename IntervalType, typename Value>
using IntervalMap = IntervalTree<IntervalType, Value>;

}
