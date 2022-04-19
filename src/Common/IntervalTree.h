#pragma once

#include <base/defines.h>
#include <base/sort.h>

#include <vector>
#include <utility>


namespace DB
{

/** Structure that holds closed interval with left and right.
  * Interval left must be less than interval right.
  * Example: [1, 1] is valid interval, that contain point 1.
  */
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

/** Tree structure that allow to efficiently retrieve all intervals that intersect specific point.
  * https://en.wikipedia.org/wiki/Interval_tree
  *
  * Search for all intervals intersecting point has complexity O(log(n) + k), k is count of intervals that intersect point.
  * If we need to only check if there are some interval intersecting point such operation has complexity O(log(n)).
  *
  * There is invariant that interval left must be less than interval right, otherwise such interval could not contain any point.
  * If that invariant is broken, inserting such interval in IntervalTree will return false.
  *
  * Explanation:
  *
  * IntervalTree structure is balanced tree. Each node contains:
  * 1. Point
  * 2. Intervals sorted by left ascending that intersect that point.
  * 3. Intervals sorted by right descending that intersect that point.
  *
  * Build:
  *
  * To keep tree relatively balanced we can use median of all segment points.
  * On each step build tree node with intervals. For root node input intervals are all intervals.
  * First split intervals in 4 groups.
  * 1. Intervals that lie that are less than median point. Interval right is less than median point.
  * 2. Intervals that lie that are greater than median point. Interval right is less than median point.
  * 3. Intervals that intersect node sorted by left ascending.
  * 4. Intervals that intersect node sorted by right descending.
  *
  * If intervals in 1 group are not empty. Continue build left child recursively with intervals from 1 group.
  * If intervals in 2 group are not empty. Continue build right child recursively with intervals from 2 group.
  *
  * Search:
  *
  * Search for intervals intersecting point is started from root node.
  * If search point is less than point in node, then we check intervals sorted by left ascending
  * until left is greater than search point.
  * If there is left child, continue search recursively in left child.
  *
  * If search point is greater than point in node, then we check intervals sorted by right descending
  * until right is less than search point.
  * If there is right child, continue search recursively in right child.
  *
  * If search point is equal to point in node, then we can emit all intervals that intersect current tree node
  * and stop searching.
  *
  * Additional details:
  * 1. To improve cache locality tree is stored implicitly in array, after build method is called
  * other intervals cannot be added to the tree.
  * 2. Additionally to improve cache locality in tree node we store sorted intervals for all nodes in separate
  * array. In node we store only start of its sorted intervals, and also size of intersecting intervals.
  * If we need to retrieve intervals sorted by left ascending they will be stored in indexes
  * [sorted_intervals_start_index, sorted_intervals_start_index + intersecting_intervals_size).
  * If we need to retrieve intervals sorted by right descending they will be store in indexes
  * [sorted_intervals_start_index + intersecting_intervals_size, sorted_intervals_start_index + intersecting_intervals_size * 2).
  */
template <typename Interval, typename Value>
class IntervalTree
{
public:
    using IntervalStorageType = typename Interval::IntervalStorageType;

    static constexpr bool is_empty_value = std::is_same_v<Value, IntervalTreeVoidValue>;

    IntervalTree() { nodes.resize(1); }

    template <typename TValue = Value>
    requires std::is_same_v<Value, IntervalTreeVoidValue>
    ALWAYS_INLINE bool emplace(Interval interval)
    {
        assert(!tree_is_built);
        if (unlikely(interval.left > interval.right))
            return false;

        sorted_intervals.emplace_back(interval);
        increaseIntervalsSize();

        return true;
    }

    template <typename TValue = Value, std::enable_if_t<!std::is_same_v<TValue, IntervalTreeVoidValue>, bool> = true, typename... Args>
    ALWAYS_INLINE bool emplace(Interval interval, Args &&... args)
    {
        assert(!tree_is_built);
        if (unlikely(interval.left > interval.right))
            return false;

        sorted_intervals.emplace_back(
            std::piecewise_construct, std::forward_as_tuple(interval), std::forward_as_tuple(std::forward<Args>(args)...));
        increaseIntervalsSize();

        return true;
    }

    template <typename TValue = Value>
    requires std::is_same_v<TValue, IntervalTreeVoidValue>
    bool insert(Interval interval)
    {
        return emplace(interval);
    }

    template <typename TValue = Value>
    requires (!std::is_same_v<TValue, IntervalTreeVoidValue>)
    bool insert(Interval interval, const Value & value)
    {
        return emplace(interval, value);
    }

    template <typename TValue = Value>
    requires (!std::is_same_v<TValue, IntervalTreeVoidValue>)
    bool insert(Interval interval, Value && value)
    {
        return emplace(interval, std::move(value));
    }

    /// Build tree, after that intervals cannot be inserted, and only search or iteration can be performed.
    void build()
    {
        assert(!tree_is_built);
        nodes.clear();
        nodes.reserve(sorted_intervals.size());
        buildTree();
        tree_is_built = true;
    }

    /** Find all intervals intersecting point.
      *
      * Callback interface for IntervalSet:
      *
      * template <typename IntervalType>
      * struct IntervalSetCallback
      * {
      *     bool operator()(const IntervalType & interval)
      *     {
      *         bool should_continue_interval_iteration = false;
      *         return should_continue_interval_iteration;
      *     }
      * };
      *
      * Callback interface for IntervalMap:
      *
      * template <typename IntervalType, typename Value>
      * struct IntervalMapCallback
      * {
      *     bool operator()(const IntervalType & interval, const Value & value)
      *     {
      *         bool should_continue_interval_iteration = false;
      *         return should_continue_interval_iteration;
      *     }
      * };
      */

    template <typename IntervalCallback>
    void find(IntervalStorageType point, IntervalCallback && callback) const
    {
        if (unlikely(!tree_is_built))
        {
            findIntervalsNonConstructedImpl(point, callback);
            return;
        }

        findIntervalsImpl(point, callback);
    }

    /// Check if there is an interval intersecting point
    bool has(IntervalStorageType point) const
    {
        bool has_intervals = false;

        if constexpr (is_empty_value)
        {
            find(point, [&](auto &)
            {
                has_intervals = true;
                return false;
            });
        }
        else
        {
            find(point, [&](auto &, auto &)
            {
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
        size_t last_interval_index = 0;

        if (likely(end_index < nodes.size()))
            last_interval_index = nodes[end_index].sorted_intervals_range_size;

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
        size_t last_interval_index = 0;

        if (likely(end_index < nodes.size()))
            last_interval_index = nodes[end_index].sorted_intervals_range_size;

        return Iterator(end_index, last_interval_index, this);
    }

    const_iterator cbegin() const { return begin(); }

    const_iterator cend() const { return end(); }

    size_t getIntervalsSize() const { return intervals_size; }

    size_t getSizeInBytes() const
    {
        size_t nodes_size_in_bytes = nodes.size() * sizeof(Node);
        size_t intervals_size_in_bytes = sorted_intervals.size() * sizeof(IntervalWithValue);
        size_t result = nodes_size_in_bytes + intervals_size_in_bytes;

        return result;
    }

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
                    /// This is case when point == middle_element.
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
        /// Before tree is build we store all intervals size in our first node to allow tree iteration.
        ++intervals_size;
        nodes[0].sorted_intervals_range_size = intervals_size;
    }

    std::vector<Node> nodes;
    std::vector<IntervalWithValue> sorted_intervals;
    size_t intervals_size = 0;
    bool tree_is_built = false;

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

        ::nth_element(points.begin(), points.begin() + middle_element_index, points.end());

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
