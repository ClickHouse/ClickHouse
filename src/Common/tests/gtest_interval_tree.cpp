#include <gtest/gtest.h>

#include <set>
#include <map>

#include <base/types.h>
#include <Common/IntervalTree.h>


using namespace DB;
using Int64Interval = Interval<Int64>;

template <typename IntervalType>
std::set<IntervalType> intervalSetToSet(const IntervalSet<IntervalType> & interval_set)
{
    std::set<IntervalType> result;

    for (const auto & interval : interval_set)
        result.insert(interval);

    return result;
}

template <typename IntervalType, typename Value>
std::map<IntervalType, Value> intervalMapToMap(const IntervalMap<IntervalType, Value> & interval_map)
{
    std::map<IntervalType, Value> result;

    for (const auto & [interval, value] : interval_map)
        result.emplace(interval, value);

    return result;
}

template <typename IntervalType>
struct CollectIntervalsSetCallback
{
    explicit CollectIntervalsSetCallback(std::set<IntervalType> & result_intervals_)
        : result_intervals(result_intervals_)
    {
    }

    bool operator()(IntervalType interval)
    {
        result_intervals.insert(interval);
        return true;
    }

    std::set<IntervalType> & result_intervals;
};

using CollectIntervalsSetInt64Callback = CollectIntervalsSetCallback<Int64Interval>;

template <typename IntervalType>
std::set<IntervalType> intervalSetFindIntervals(const IntervalSet<IntervalType> & interval_set, typename IntervalType::IntervalStorageType point)
{
    std::set<IntervalType> result;
    CollectIntervalsSetCallback<IntervalType> callback(result);

    interval_set.find(point, callback);

    return result;
}

template <typename IntervalType, typename Value>
struct CollectIntervalsMapCallback
{
    explicit CollectIntervalsMapCallback(std::map<IntervalType, Value> & result_intervals_)
        : result_intervals(result_intervals_)
    {
    }

    bool operator()(IntervalType interval, const Value & value)
    {
        result_intervals.emplace(interval, value);
        return true;
    }

    std::map<IntervalType, Value> & result_intervals;
};


template <typename IntervalType, typename Value>
std::map<IntervalType, Value> intervalMapFindIntervals(const IntervalMap<IntervalType, Value> & interval_set, typename IntervalType::IntervalStorageType point)
{
    std::map<IntervalType, Value> result;
    CollectIntervalsMapCallback callback(result);

    interval_set.find(point, callback);

    return result;
}

TEST(IntervalTree, IntervalSetBasic)
{
    for (size_t intervals_size = 0; intervals_size < 120; ++intervals_size)
    {
        std::set<Int64Interval> expected;
        IntervalSet<Int64Interval> set;

        for (size_t interval_index = 0; interval_index < intervals_size; ++interval_index)
        {
            auto interval = Int64Interval(interval_index * 2, interval_index * 2 + 1);
            expected.insert(interval);
            set.insert(interval);
        }

        ASSERT_TRUE(set.getIntervalsSize() == expected.size());
        ASSERT_TRUE(set.getIntervalsSize() == intervals_size);
        ASSERT_TRUE(intervalSetToSet(set) == expected);

        for (const auto & expected_interval : expected)
        {
            std::set<Int64Interval> expected_intervals = {{expected_interval}};

            auto actual_intervals = intervalSetFindIntervals(set, expected_interval.left);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            actual_intervals = intervalSetFindIntervals(set, expected_interval.right);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            ASSERT_TRUE(set.has(expected_interval.left));
            ASSERT_TRUE(set.has(expected_interval.right));
        }

        set.build();

        ASSERT_TRUE(intervalSetToSet(set) == expected);

        for (const auto & expected_interval : expected)
        {
            auto actual_interval = intervalSetFindIntervals(set, expected_interval.left);
            ASSERT_TRUE(actual_interval.size() == 1);
            ASSERT_TRUE(actual_interval == std::set<Int64Interval>{expected_interval});

            actual_interval = intervalSetFindIntervals(set, expected_interval.right);
            ASSERT_TRUE(actual_interval.size() == 1);
            ASSERT_TRUE(actual_interval == std::set<Int64Interval>{expected_interval});

            ASSERT_TRUE(set.has(expected_interval.left));
            ASSERT_TRUE(set.has(expected_interval.right));
        }
    }
}

TEST(IntervalTree, IntervalSetPoints)
{
    for (size_t intervals_size = 0; intervals_size < 120; ++intervals_size)
    {
        std::set<Int64Interval> expected;
        IntervalSet<Int64Interval> set;

        for (size_t interval_index = 0; interval_index < intervals_size; ++interval_index)
        {
            auto interval = Int64Interval(interval_index, interval_index);
            expected.insert(interval);
            set.insert(interval);
        }

        ASSERT_TRUE(set.getIntervalsSize() == expected.size());
        ASSERT_TRUE(set.getIntervalsSize() == intervals_size);
        ASSERT_TRUE(intervalSetToSet(set) == expected);

        for (const auto & expected_interval : expected)
        {
            std::set<Int64Interval> expected_intervals = {{expected_interval}};

            auto actual_intervals = intervalSetFindIntervals(set, expected_interval.left);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            actual_intervals = intervalSetFindIntervals(set, expected_interval.right);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            ASSERT_TRUE(set.has(expected_interval.left));
            ASSERT_TRUE(set.has(expected_interval.right));
        }

        set.build();

        ASSERT_TRUE(intervalSetToSet(set) == expected);

        for (const auto & expected_interval : expected)
        {
            auto actual_interval = intervalSetFindIntervals(set, expected_interval.left);
            ASSERT_TRUE(actual_interval.size() == 1);
            ASSERT_TRUE(actual_interval == std::set<Int64Interval>{expected_interval});

            actual_interval = intervalSetFindIntervals(set, expected_interval.right);
            ASSERT_TRUE(actual_interval.size() == 1);
            ASSERT_TRUE(actual_interval == std::set<Int64Interval>{expected_interval});

            ASSERT_TRUE(set.has(expected_interval.left));
            ASSERT_TRUE(set.has(expected_interval.right));
        }
    }
}

TEST(IntervalTree, IntervalSetIntersectingIntervals)
{
    for (size_t intervals_size = 0; intervals_size < 120; ++intervals_size)
    {
        std::set<Int64Interval> expected;
        IntervalSet<Int64Interval> set;

        for (size_t interval_index = 0; interval_index < intervals_size; ++interval_index)
        {
            auto interval = Int64Interval(0, interval_index * 2 + 1);
            expected.insert(interval);
            set.insert(interval);
        }

        ASSERT_TRUE(set.getIntervalsSize() == expected.size());
        ASSERT_TRUE(set.getIntervalsSize() == intervals_size);
        ASSERT_TRUE(intervalSetToSet(set) == expected);

        for (const auto & expected_interval : expected)
        {
            auto actual_intervals = intervalSetFindIntervals(set, expected_interval.left);
            ASSERT_TRUE(actual_intervals.size() == expected.size());
            ASSERT_TRUE(actual_intervals == expected);

            ASSERT_TRUE(set.has(expected_interval.left));
            ASSERT_TRUE(set.has(expected_interval.right));
        }

        set.build();

        ASSERT_TRUE(intervalSetToSet(set) == expected);

        for (const auto & expected_interval : expected)
        {
            auto actual_intervals = intervalSetFindIntervals(set, expected_interval.left);
            ASSERT_TRUE(actual_intervals.size() == expected.size());
            ASSERT_TRUE(actual_intervals == expected);

            ASSERT_TRUE(set.has(expected_interval.left));
            ASSERT_TRUE(set.has(expected_interval.right));
        }
    }
}

TEST(IntervalTree, IntervalSetIterators)
{
    {
        IntervalSet<Int64Interval> set;
        ASSERT_TRUE(set.begin() == set.end());
        ASSERT_TRUE(set.cbegin() == set.cend());
        set.build();
        ASSERT_TRUE(set.begin() == set.end());
        ASSERT_TRUE(set.cbegin() == set.cend());
    }
    {
        IntervalSet<Int64Interval> set;
        set.emplace(Int64Interval(0, 5));
        ASSERT_TRUE(set.begin() != set.end());
        ASSERT_TRUE((*set.begin()).left == 0);
        ASSERT_TRUE((*set.begin()).right == 5);
        ASSERT_TRUE(set.begin()->left == 0);
        ASSERT_TRUE(set.begin()->right == 5);
        auto begin = set.begin();
        ++begin;
        ASSERT_TRUE(begin == set.end());

        begin = set.begin();
        begin++;
        ASSERT_TRUE(begin == set.end());

        auto end = set.end();
        --end;
        ASSERT_TRUE(set.begin() == end);

        end = set.end();
        end--;
        ASSERT_TRUE(set.begin() == end);
    }
    {
        for (size_t intervals_size = 0; intervals_size < 120; ++intervals_size)
        {
            std::set<Int64Interval> expected;
            IntervalSet<Int64Interval> set;

            for (size_t interval_index = 0; interval_index < intervals_size; ++interval_index)
            {
                auto interval = Int64Interval(interval_index * 2, interval_index * 2 + 1);
                set.insert(interval);
                expected.insert(interval);
            }

            auto end = set.end();
            auto begin = set.begin();

            std::set<Int64Interval> actual;

            while (end != begin)
            {
                --end;
                actual.insert(*end);
            }

            if (end != begin)
                actual.insert(*end);

            ASSERT_TRUE(actual == expected);
        }
    }
}

TEST(IntervalTree, IntervalSetInvalidInterval)
{
    IntervalSet<Int64Interval> interval_set;
    ASSERT_TRUE(!interval_set.insert(Int64Interval(10, 0)));
    ASSERT_TRUE(!interval_set.insert(Int64Interval(15, 10)));
    ASSERT_TRUE(interval_set.insert(Int64Interval(20, 25)));

    std::set<Int64Interval> expected;
    expected.insert({20, 25});

    auto actual = intervalSetFindIntervals(interval_set, 20);

    ASSERT_TRUE(actual == expected);
    ASSERT_TRUE(interval_set.has(20));

    interval_set.build();

    actual = intervalSetFindIntervals(interval_set, 20);

    ASSERT_TRUE(actual == expected);
    ASSERT_TRUE(interval_set.has(20));
}

TEST(IntervalTree, IntervalMapBasic)
{
    for (size_t intervals_size = 0; intervals_size < 120; ++intervals_size)
    {
        std::map<Int64Interval, std::string> expected;
        IntervalMap<Int64Interval, std::string> map;

        for (size_t interval_index = 0; interval_index < intervals_size; ++interval_index)
        {
            auto interval = Int64Interval(interval_index * 2, interval_index * 2 + 1);
            auto value = std::to_string(interval.left);
            expected.emplace(interval, value);
            map.emplace(interval, value);
        }

        ASSERT_TRUE(map.getIntervalsSize() == expected.size());
        ASSERT_TRUE(map.getIntervalsSize() == intervals_size);
        ASSERT_TRUE(intervalMapToMap(map) == expected);

        for (const auto & [expected_interval, value] : expected)
        {
            std::map<Int64Interval, std::string> expected_intervals = {{expected_interval, std::to_string(expected_interval.left)}};

            auto actual_intervals = intervalMapFindIntervals(map, expected_interval.left);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            actual_intervals = intervalMapFindIntervals(map, expected_interval.right);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            ASSERT_TRUE(map.has(expected_interval.left));
            ASSERT_TRUE(map.has(expected_interval.right));
        }

        map.build();

        ASSERT_TRUE(intervalMapToMap(map) == expected);

        for (const auto & [expected_interval, value] : expected)
        {
            std::map<Int64Interval, std::string> expected_intervals = {{expected_interval, std::to_string(expected_interval.left)}};

            auto actual_intervals = intervalMapFindIntervals(map, expected_interval.left);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            actual_intervals = intervalMapFindIntervals(map, expected_interval.right);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            ASSERT_TRUE(map.has(expected_interval.left));
            ASSERT_TRUE(map.has(expected_interval.right));
        }
    }
}

TEST(IntervalTree, IntervalMapPoints)
{
    for (size_t intervals_size = 0; intervals_size < 120; ++intervals_size)
    {
        std::map<Int64Interval, std::string> expected;
        IntervalMap<Int64Interval, std::string> map;

        for (size_t interval_index = 0; interval_index < intervals_size; ++interval_index)
        {
            auto interval = Int64Interval(interval_index, interval_index);
            auto value = std::to_string(interval.left);
            expected.emplace(interval, value);
            map.emplace(interval, value);
        }

        ASSERT_TRUE(map.getIntervalsSize() == expected.size());
        ASSERT_TRUE(map.getIntervalsSize() == intervals_size);
        ASSERT_TRUE(intervalMapToMap(map) == expected);

        for (const auto & [expected_interval, value] : expected)
        {
            std::map<Int64Interval, std::string> expected_intervals = {{expected_interval, std::to_string(expected_interval.left)}};

            auto actual_intervals = intervalMapFindIntervals(map, expected_interval.left);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            actual_intervals = intervalMapFindIntervals(map, expected_interval.right);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            ASSERT_TRUE(map.has(expected_interval.left));
            ASSERT_TRUE(map.has(expected_interval.right));
        }

        map.build();

        ASSERT_TRUE(intervalMapToMap(map) == expected);

        for (const auto & [expected_interval, value] : expected)
        {
            std::map<Int64Interval, std::string> expected_intervals = {{expected_interval, std::to_string(expected_interval.left)}};

            auto actual_intervals = intervalMapFindIntervals(map, expected_interval.left);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            actual_intervals = intervalMapFindIntervals(map, expected_interval.right);
            ASSERT_TRUE(actual_intervals.size() == 1);
            ASSERT_TRUE(actual_intervals == expected_intervals);

            ASSERT_TRUE(map.has(expected_interval.left));
            ASSERT_TRUE(map.has(expected_interval.right));
        }
    }
}

TEST(IntervalTree, IntervalMapIntersectingIntervals)
{
    for (size_t intervals_size = 0; intervals_size < 120; ++intervals_size)
    {
        std::map<Int64Interval, std::string> expected;
        IntervalMap<Int64Interval, std::string> map;

        for (size_t interval_index = 0; interval_index < intervals_size; ++interval_index)
        {
            auto interval = Int64Interval(0, interval_index * 2 + 1);
            auto value = std::to_string(interval.left);
            expected.emplace(interval, value);
            map.emplace(interval, value);
        }

        ASSERT_TRUE(map.getIntervalsSize() == expected.size());
        ASSERT_TRUE(map.getIntervalsSize() == intervals_size);
        ASSERT_TRUE(intervalMapToMap(map) == expected);

        for (const auto & [expected_interval, value] : expected)
        {
            auto actual_intervals = intervalMapFindIntervals(map, expected_interval.left);

            ASSERT_TRUE(actual_intervals.size() == expected.size());
            ASSERT_TRUE(actual_intervals == expected);

            ASSERT_TRUE(map.has(expected_interval.left));
            ASSERT_TRUE(map.has(expected_interval.right));
        }

        map.build();

        ASSERT_TRUE(intervalMapToMap(map) == expected);

        for (const auto & [expected_interval, value] : expected)
        {
            auto actual_intervals = intervalMapFindIntervals(map, expected_interval.left);

            ASSERT_TRUE(actual_intervals.size() == expected.size());
            ASSERT_TRUE(actual_intervals == expected);

            ASSERT_TRUE(map.has(expected_interval.left));
            ASSERT_TRUE(map.has(expected_interval.right));
        }
    }
}

TEST(IntervalTree, IntervalMapIterators)
{
    {
        IntervalMap<Int64Interval, std::string> map;
        ASSERT_TRUE(map.begin() == map.end());
        ASSERT_TRUE(map.cbegin() == map.cend());
        map.build();
        ASSERT_TRUE(map.begin() == map.end());
        ASSERT_TRUE(map.cbegin() == map.cend());
    }
    {
        IntervalMap<Int64Interval, std::string> map;
        map.emplace(Int64Interval(0, 5), "value");
        ASSERT_TRUE(map.begin() != map.end());
        ASSERT_TRUE((*map.begin()).first.left == 0);
        ASSERT_TRUE((*map.begin()).first.right == 5);
        ASSERT_TRUE((*map.begin()).second == "value");
        ASSERT_TRUE(map.begin()->first.left == 0);
        ASSERT_TRUE(map.begin()->first.right == 5);
        ASSERT_TRUE(map.begin()->second == "value");
        auto begin = map.begin();
        ++begin;
        ASSERT_TRUE(begin == map.end());

        begin = map.begin();
        begin++;
        ASSERT_TRUE(begin == map.end());

        auto end = map.end();
        --end;
        ASSERT_TRUE(map.begin() == end);

        end = map.end();
        end--;
        ASSERT_TRUE(map.begin() == end);
    }
    {
        for (size_t intervals_size = 0; intervals_size < 120; ++intervals_size)
        {
            std::map<Int64Interval, std::string> expected;
            IntervalMap<Int64Interval, std::string> map;

            for (size_t interval_index = 0; interval_index < intervals_size; ++interval_index)
            {
                auto interval = Int64Interval(interval_index * 2, interval_index * 2 + 1);
                auto value = std::to_string(interval.left);
                map.emplace(interval, value);
                expected.emplace(interval, value);
            }

            auto end = map.end();
            auto begin = map.begin();

            std::map<Int64Interval, std::string> actual;

            while (end != begin)
            {
                --end;
                actual.insert(*end);
            }

            if (end != begin)
                actual.insert(*end);

            ASSERT_TRUE(actual == expected);
        }
    }
}

TEST(IntervalTree, IntervalMapInvalidInterval)
{
    IntervalMap<Int64Interval, std::string> interval_map;
    ASSERT_TRUE(!interval_map.insert(Int64Interval(10, 0), "Value"));
    ASSERT_TRUE(!interval_map.insert(Int64Interval(15, 10), "Value"));
    ASSERT_TRUE(interval_map.insert(Int64Interval(20, 25), "Value"));

    std::map<Int64Interval, std::string> expected;
    expected.emplace(Int64Interval{20, 25}, "Value");

    auto actual = intervalMapFindIntervals(interval_map, 20);

    ASSERT_TRUE(actual == expected);
    ASSERT_TRUE(interval_map.has(20));

    interval_map.build();

    actual = intervalMapFindIntervals(interval_map, 20);

    ASSERT_TRUE(actual == expected);
    ASSERT_TRUE(interval_map.has(20));
}
