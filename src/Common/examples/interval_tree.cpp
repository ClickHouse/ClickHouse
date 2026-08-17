#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <set>
#include <vector>

#include <Common/randomSeed.h>
#include <Common/Stopwatch.h>
#include <Common/IntervalTree.h>

using namespace DB;
using Int64Interval = Interval<Int64>;

int main(int, char **)
{
    {
        IntervalSet<Int64Interval> tree;

        tree.emplace(Int64Interval(0, 5));
        tree.emplace(Int64Interval(10, 15));

        tree.build();

        for (const auto & interval : tree)
        {
            std::cout << "Interval left " << interval.left << " right " << interval.right << std::endl;
        }
    }
    {
        IntervalMap<Int64Interval, std::string> tree;

        tree.emplace(Int64Interval(0, 5), "value1");
        tree.emplace(Int64Interval(10, 15), "value2");

        tree.build();

        for (const auto & [interval, value] : tree)
        {
            std::cout << "Interval left " << interval.left << " right " << interval.right;
            std::cout << " value " << value << std::endl;
        }
    }
    {
        IntervalSet<Int64Interval> tree;
        for (size_t i = 0; i < 5; ++i)
        {
            tree.emplace(Int64Interval(0, i));
        }

        tree.build();

        for (const auto & interval : tree)
        {
            std::cout << "Interval left " << interval.left << " right " << interval.right << std::endl;
        }

        for (Int64 i = 0; i < 5; ++i)
        {
            tree.find(i, [](auto & interval)
            {
                std::cout << "Interval left " << interval.left << " right " << interval.right << std::endl;
                return true;
            });
        }
    }
    {
        IntervalMap<Int64Interval, std::string> tree;
        for (size_t i = 0; i < 5; ++i)
        {
            tree.emplace(Int64Interval(0, i), "Value " + std::to_string(i));
        }

        tree.build();

        for (const auto & [interval, value] : tree)
        {
            std::cout << "Interval left " << interval.left << " right " << interval.right;
            std::cout << " value " << value << std::endl;
        }

        for (Int64 i = 0; i < 5; ++i)
        {
            tree.find(i, [](auto & interval, auto & value)
            {
                std::cout << "Interval left " << interval.left << " right " << interval.right;
                std::cout << " value " << value << std::endl;

                return true;
            });
        }
    }

    return 0;
}
