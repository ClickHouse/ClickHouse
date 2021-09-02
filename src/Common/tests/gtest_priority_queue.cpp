#include <gtest/gtest.h>

#include <random>

#include <Common/PriorityQueue.h>

using namespace DB;

TEST(PriorityQueue, Simple)
{
    PriorityQueue<int> my;
    std::priority_queue<int> original;

    for (int i = 0; i < 1000; ++i)
    {
        my.push(i);
        original.emplace(i);
    }

    for (int i = 0; i < 1000; ++i)
    {
        ASSERT_EQ(my.pop(), original.top());
        original.pop();
    }
}
