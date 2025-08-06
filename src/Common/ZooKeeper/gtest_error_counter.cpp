#include <gtest/gtest.h>
#include <Coordination/ErrorCounter.h>
#include <algorithm>

using namespace Coordination;

TEST(ErrorCounter, IncrementSingleError)
{
    ErrorCounter counter;
    counter.increment(Error::ZCONNECTIONLOSS);
    
    auto it = std::find_if(counter.begin(), counter.end(), 
        [](const auto & pair) { return pair.first == Error::ZCONNECTIONLOSS; });
    
    ASSERT_NE(it, counter.end());
    EXPECT_EQ(it->second, 1);
}

TEST(ErrorCounter, IncrementMultipleTimes)
{
    ErrorCounter counter;
    counter.increment(Error::ZNONODE);
    counter.increment(Error::ZNONODE);
    counter.increment(Error::ZNONODE);
    
    auto it = std::find_if(counter.begin(), counter.end(),
        [](const auto& pair) { return pair.first == Error::ZNONODE; });
    
    ASSERT_NE(it, counter.end());
    EXPECT_EQ(it->second, 3);
}

TEST(ErrorCounter, IncrementDifferentErrors)
{
    ErrorCounter counter;
    counter.increment(Error::ZCONNECTIONLOSS);
    counter.increment(Error::ZNONODE);
    counter.increment(Error::ZSESSIONEXPIRED);
    counter.increment(Error::ZNONODE);
    
    std::map<Error, UInt32> expected = {
        {Error::ZCONNECTIONLOSS, 1},
        {Error::ZNONODE, 2},
        {Error::ZSESSIONEXPIRED, 1}
    };
    
    for (const auto & [error, count] : counter)
    {
        if (expected.count(error))
        {
            EXPECT_EQ(count, expected[error]);
        }
        else
        {
            EXPECT_EQ(count, 0);
        }
    }
}

TEST(ErrorCounter, IteratorTraversesAllErrors)
{
    ErrorCounter counter;
    size_t total = 0;
    for (const auto & pair : counter)
    {
        total++;
    }
    EXPECT_EQ(total, magic_enum::enum_count<Error>());
}

TEST(ErrorCounter, ZeroCountsForUnobservedErrors)
{
    ErrorCounter counter;
    counter.increment(Error::ZNONODE);
    
    for (const auto & [error, count] : counter)
    {
        if (error == Error::ZNONODE)
        {
            EXPECT_EQ(count, 1);
        }
        else
        {
            EXPECT_EQ(count, 0);
        }
    }
}

TEST(ErrorCounter, IteratorPostIncrement)
{
    ErrorCounter counter;
    auto it = counter.begin();
    Error first_error = (*it).first;
    ++it;
    Error second_error = (*it).first;
    EXPECT_NE(first_error, second_error);
}

TEST(ErrorCounter, RangeBasedForLoop)
{
    ErrorCounter counter;
    counter.increment(Error::ZBADARGUMENTS);
    counter.increment(Error::ZBADARGUMENTS);
    
    bool found = false;
    for (const auto & [error, count] : counter)
    {
        if (error == Error::ZBADARGUMENTS)
        {
            EXPECT_EQ(count, 2);
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

TEST(ErrorCounter, STLAlgorithmsCompatibility)
{
    ErrorCounter counter;
    counter.increment(Error::ZCONNECTIONLOSS);
    counter.increment(Error::ZNONODE);
    counter.increment(Error::ZNONODE);
    
    // Count non-zero errors
    auto non_zero_count = std::count_if(counter.begin(), counter.end(),
        [](const auto& pair) { return pair.second > 0; });
    EXPECT_EQ(non_zero_count, 2);
    
    // Sum all error counts
    auto total = std::accumulate(counter.begin(), counter.end(), 0u,
        [](UInt32 sum, const auto & pair) { return sum + pair.second; });
    EXPECT_EQ(total, 3);
}
