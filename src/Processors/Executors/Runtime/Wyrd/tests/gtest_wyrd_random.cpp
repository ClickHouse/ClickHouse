#include <Processors/Executors/Runtime/Wyrd/Random.h>

#include <gtest/gtest.h>

using namespace DB::Runtime::Wyrd;

TEST(Wyrd, RandomSeedReplaysTheSequence)
{
    setRandomSeed(7);
    UInt64 first = randomNumber();
    UInt64 second = randomNumber();

    setRandomSeed(7);
    EXPECT_EQ(randomNumber(), first);
    EXPECT_EQ(randomNumber(), second);

    setRandomSeed(8);
    EXPECT_NE(randomNumber(), first);
}

TEST(Wyrd, RandomEventFrequencies)
{
    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_TRUE(randomEventEvery(1));
        EXPECT_FALSE(randomEventEvery(0));
    }
}
