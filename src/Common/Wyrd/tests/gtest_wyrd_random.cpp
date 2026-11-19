#include <Processors/Executors/Runtime/Wyrd/Random.h>

#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>

using namespace DB::Runtime::Wyrd;

WYRD_TEST(Wyrd, RandomSeedReplaysTheSequence)
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

WYRD_TEST(Wyrd, RandomEventProbabilities)
{
    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_TRUE(randomEvent(1.0));
        EXPECT_FALSE(randomEvent(0.0));
    }
}
