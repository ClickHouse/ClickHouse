#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Race.h>

using namespace DB::Runtime::Wyrd;

WYRD_TEST(Wyrd, FutexWakeAllReleasesEveryWaiter)
{
    Atomic<UInt32> fired{0};
    Atomic<size_t> released{0};
    bool data = false;

    Race race;
    race.add(
        2,
        [&](size_t)
        {
            UInt32 value = fired.load();
            while (value == 0)
                futexWaitFetch(fired, value);
            EXPECT_TRUE(data);
            released.fetch_add(1);
        });
    race.add(
        1,
        [&](size_t)
        {
            data = true;
            fired.store(1);
            futexWakeAll(fired);
        });
    race.run();

    EXPECT_EQ(released.load(), 2u);
}

WYRD_TEST(Wyrd, FutexWakeOneHandsTokensOneByOne)
{
    Atomic<UInt32> tokens{0};
    Atomic<size_t> consumed{0};

    Race race;
    race.add(
        2,
        [&](size_t)
        {
            while (true)
            {
                UInt32 value = tokens.load();
                while (value == 0)
                    futexWaitFetch(tokens, value);
                if (tokens.compare_exchange_strong(value, value - 1))
                {
                    consumed.fetch_add(1);
                    return;
                }
            }
        });
    race.add(
        1,
        [&](size_t)
        {
            for (size_t i = 0; i < 2; ++i)
            {
                tokens.fetch_add(1);
                futexWakeOne(tokens);
            }
        });
    race.run();

    EXPECT_EQ(consumed.load(), 2u);
    EXPECT_EQ(tokens.load(), 0u);
}
