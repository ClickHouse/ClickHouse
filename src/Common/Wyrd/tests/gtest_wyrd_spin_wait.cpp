#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/SpinWait.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Race.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <base/types.h>

using namespace DB::Runtime::Wyrd;

WYRD_TEST(Wyrd, SpinWaitBetweenFailedExchanges)
{
    Atomic<UInt64> counter{0};

    Race race;
    race.add(
        3,
        [&](size_t)
        {
            for (size_t i = 0; i < 50; ++i)
            {
                SpinWait spin_wait;
                UInt64 value = counter.load();
                while (!counter.compare_exchange_weak(value, value + 1))
                    spin_wait.spin();
            }
        });
    race.run();

    EXPECT_EQ(counter.load(), 150u);
}

WYRD_TEST(Wyrd, SpinWaitRoundsStayBounded, .runs = 1)
{
    SpinWait spin_wait;
    for (size_t i = 0; i < 20; ++i)
        spin_wait.spin();
}
