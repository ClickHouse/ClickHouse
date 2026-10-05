#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Race.h>

#include <mutex>

using namespace DB::Runtime::Wyrd;

WYRD_TEST(Wyrd, MutexProtectsPlainCounter)
{
    Mutex mutex;
    size_t counter = 0;

    Race race;
    race.add(
        3,
        [&](size_t)
        {
            for (size_t i = 0; i < 100; ++i)
            {
                std::lock_guard lock(mutex);
                ++counter;
            }
        });
    race.run();

    EXPECT_EQ(counter, 300u);
}

WYRD_TEST(Wyrd, MutexUniqueLockHandsOver)
{
    Mutex mutex;
    size_t owner_changes = 0;
    size_t last_owner = 0;

    Race race;
    race.add(
        2,
        [&](size_t index)
        {
            for (size_t i = 0; i < 50; ++i)
            {
                std::unique_lock lock(mutex);
                if (last_owner != index + 1)
                    ++owner_changes;
                last_owner = index + 1;
            }
        });
    race.run();

    EXPECT_GE(owner_changes, 2u);
}

WYRD_TEST(Wyrd, MutexTryLockNeverEntersAHeldSection)
{
    Mutex mutex;
    Atomic<size_t> inside{0};
    Atomic<size_t> acquired{0};

    Race race;
    race.add(
        3,
        [&](size_t)
        {
            for (size_t i = 0; i < 50; ++i)
            {
                std::unique_lock lock(mutex, std::try_to_lock);
                if (!lock.owns_lock())
                    continue;
                EXPECT_EQ(inside.fetch_add(1), 0u);
                inside.fetch_sub(1);
                acquired.fetch_add(1);
            }
        });
    race.run();

    EXPECT_GT(acquired.load(), 0u);
}
