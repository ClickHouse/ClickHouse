#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/WaitGroup.h>

#include <mutex>

using namespace DB::Runtime::Wyrd;

WYRD_TEST(Wyrd, MutexProtectsPlainCounter)
{
    Mutex mutex;
    size_t counter = 0;

    WaitGroup group;
    group.add(
        3,
        [&](size_t)
        {
            for (size_t i = 0; i < 100; ++i)
            {
                std::lock_guard lock(mutex);
                ++counter;
            }
        });
    group.join();

    EXPECT_EQ(counter, 300u);
}

WYRD_TEST(Wyrd, MutexUniqueLockHandsOver)
{
    Mutex mutex;
    size_t owner_changes = 0;
    size_t last_owner = 0;

    WaitGroup group;
    group.add(
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
    group.join();

    EXPECT_GE(owner_changes, 2u);
}
