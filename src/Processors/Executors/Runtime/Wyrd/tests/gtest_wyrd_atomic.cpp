#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Race.h>
#include <base/types.h>

using namespace DB::Runtime::Wyrd;

WYRD_TEST(Wyrd, AtomicIntegerOperations)
{
    Atomic<UInt64> counter{5};
    EXPECT_EQ(counter.load(), 5u);

    counter.store(7);
    EXPECT_EQ(counter.load(), 7u);
    EXPECT_EQ(counter.exchange(1), 7u);
    EXPECT_EQ(counter.fetch_add(2), 1u);
    EXPECT_EQ(counter.fetch_sub(1), 3u);
    EXPECT_EQ(counter.load(), 2u);

    UInt64 expected = 0;
    EXPECT_FALSE(counter.compare_exchange_strong(expected, 9));
    EXPECT_EQ(expected, 2u);
    EXPECT_TRUE(counter.compare_exchange_strong(expected, 9));
    EXPECT_EQ(counter.load(), 9u);

    while (!counter.compare_exchange_weak(expected, 10))
    {
    }
    EXPECT_EQ(counter.load(), 10u);
}

WYRD_TEST(Wyrd, AtomicWaitAndNotify)
{
    Atomic<UInt32> stage{0};
    Atomic<size_t> woken{0};

    Race race;
    race.add(
        2,
        [&](size_t)
        {
            stage.wait(0);
            woken.fetch_add(1);
        });
    race.add(
        1,
        [&](size_t)
        {
            stage.store(1);
            stage.notify_all();
        });
    race.run();

    EXPECT_EQ(woken.load(), 2u);

    Race pair;
    pair.add(
        1,
        [&](size_t)
        {
            stage.wait(1);
            EXPECT_EQ(stage.load(), 2u);
        });
    pair.add(
        1,
        [&](size_t)
        {
            stage.store(2);
            stage.notify_one();
        });
    pair.run();
}

WYRD_TEST(Wyrd, AtomicEnumAndPointer)
{
    enum class State
    {
        Idle,
        Busy,
    };

    Atomic<State> state{State::Idle};
    State expected = State::Busy;
    EXPECT_FALSE(state.compare_exchange_strong(expected, State::Busy));
    EXPECT_EQ(expected, State::Idle);
    EXPECT_TRUE(state.compare_exchange_strong(expected, State::Busy));
    EXPECT_EQ(state.load(), State::Busy);

    int first = 1;
    int second = 2;
    Atomic<int *> pointer{&first};
    EXPECT_EQ(pointer.exchange(&second), &first);
    EXPECT_EQ(*pointer.load(), 2);
}
