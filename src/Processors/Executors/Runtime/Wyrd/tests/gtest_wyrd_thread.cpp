#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/WaitGroup.h>

#include <array>

using namespace DB::Runtime::Wyrd;

WYRD_TEST(Wyrd, ThreadJoinWaitsForBody)
{
    Atomic<int> ran{0};
    Thread thread([&] { ran.store(1); });
    thread.join();
    EXPECT_EQ(ran.load(), 1);
}

WYRD_TEST(Wyrd, ThreadWaitGroupGivesEveryIndexOnce)
{
    std::array<Atomic<int>, 4> seen{};

    WaitGroup group;
    group.add(4, [&](size_t index) { ++seen[index]; });
    group.join();

    for (const auto & slot : seen)
        EXPECT_EQ(slot.load(), 1);
}
