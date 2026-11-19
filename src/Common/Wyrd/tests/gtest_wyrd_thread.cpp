#include <Common/Wyrd/Facade/Atomic.h>
#include <Common/Wyrd/Facade/Thread.h>
#include <Common/Wyrd/Testing/Test.h>
#include <Common/Wyrd/Testing/Race.h>

#include <array>

using namespace DB::Wyrd;

WYRD_TEST(Wyrd, ThreadJoinWaitsForBody)
{
    Atomic<int> ran{0};
    Thread thread([&] { ran.store(1); });
    thread.join();
    EXPECT_EQ(ran.load(), 1);
}

WYRD_TEST(Wyrd, ThreadRaceGivesEveryIndexOnce)
{
    std::array<Atomic<int>, 4> seen{};

    Race race;
    race.add(4, [&](size_t index) { seen[index].fetch_add(1); });
    race.run();

    for (const auto & slot : seen)
        EXPECT_EQ(slot.load(), 1);
}
