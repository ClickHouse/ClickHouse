#if defined(WYRD_ADVERSARY)

#include <Common/Wyrd/Strategies/Adversary/Access.h>
#include <Common/Wyrd/Strategies/Adversary/InjectFault.h>
#include <Common/Wyrd/Facade/Atomic.h>
#include <Common/Wyrd/Facade/Mutex.h>
#include <Common/Wyrd/Facade/Preemption.h>
#include <Common/Wyrd/Testing/LockFreeScope.h>
#include <Common/Wyrd/Testing/Test.h>
#include <Common/Wyrd/Testing/Race.h>
#include <base/types.h>

#if defined(OS_LINUX)
#    include <sched.h>
#endif

using namespace DB::Wyrd;

WYRD_TEST(Wyrd, AdversaryYieldsAtEveryFaultPoint, .runs = 1, .adversary = {.pin_to_single_core = false, .yield_probability = 1.0, .suspend_probability = 0.0})
{
    Atomic<size_t> counter{0};
    for (size_t i = 0; i < 100; ++i)
        counter.fetch_add(1);
    EXPECT_EQ(counter.load(), 100u);

    EXPECT_GE(adversary()->report().yields, 100u);
}

WYRD_TEST(Wyrd, AdversaryYieldsAtEveryPreemptionPoint, .runs = 1, .adversary = {.pin_to_single_core = false, .yield_probability = 1.0, .suspend_probability = 0.0})
{
    for (size_t i = 0; i < 100; ++i)
        preemptionPoint();

    EXPECT_EQ(adversary()->report().yields, 100u);
}

WYRD_TEST(Wyrd, AdversarySpuriousFailureEveryTime, .runs = 1, .adversary = {.pin_to_single_core = false, .spurious_failure_probability = 1.0, .suspend_probability = 0.0})
{
    Atomic<int> value{0};
    int expected = 0;
    EXPECT_FALSE(value.compare_exchange_weak(expected, 1));
    EXPECT_EQ(expected, 0);
    EXPECT_TRUE(value.compare_exchange_strong(expected, 1));

    Mutex mutex;
    EXPECT_FALSE(mutex.try_lock());

    EXPECT_EQ(adversary()->report().spurious_failures, 2u);
}

WYRD_TEST(
    Wyrd,
    AdversarySuspendsInsideLockFreeScope,
    .adversary = {.pin_to_single_core = false, .yield_probability = 0.5, .suspend_probability = 1.0, .max_suspended_threads = 1, .resume_probability = 1.0})
{
    Atomic<size_t> counter{0};
    Atomic<UInt32> arrived{0};

    Race race;
    race.add(
        3,
        [&](size_t)
        {
            arrived.fetch_add(1);
            arrived.notify_all();
            for (UInt32 seen = arrived.load(); seen < 3; seen = arrived.load())
                arrived.wait(seen);

            LockFreeScope scope;
            for (size_t i = 0; i < 200; ++i)
            {
                counter.fetch_add(1);
                progress();
            }
        });
    race.run();

    EXPECT_EQ(counter.load(), 600u);
    EXPECT_GT(adversary()->report().suspends, 0u);
}

TEST(Wyrd, AdversaryNothingInstalledMeansNoFaults)
{
    EXPECT_EQ(adversary(), nullptr);

    Atomic<int> value{0};
    int expected = 0;
    EXPECT_TRUE(value.compare_exchange_weak(expected, 1));
    EXPECT_FALSE(spuriousFailure());
}

#if defined(OS_LINUX)

TEST(Wyrd, AdversaryThreadExitRestoresAffinity)
{
    cpu_set_t before;
    CPU_ZERO(&before);
    ASSERT_EQ(sched_getaffinity(0, sizeof(before), &before), 0);

    Adversary pinning(AdversaryParams{.pin_to_single_core = true});
    pinning.threadEnter();
    pinning.threadExit();

    cpu_set_t after;
    CPU_ZERO(&after);
    ASSERT_EQ(sched_getaffinity(0, sizeof(after), &after), 0);
    EXPECT_TRUE(CPU_EQUAL(&before, &after));
}

#endif

#endif
