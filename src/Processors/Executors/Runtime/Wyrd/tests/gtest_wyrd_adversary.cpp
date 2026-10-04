#if defined(WYRD_ADVERSARY)

#include <Processors/Executors/Runtime/Wyrd/Adversary/Access.h>
#include <Processors/Executors/Runtime/Wyrd/Adversary/InjectFault.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/LockFree.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Run.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/WaitGroup.h>
#include <base/types.h>

#include <gtest/gtest.h>

using namespace DB::Runtime::Wyrd;

namespace
{

struct AdversaryGuard
{
    explicit AdversaryGuard(AdversaryParams params) { setAdversary(params); }
    ~AdversaryGuard() { resetAdversary(); }
};

}

TEST(Wyrd, AdversaryYieldsAtEveryFaultPoint)
{
    AdversaryGuard guard({.yield_freq = 1, .suspend_freq = 0, .pin_to_single_core = false});

    runTest(
        []
        {
            Atomic<size_t> counter{0};
            for (size_t i = 0; i < 100; ++i)
                ++counter;
            EXPECT_EQ(counter.load(), 100u);
        });

    EXPECT_GE(adversary()->report().yields, 100u);
}

TEST(Wyrd, AdversarySpuriousFailureEveryTime)
{
    AdversaryGuard guard({.spurious_failure_every = 1, .suspend_freq = 0, .pin_to_single_core = false});

    runTest(
        []
        {
            Atomic<int> value{0};
            int expected = 0;
            EXPECT_FALSE(value.compare_exchange_weak(expected, 1));
            EXPECT_EQ(expected, 0);
            EXPECT_TRUE(value.compare_exchange_strong(expected, 1));
        });

    EXPECT_EQ(adversary()->report().spurious_failures, 1u);
}

TEST(Wyrd, AdversarySuspendsInsideLockFreeScope)
{
    AdversaryGuard guard({.yield_freq = 2, .suspend_freq = 1, .suspended_threads_limit = 1, .resume_freq = 1, .pin_to_single_core = false});

    runTest(
        []
        {
            Atomic<size_t> counter{0};
            Atomic<UInt32> arrived{0};

            WaitGroup group;
            group.add(
                3,
                [&](size_t)
                {
                    ++arrived;
                    arrived.notify_all();
                    for (UInt32 seen = arrived.load(); seen < 3; seen = arrived.load())
                        arrived.wait(seen);

                    LockFreeScope scope;
                    for (size_t i = 0; i < 200; ++i)
                    {
                        ++counter;
                        progress();
                    }
                });
            group.join();

            EXPECT_EQ(counter.load(), 600u);
        });

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

#endif
