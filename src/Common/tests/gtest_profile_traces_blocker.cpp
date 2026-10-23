#include "config.h"

#include <gtest/gtest.h>

#include <Common/CoroutineStack.h>
#include <Common/ProfileTracesBlocker.h>
#include <Common/StackfulCoroutine.h>
#include <base/scope_guard.h>

#include <array>
#include <optional>
#include <thread>

#if USE_SILK
#    include <Common/tests/gtest_silk_scheduler.h>
#    include <silk/fibers/fiber.h>
#    include <silk/fibers/future.h>
#endif

using DB::ProfileTracesBlocker;
using DB::ProfileTracesStreamBlocker;

TEST(ProfileTracesBlocker, StreamScopePreservesMemorySettings)
{
    std::thread([]
    {
        DB::ThreadStatus thread;
        thread.setMemorySampleConfig({0.5, 7, 100});
        const auto level = MemoryTrackerBlockerInThread::getLevel();
        EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
        {
            ProfileTracesStreamBlocker stream_blocker;
            EXPECT_TRUE(ProfileTracesBlocker::isBlocked());
            EXPECT_EQ(MemoryTrackerBlockerInThread::getLevel(), level);
            EXPECT_EQ(thread.getEffectiveSampleProbability(10), 0.5);
            {
                ProfileTracesBlocker full_blocker;
                EXPECT_TRUE(ProfileTracesBlocker::isBlocked());
                EXPECT_EQ(thread.getEffectiveSampleProbability(10), 0);
            }
            EXPECT_TRUE(ProfileTracesBlocker::isBlocked());
            EXPECT_EQ(thread.getEffectiveSampleProbability(10), 0.5);
        }
        EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
        EXPECT_EQ(MemoryTrackerBlockerInThread::getLevel(), level);
        EXPECT_EQ(thread.getEffectiveSampleProbability(10), 0.5);
    }).join();
}

TEST(ProfileTracesBlocker, CoroutineSuspensionAndThreadMigration)
{
    size_t resumes = 0;
    StackfulCoroutine coroutine(CoroutineStack{}, [&](auto suspend)
    {
        EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
        ProfileTracesStreamBlocker outer;
        ++resumes;
        suspend();
        EXPECT_TRUE(ProfileTracesBlocker::isBlocked());
        {
            ProfileTracesStreamBlocker inner;
            ++resumes;
            suspend();
            EXPECT_TRUE(ProfileTracesBlocker::isBlocked());
        }
        EXPECT_TRUE(ProfileTracesBlocker::isBlocked());
        ++resumes;
    });

    coroutine.resume();
    EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
    EXPECT_EQ(resumes, 1);
    {
        ProfileTracesStreamBlocker caller;
        coroutine.resume();
        EXPECT_TRUE(ProfileTracesBlocker::isBlocked());
    }
    EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
    EXPECT_EQ(resumes, 2);

    std::thread([&]
    {
        DB::ThreadStatus thread;
        thread.setMemorySampleConfig({0.25, 0, 0});
        EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
        coroutine.resume();
        EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
        EXPECT_EQ(thread.getEffectiveSampleProbability(10), 0.25);
    }).join();
    EXPECT_EQ(resumes, 3);
    EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
}

TEST(ProfileTracesBlocker, SuspendedCoroutineUnwindsOnAnotherThread)
{
    bool unwound = false;
    std::optional<StackfulCoroutine> coroutine;
    coroutine.emplace(CoroutineStack{}, [&](auto suspend)
    {
        ProfileTracesStreamBlocker outer;
        ProfileTracesStreamBlocker inner;
        SCOPE_EXIT({
            EXPECT_TRUE(ProfileTracesBlocker::isBlocked());
            unwound = true;
        });
        suspend();
        FAIL() << "A cancelled coroutine must unwind without continuing its task";
    });
    coroutine->resume();
    EXPECT_FALSE(ProfileTracesBlocker::isBlocked());

    std::thread([&]
    {
        {
            ProfileTracesStreamBlocker caller;
            coroutine.reset();
            EXPECT_TRUE(ProfileTracesBlocker::isBlocked());
        }
        EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
    }).join();
    EXPECT_TRUE(unwound);
    EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
}

#if USE_SILK
namespace
{
class ProfileTracesBlockerFiberTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        initializeFiberSchedulerForTests();
    }
};
}

TEST_F(ProfileTracesBlockerFiberTest, ConcurrentFibersKeepTheirOwnSuppression)
{
    std::array<silk::FiberFuture, 8> futures;
    for (size_t index = 0; index < futures.size(); ++index)
    {
        ASSERT_EQ(Silk::spawn([blocked = index % 2 == 0]() -> int
        {
            EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
            std::optional<ProfileTracesStreamBlocker> blocker;
            if (blocked)
                blocker.emplace();
            for (size_t iteration = 0; iteration < 16; ++iteration)
            {
                silk::FiberScheduler::yield();
                EXPECT_EQ(ProfileTracesBlocker::isBlocked(), blocked);
            }
            return 0;
        }, futures[index]), 0);
    }
    for (auto & future : futures)
        EXPECT_EQ(future.wait(), 0);
    EXPECT_FALSE(ProfileTracesBlocker::isBlocked());
}
#endif
