#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <thread>

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Stopwatch.h>

/// Cases that name the helper are compiled only where its header exists, so this file also builds
/// against a tree without it. The queue cases below use only the public queue API.
#if __has_include(<Common/saturatedWaitDuration.h>)
#include <Common/saturatedWaitDuration.h>
#define SATURATED_WAIT_DURATION_AVAILABLE 1
#else
#define SATURATED_WAIT_DURATION_AVAILABLE 0
#endif

using namespace DB;

#if SATURATED_WAIT_DURATION_AVAILABLE

namespace
{

/// The bound must leave room for steady_clock::now() on top of the converted duration, so the
/// nanosecond product may not exceed half of the Int64 range. Computed in __int128 so this check
/// cannot overflow itself.
void expectNanosecondsAndAdditionSafe(std::chrono::milliseconds d)
{
    EXPECT_GE(d.count(), 0);
    EXPECT_LE(d.count(), MAX_WAIT_MILLISECONDS);

    const __int128 ns = static_cast<__int128>(d.count()) * 1'000'000;
    EXPECT_LE(ns, static_cast<__int128>(std::numeric_limits<Int64>::max() / 2));
}

}

TEST(SaturatedWaitDuration, BoundIsHalfNanosecondRange)
{
    EXPECT_EQ(MAX_WAIT_MILLISECONDS, (std::numeric_limits<Int64>::max() / 2) / 1'000'000);
    /// Sanity: the bound is far above anything an operator can mean, so it is a representability
    /// bound and not a policy cap (about 146 years).
    EXPECT_GT(MAX_WAIT_MILLISECONDS, 100LL * 365 * 24 * 3600 * 1000);
}

TEST(SaturatedWaitDuration, PreservesValuesInRange)
{
    for (Int64 ms : {Int64{0}, Int64{1}, Int64{180'000}, MAX_WAIT_MILLISECONDS})
    {
        SCOPED_TRACE(ms);
        EXPECT_EQ(saturatedWaitMilliseconds(ms).count(), ms);
        expectNanosecondsAndAdditionSafe(saturatedWaitMilliseconds(ms));
    }
}

TEST(SaturatedWaitDuration, SaturatesAboveBound)
{
    /// 9'223'372'036'854 fits the milliseconds -> nanoseconds product but leaves only 0.776 ms
    /// below Int64::max, so steady_clock::now() + duration still wraps. It must be clamped.
    for (Int64 ms : {MAX_WAIT_MILLISECONDS + 1,
                     Int64{9'223'372'036'854},
                     Int64{9'223'372'036'855},
                     Int64{9'223'372'036'854'775},
                     std::numeric_limits<Int64>::max()})
    {
        SCOPED_TRACE(ms);
        EXPECT_EQ(saturatedWaitMilliseconds(ms).count(), MAX_WAIT_MILLISECONDS);
        expectNanosecondsAndAdditionSafe(saturatedWaitMilliseconds(ms));
    }
}

TEST(SaturatedWaitDuration, ClampsNegativeToZero)
{
    /// A negative count must not become a huge wait once read as unsigned.
    for (Int64 ms : {Int64{-1}, Int64{-9'223'372'036'854'775}, std::numeric_limits<Int64>::min()})
    {
        SCOPED_TRACE(ms);
        EXPECT_EQ(saturatedWaitMilliseconds(ms).count(), 0);
        expectNanosecondsAndAdditionSafe(saturatedWaitMilliseconds(ms));
    }
}

TEST(SaturatedWaitDuration, IsSignednessAgnostic)
{
    /// Several call sites hold the count in an unsigned type, so a negative value arrives as a
    /// huge positive one. A plain `>` comparison against a signed bound would let it through.
    EXPECT_EQ(saturatedWaitMilliseconds(std::numeric_limits<uint64_t>::max()).count(), MAX_WAIT_MILLISECONDS);
    EXPECT_EQ(saturatedWaitMilliseconds(static_cast<uint64_t>(MAX_WAIT_MILLISECONDS) + 1).count(), MAX_WAIT_MILLISECONDS);
    EXPECT_EQ(saturatedWaitMilliseconds(static_cast<uint64_t>(MAX_WAIT_MILLISECONDS)).count(), MAX_WAIT_MILLISECONDS);
    EXPECT_EQ(saturatedWaitMilliseconds(uint64_t{180'000}).count(), 180'000);
    EXPECT_EQ(saturatedWaitMilliseconds(uint32_t{0}).count(), 0);
    expectNanosecondsAndAdditionSafe(saturatedWaitMilliseconds(std::numeric_limits<uint64_t>::max()));
}

TEST(SaturatedWaitDuration, NonZeroCountKeepsZeroPolicy)
{
    /// NuRaft arms its send/receive timers only for a nonzero count, so this conversion may never
    /// turn a nonzero input into zero. It converts to unsigned first, exactly like the uint64_t
    /// parameter it feeds, and caps only the upper end.
    EXPECT_EQ(saturatedWaitMillisecondsCountNonZero(Int64{0}), 0u);
    EXPECT_EQ(saturatedWaitMillisecondsCountNonZero(Int64{1}), 1u);
    EXPECT_EQ(saturatedWaitMillisecondsCountNonZero(Int64{180'000}), 180'000u);
    EXPECT_EQ(saturatedWaitMillisecondsCountNonZero(MAX_WAIT_MILLISECONDS), static_cast<UInt64>(MAX_WAIT_MILLISECONDS));
    EXPECT_EQ(saturatedWaitMillisecondsCountNonZero(MAX_WAIT_MILLISECONDS + 1), static_cast<UInt64>(MAX_WAIT_MILLISECONDS));
    EXPECT_EQ(saturatedWaitMillisecondsCountNonZero(std::numeric_limits<uint64_t>::max()), static_cast<UInt64>(MAX_WAIT_MILLISECONDS));

    /// The rows that would catch a regression to the wait helper's negative-to-zero rule. The value
    /// must be the bound exactly: a merely nonzero result such as 1 would satisfy "not disabled"
    /// and still make the timer fire at once, which is the mirror of the hazard guarded here.
    EXPECT_EQ(saturatedWaitMillisecondsCountNonZero(Int64{-1}), static_cast<UInt64>(MAX_WAIT_MILLISECONDS));
    EXPECT_EQ(saturatedWaitMillisecondsCountNonZero(Int64{-9'223'372'036'854'775}), static_cast<UInt64>(MAX_WAIT_MILLISECONDS));
}

TEST(SaturatedWaitDuration, MicrosecondsGuardKeepsPolicyMagnitude)
{
    /// The session timeout is policy, not a wait: it is replicated, stored as the session TTL and
    /// echoed to the client. Only the multiplication is made total.
    constexpr Int64 max_ms = std::numeric_limits<Int64>::max() / 1000;
    EXPECT_EQ(saturatedMicrosecondsFromMilliseconds(Int64{30'000}), 30'000'000);
    EXPECT_EQ(saturatedMicrosecondsFromMilliseconds(Int64{0}), 0);
    EXPECT_EQ(saturatedMicrosecondsFromMilliseconds(max_ms), max_ms * 1000);
    EXPECT_EQ(saturatedMicrosecondsFromMilliseconds(max_ms + 1), std::numeric_limits<Int64>::max());
    EXPECT_EQ(saturatedMicrosecondsFromMilliseconds(std::numeric_limits<Int64>::max()), std::numeric_limits<Int64>::max());
    EXPECT_EQ(saturatedMicrosecondsFromMilliseconds(std::numeric_limits<uint64_t>::max()), std::numeric_limits<Int64>::max());
    EXPECT_EQ(saturatedMicrosecondsFromMilliseconds(Int64{-30'000}), -30'000'000);
    EXPECT_EQ(saturatedMicrosecondsFromMilliseconds(std::numeric_limits<Int64>::min()), std::numeric_limits<Int64>::min());

    /// It must NOT reduce a large but legitimate timeout to the wait bound: a one year session
    /// timeout has to stay a one year session timeout.
    constexpr Int64 one_year_ms = 365LL * 24 * 3600 * 1000;
    EXPECT_EQ(saturatedMicrosecondsFromMilliseconds(one_year_ms), one_year_ms * 1000);
    EXPECT_GT(saturatedMicrosecondsFromMilliseconds(std::numeric_limits<Int64>::max()) / 1000, MAX_WAIT_MILLISECONDS);
}

#endif

TEST(SaturatedWaitDuration, ConcurrentBoundedQueuePopKeepsHugeTimeout)
{
    /// popImpl converts the caller's count with the predicate overload of wait_for, so an
    /// unsaturated huge count wraps the deadline into the past: the pop gives up at once and
    /// reports failure although the caller asked to wait a very long time. Satisfying the
    /// predicate a little later separates the two behaviours without waiting out the real timeout.
    for (uint64_t timeout_ms : {uint64_t{9'223'372'036'854},
                                uint64_t{9'223'372'036'855},
                                uint64_t{9'223'372'036'854'775},
                                std::numeric_limits<uint64_t>::max()})
    {
        SCOPED_TRACE(timeout_ms);
        ConcurrentBoundedQueue<int> queue(1);

        std::thread producer(
            [&]
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                ASSERT_TRUE(queue.tryPush(42, 1000));
            });

        int x = 0;
        Stopwatch watch;
        const bool popped = queue.tryPop(x, timeout_ms);
        const auto elapsed_ms = watch.elapsedMilliseconds();
        producer.join();

        EXPECT_TRUE(popped);
        EXPECT_EQ(x, 42);
        EXPECT_GE(elapsed_ms, 150u);
    }
}

TEST(SaturatedWaitDuration, ConcurrentBoundedQueuePushKeepsHugeTimeout)
{
    /// Same for emplaceImpl on a full queue. Both *Impl functions convert, so both need a case or
    /// one of the two edits is untested.
    for (uint64_t timeout_ms : {uint64_t{9'223'372'036'854}, std::numeric_limits<uint64_t>::max()})
    {
        SCOPED_TRACE(timeout_ms);
        ConcurrentBoundedQueue<int> queue(1);
        ASSERT_TRUE(queue.tryPush(1, 1000));

        std::thread consumer(
            [&]
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                int taken = 0;
                ASSERT_TRUE(queue.tryPop(taken, 1000));
            });

        Stopwatch watch;
        const bool pushed = queue.tryPush(2, timeout_ms);
        const auto elapsed_ms = watch.elapsedMilliseconds();
        consumer.join();

        EXPECT_TRUE(pushed);
        EXPECT_GE(elapsed_ms, 150u);
    }
}

TEST(SaturatedWaitDuration, ConcurrentBoundedQueueZeroTimeoutStillReturnsAtOnce)
{
    /// Saturation must not turn "do not wait" into a wait. An empty queue reports failure however
    /// long the wait lasted, and an elapsed bound accepts any wait shorter than the bound, so
    /// neither can say the wait did not happen. A producer that pushes a little later makes it an
    /// ordering fact instead: the item must still be in the queue afterwards, which is only true if
    /// the pop returned before the push landed. The delay stays below the shortest spurious wait
    /// worth catching, because a wait shorter than it would go unnoticed.
    constexpr UInt64 push_after_ms = 100;

    ConcurrentBoundedQueue<int> queue(1);

    std::thread producer(
        [&]
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(push_after_ms));
            ASSERT_TRUE(queue.tryPush(42, 1000));
        });

    int x = 0;
    Stopwatch watch;
    const bool popped = queue.tryPop(x, 0);
    const auto elapsed_ms = watch.elapsedMilliseconds();
    producer.join();

    EXPECT_FALSE(popped);
    EXPECT_LT(elapsed_ms, 300u);

    int still_queued = 0;
    EXPECT_TRUE(queue.tryPop(still_queued, 0));
    EXPECT_EQ(still_queued, 42);
}
