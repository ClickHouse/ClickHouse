#include "config.h"

#if USE_NURAFT

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <thread>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperDispatcher.h>
#include <Common/Stopwatch.h>

/// Cases that name the helper, or that reach the private interruptibleSleep, are compiled only
/// where its header exists, so this file also builds against a tree without it.
/// WaitCommittedUptoKeepsHugeTimeout below uses only the public KeeperContext API.
#if __has_include(<Common/saturatedWaitDuration.h>)
#include <Common/saturatedWaitDuration.h>
#define SATURATED_WAIT_DURATION_AVAILABLE 1
#else
#define SATURATED_WAIT_DURATION_AVAILABLE 0
#endif

using namespace DB;

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsMilliseconds container_gc_period_ms;
    extern const CoordinationSettingsMilliseconds dead_session_check_period_ms;
    extern const CoordinationSettingsMilliseconds operation_timeout_ms;
    extern const CoordinationSettingsMilliseconds session_shutdown_timeout;
    extern const CoordinationSettingsMilliseconds session_timeout_ms;
    extern const CoordinationSettingsMilliseconds startup_timeout;
    extern const CoordinationSettingsMilliseconds stream_in_flight_drain_timeout_ms;
    extern const CoordinationSettingsMilliseconds stream_suspect_retry_delay_ms;
    extern const CoordinationSettingsMilliseconds ttl_gc_period_ms;
}

namespace
{

/// Millisecond counts that a wait must survive. The first is representable as nanoseconds but
/// leaves less than a millisecond below Int64::max, so `steady_clock::now() + duration` wraps; the
/// next two overflow the milliseconds -> nanoseconds product itself.
const std::vector<Int64> huge_timeouts_ms = {
    9'223'372'036'854LL,
    9'223'372'036'855LL,
    9'223'372'036'854'775LL,
    std::numeric_limits<Int64>::max(),
};

/// waitCommittedUpto takes the count as an unsigned parameter, so a negative count read as unsigned
/// must not become a huge wait either.
std::vector<UInt64> hugeUnsignedTimeoutsMs()
{
    std::vector<UInt64> result;
    for (Int64 ms : huge_timeouts_ms)
        result.push_back(static_cast<UInt64>(ms));
    result.push_back(std::numeric_limits<UInt64>::max());
    return result;
}

/// The predicate is satisfied after this long, so a wait that kept its (saturated, ~146 year)
/// duration returns true, while a wait whose duration was lost to overflow returns false at once.
constexpr Int64 notify_after_ms = 300;

#if SATURATED_WAIT_DURATION_AVAILABLE
/// For the opposite direction, where the wait must not happen at all, the predicate is satisfied
/// sooner: the ordering oracle can only see a spurious wait that lasts at least this long, so the
/// delay has to stay below the shortest regression worth catching rather than above it.
constexpr Int64 signal_after_ms = 100;
#endif

}

class KeeperSaturatedWaitTest : public ::testing::Test
{
};

/// Site 2. waitCommittedUpto takes the timeout as a parameter, so the saturation belongs to this
/// callee rather than to its caller.
///
/// The oracle is behavioural and does not need a sanitizer: with an unsaturated count the deadline
/// wraps to the past, so the wait gives up immediately and reports failure even though the operator
/// asked for a very long timeout. That lost timeout is the user-visible misbehaviour; the overflow
/// on the way there is the undefined behaviour a sanitizer build additionally reports.
TEST_F(KeeperSaturatedWaitTest, WaitCommittedUptoKeepsHugeTimeout)
{
    for (UInt64 timeout_ms : hugeUnsignedTimeoutsMs())
    {
        SCOPED_TRACE(timeout_ms);

        auto keeper_context = std::make_shared<KeeperContext>(true, std::make_shared<CoordinationSettings>());
        keeper_context->setLastCommitIndex(1);

        std::thread committer(
            [&]
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(notify_after_ms));
                keeper_context->setLastCommitIndex(10);
            });

        Stopwatch watch;
        const bool committed = keeper_context->waitCommittedUpto(10, timeout_ms);
        const auto elapsed_ms = watch.elapsedMilliseconds();
        committer.join();

        EXPECT_TRUE(committed);
        EXPECT_GE(elapsed_ms, static_cast<UInt64>(notify_after_ms) / 2);
    }
}

#if SATURATED_WAIT_DURATION_AVAILABLE

/// Sites 5 to 7. All three callers of interruptibleSleep build the period from a coordination
/// setting, so one saturation in the callee covers every one of them.
/// The period arrives already typed as std::chrono::milliseconds, whose representation is signed,
/// and all three callers build it from a signed totalMilliseconds(), so the reachable extremes here
/// are the signed ones. An unsigned count would already have wrapped at the call site.
TEST_F(KeeperSaturatedWaitTest, InterruptibleSleepKeepsHugePeriod)
{
    for (Int64 period_ms : huge_timeouts_ms)
    {
        SCOPED_TRACE(period_ms);

        KeeperDispatcher dispatcher;

        std::thread shutdown_signaller(
            [&]
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(notify_after_ms));
                dispatcher.signalShutdown();
            });

        Stopwatch watch;
        dispatcher.interruptibleSleep(std::chrono::milliseconds(period_ms));
        const auto elapsed_ms = watch.elapsedMilliseconds();
        /// Read where the wait returned, not after joining the signaller: the signaller sets the
        /// flag unconditionally, so a reading taken after the join would be true whatever ended the
        /// wait, and would assert nothing.
        const bool shutdown_was_signalled_when_the_wait_returned = dispatcher.isShuttingDown();
        shutdown_signaller.join();

        /// Without saturation the wrapped deadline is already in the past, so this returns at once
        /// instead of sleeping until shutdown is signalled.
        EXPECT_GE(elapsed_ms, static_cast<UInt64>(notify_after_ms) / 2);
        /// The elapsed bound alone would also accept a period silently shortened to anything from
        /// 150 ms upwards, which times out instead of keeping the requested period. This pins why
        /// the wait ended: the predicate became true, so the saturated period was really kept.
        EXPECT_TRUE(shutdown_was_signalled_when_the_wait_returned);
    }
}

/// A non-positive period still returns immediately: saturation must not turn "already expired" into
/// a wait, otherwise shutdown paths that pass a wrapped negative count would hang.
TEST_F(KeeperSaturatedWaitTest, InterruptibleSleepReturnsAtOnceForNonPositivePeriod)
{
    for (Int64 period_ms : {Int64{0}, Int64{-1}, Int64{-9'223'372'036'854'775}})
    {
        SCOPED_TRACE(period_ms);

        /// A fresh dispatcher per period: the flag latches once signalled, so a shared one would
        /// already be shutting down for every iteration after the first and the oracle below would
        /// read true without any wait having happened.
        KeeperDispatcher dispatcher;

        std::thread shutdown_signaller(
            [&]
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(signal_after_ms));
                dispatcher.signalShutdown();
            });

        Stopwatch watch;
        dispatcher.interruptibleSleep(std::chrono::milliseconds(period_ms));
        const auto elapsed_ms = watch.elapsedMilliseconds();
        /// Sampled before the join for the same reason as above: the signaller sets the flag
        /// unconditionally, so a reading taken afterwards is true whatever ended the wait.
        const bool shutdown_was_signalled_when_the_wait_returned = dispatcher.isShuttingDown();
        shutdown_signaller.join();

        /// An elapsed bound alone accepts any wait shorter than the signal delay, so it cannot say
        /// the wait did not happen. This pins the ordering: the call returned while the predicate
        /// was still false, so it did not wait at all.
        EXPECT_FALSE(shutdown_was_signalled_when_the_wait_returned);
        EXPECT_LT(elapsed_ms, static_cast<UInt64>(notify_after_ms));
    }
}

/// The bound is a representability bound, not a policy cap: every coordination default, and a value
/// as large as a year, passes through untouched.
TEST_F(KeeperSaturatedWaitTest, CoordinationDefaultsAreUnaffected)
{
    CoordinationSettings settings;
    std::vector<Int64> values{
        settings[CoordinationSetting::operation_timeout_ms].totalMilliseconds(),
        settings[CoordinationSetting::session_timeout_ms].totalMilliseconds(),
        settings[CoordinationSetting::startup_timeout].totalMilliseconds(),
        settings[CoordinationSetting::session_shutdown_timeout].totalMilliseconds(),
        settings[CoordinationSetting::dead_session_check_period_ms].totalMilliseconds(),
        settings[CoordinationSetting::ttl_gc_period_ms].totalMilliseconds(),
        settings[CoordinationSetting::container_gc_period_ms].totalMilliseconds(),
        settings[CoordinationSetting::stream_suspect_retry_delay_ms].totalMilliseconds(),
        settings[CoordinationSetting::stream_in_flight_drain_timeout_ms].totalMilliseconds(),
        365LL * 24 * 3600 * 1000,
    };

    for (Int64 ms : values)
    {
        SCOPED_TRACE(ms);
        EXPECT_GT(ms, 0);
        EXPECT_EQ(saturatedWaitMilliseconds(ms).count(), ms);
        EXPECT_EQ(saturatedWaitMillisecondsCountNonZero(ms), static_cast<UInt64>(ms));
    }
}

#endif

#endif
