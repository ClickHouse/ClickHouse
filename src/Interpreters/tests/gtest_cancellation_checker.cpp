#include <gtest/gtest.h>

#include <chrono>
#include <limits>
#include <thread>

#include <base/scope_guard.h>

#include <Common/Exception.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Settings.h>
#include <Interpreters/CancellationChecker.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/IAST.h>
#include <QueryPipeline/ExecutionSpeedLimits.h>
#include <QueryPipeline/SizeLimits.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{

QueryStatusPtr makeQueryStatus(const String & query_id)
{
    ClientInfo client_info;
    client_info.current_query_id = query_id;
    Settings settings;
    return std::make_shared<QueryStatus>(
        getContext().context,
        "SELECT 1",
        /*normalized_query_hash_*/ 0,
        client_info,
        /*priority_handle_*/ QueryPriorities::Handle{},
        /*query_slot_*/ nullptr,
        /*memory_reservation_*/ nullptr,
        /*thread_group_*/ nullptr,
        IAST::QueryKind::Select,
        settings,
        /*watch_start_nanoseconds*/ 0,
        /*is_internal*/ false);
}

}

/// The worker arms its wait for the earliest deadline present at wait entry. A deadline appended
/// later that is earlier than the armed one must re-arm the wait; before the fix the notification
/// was swallowed by the wake-up predicate and the new deadline fired only when the stale one expired.
TEST(CancellationChecker, RearmsWaitOnEarlierDeadline)
{
    auto & checker = CancellationChecker::getInstance();

    auto long_query = makeQueryStatus("gtest_cancellation_checker_long");
    auto short_query = makeQueryStatus("gtest_cancellation_checker_short");

    std::thread worker([&] { checker.workerFunction(); });
    /// Runs on every exit path (including early ASSERT returns, which would otherwise destroy a
    /// joinable thread and terminate); also drains leftover tasks from the singleton.
    SCOPE_EXIT({
        checker.appendDoneTasks(long_query);
        checker.appendDoneTasks(short_query);
        checker.terminateThread();
        worker.join();
    });

    /// Arm the worker's wait toward a deadline 10 minutes away and wait until the worker has
    /// actually parked on it, so the buggy interleaving is reproduced deterministically.
    ASSERT_TRUE(checker.appendTask(long_query, /*timeout_us=*/600'000'000, OverflowMode::THROW));
    for (int i = 0; i < 2000 && checker.getArmedDeadline() == 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    ASSERT_NE(checker.getArmedDeadline(), 0u);

    /// The long query finishes; removal intentionally does not notify the worker, so the stale
    /// 10-minute arm stays in place.
    checker.appendDoneTasks(long_query);

    /// A short deadline appended while the worker is armed for the (already removed) long one.
    ASSERT_TRUE(checker.appendTask(short_query, /*timeout_us=*/100'000, OverflowMode::THROW));

    /// 100 ms deadline + 100 ms cancellation grid; poll with a generous bound for sanitizer builds.
    bool killed = false;
    for (int i = 0; i < 200 && !killed; ++i)
    {
        killed = short_query->isKilled();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    EXPECT_TRUE(killed);
    EXPECT_FALSE(long_query->isKilled());
}

/// A task must never be cancelled before its timeout has elapsed: the query then fails with a
/// self-contradictory `Timeout exceeded: elapsed 999.672 ms, maximum: 1000 ms`, and everything that
/// measures the query against its own timeout (`query_duration_ms` in `system.query_log`) sees a
/// value below it. Deadlines are aligned up to a grid, which usually hides how the current time is
/// converted to milliseconds - but when the deadline already sits on the grid the alignment adds no
/// padding, and rounding the current time down then placed the deadline up to 1 ms in the past.
TEST(CancellationChecker, DeadlineIsNeverBeforeTheTimeout)
{
    /// Every sub-millisecond phase of `now`, against every phase of the grid the deadline can land on.
    for (const Int64 timeout_us : {1, 900, 1'000, 7'000, 50'000, 99'000, 100'000, 100'001, 999'000, 1'000'000, 1'000'900, 60'000'000})
    {
        for (Int64 now_ms = 0; now_ms < 1000; now_ms += 7)
        {
            /// Every microsecond phase, and inside it the nanosecond remainder that a conversion of
            /// `now` to microseconds can drop - including the last microsecond of a millisecond,
            /// which is where an exact deadline lands on a whole millisecond and the grid alignment
            /// adds no padding to hide the loss.
            for (Int64 sub_ms_us = 0; sub_ms_us < 1000; ++sub_ms_us)
            {
                for (const Int64 extra_ns : {0, 1, 500, 999})
                {
                    const Int64 sub_ms_ns = sub_ms_us * 1000 + extra_ns;
                    const Int64 now_ns = (1'000'000 + now_ms) * 1'000'000 + sub_ms_ns;
                    const auto now = std::chrono::steady_clock::time_point{
                        std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::nanoseconds{now_ns})};

                    const UInt64 deadline_ms = CancellationChecker::taskDeadlineMs(now, timeout_us);

                    ASSERT_GE(static_cast<Int64>(deadline_ms) * 1'000'000, now_ns + timeout_us * 1'000)
                        << "a task appended " << sub_ms_ns << " ns into a millisecond with a timeout of " << timeout_us
                        << " us is cancelled "
                        << (static_cast<double>(now_ns + timeout_us * 1'000 - static_cast<Int64>(deadline_ms) * 1'000'000) / 1e6)
                        << " ms early";
                }
            }
        }
    }
}

/// A `max_execution_time` above one year used to be silently truncated to exactly one year, so a
/// query with `timeout_overflow_mode = 'throw'` was cancelled long before the configured limit while
/// the exception still reported the configured (much larger) value. Oversized deadlines must be
/// saturated upwards instead, and only where the deadline is genuinely not representable at all.
TEST(CancellationChecker, LargeTimeoutIsNotTruncated)
{
    static constexpr Int64 ONE_YEAR_US = 365LL * 24 * 60 * 60 * 1'000'000;

    /// One second into the epoch, so that `now` is non-zero and rounding cannot be hidden by it.
    const auto now = std::chrono::steady_clock::time_point{
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::seconds{1})};
    const Int64 now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    /// Representable deadlines keep the exact invariant.
    for (const Int64 timeout_us : {ONE_YEAR_US, ONE_YEAR_US + 1'000, ONE_YEAR_US + 100'000, 100 * ONE_YEAR_US})
    {
        const UInt64 deadline_ms = CancellationChecker::taskDeadlineMs(now, timeout_us);
        ASSERT_GE(static_cast<Int64>(deadline_ms), now_ms + (timeout_us + 999) / 1'000)
            << "a timeout of " << timeout_us << " us was truncated";
    }

    /// `Int64::max()` microseconds is not representable as `now + timeout`; the deadline saturates
    /// near the end of the representable range, which is still about 292 thousand years away.
    const UInt64 saturated_ms = CancellationChecker::taskDeadlineMs(now, std::numeric_limits<Int64>::max());
    ASSERT_GT(saturated_ms, static_cast<UInt64>(now_ms) + static_cast<UInt64>(100 * ONE_YEAR_US / 1'000));
}

/// Converting the timeout to nanoseconds for the comparison overflowed once `max_execution_time`
/// exceeded `UInt64::max()` nanoseconds (about 584 years), which `SettingFieldSeconds` accepts:
/// the first overflowing value wrapped to 384 ns, so the query timed out almost immediately
/// instead of never.
TEST(ExecutionSpeedLimits, HugeMaxExecutionTimeDoesNotOverflow)
{
    /// The first value whose nanosecond conversion no longer fits into `UInt64`.
    static constexpr Int64 FIRST_OVERFLOWING_US = static_cast<Int64>(std::numeric_limits<UInt64>::max() / 1000) + 1;

    for (const Int64 timeout_us : {FIRST_OVERFLOWING_US - 1, FIRST_OVERFLOWING_US, FIRST_OVERFLOWING_US + 1, std::numeric_limits<Int64>::max()})
    {
        ExecutionSpeedLimits limits;
        limits.max_execution_time = Poco::Timespan{timeout_us};

        EXPECT_TRUE(limits.checkTimeLimit(/*elapsed_ns=*/1'000'000'000, OverflowMode::THROW))
            << "a timeout of " << timeout_us << " us tripped after one second";
    }
}

TEST(ExecutionSpeedLimits, FormatsFractionalMaxExecutionTime)
{
    ExecutionSpeedLimits limits;
    limits.max_execution_time = Poco::Timespan{1'000'900};

    try
    {
        limits.checkTimeLimit(1'000'901'000, OverflowMode::THROW);
        FAIL() << "Expected `TIMEOUT_EXCEEDED` exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TIMEOUT_EXCEEDED);
        EXPECT_EQ(e.message(), "Timeout exceeded: elapsed 1000.901 ms, maximum: 1000.900 ms");
    }
}
