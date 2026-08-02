#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

#include <base/scope_guard.h>

#include <Core/Settings.h>
#include <Interpreters/CancellationChecker.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/IAST.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/CurrentThread.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace DB::Setting
{
extern const SettingsSeconds max_execution_time;
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

UInt64 steadyNowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

}

/// A grid-aligned deadline must never land before the exact one. Truncating either operand to whole
/// milliseconds broke this whenever the truncated deadline was already grid-aligned, because the
/// alignment then had no rounding left with which to absorb the discarded sub-millisecond.
TEST(CancellationChecker, NeverCancelsBeforeTimeout)
{
    static constexpr UInt64 NS_PER_MS = 1'000'000;
    const UInt64 grid = CancellationChecker::CANCELLATION_GRID_MS;

    size_t early_cases = 0;
    /// `max_execution_time` is a `SettingsSeconds`, which keeps microsecond resolution, so the
    /// timeouts that are not a whole number of milliseconds are as reachable as the ones that are.
    for (UInt64 timeout_us : {1'000UL, 1'999UL, 37'400UL, 100'000UL, 100'001UL, 1'000'500UL})
    {
        /// Every position on the alignment grid, combined with sub-millisecond residues that
        /// bracket both ends of the truncation window.
        for (UInt64 grid_offset_ms = 0; grid_offset_ms < grid; ++grid_offset_ms)
        {
            for (UInt64 residue_ns : {0UL, 1UL, 500'000UL, 999'998UL, 999'999UL})
            {
                const UInt64 now_ns = grid_offset_ms * NS_PER_MS + residue_ns;
                const UInt64 deadline_ms = CancellationChecker::alignedDeadlineMs(now_ns, timeout_us);

                if (deadline_ms * NS_PER_MS < now_ns + timeout_us * 1000)
                    ++early_cases;

                EXPECT_EQ(deadline_ms % grid, 0u) << "deadline is not grid-aligned";
                /// Not gratuitously late either: at most one grid step past the exact deadline.
                EXPECT_LT(deadline_ms * NS_PER_MS, now_ns + timeout_us * 1000 + grid * NS_PER_MS);
            }
        }
    }
    EXPECT_EQ(early_cases, 0u) << "deadline placed before the exact timeout";
}

/// The same contract, asserted on the deadline `appendTask` arms rather than on the helper alone.
/// The timeout is solved so that the truncated deadline is already grid-aligned, leaving the
/// alignment no rounding with which to absorb a discarded fraction: truncation is then always one
/// grid step early and correct arithmetic never is, so no waiting or tolerance is involved.
TEST(CancellationChecker, AppendTaskUsesAlignedDeadline)
{
    static constexpr UInt64 NS_PER_MS = 1'000'000;
    /// Enough clock reads to span several milliseconds, so exhausting them means the clock never
    /// exposes a sub-millisecond residue rather than that this attempt was unlucky.
    static constexpr int max_boundary_samples = 1'000'000;
    const UInt64 grid = CancellationChecker::CANCELLATION_GRID_MS;

    auto & checker = CancellationChecker::getInstance();
    std::vector<QueryStatusPtr> queries;

    std::thread worker([&] { checker.workerFunction(); });
    /// Runs on every exit path (including early ASSERT returns, which would otherwise destroy a
    /// joinable thread and terminate); also drains leftover tasks from the singleton.
    SCOPE_EXIT({
        for (const auto & query : queries)
            checker.appendDoneTasks(query);
        checker.terminateThread();
        worker.join();
    });

    /// The singleton is shared with the other tests in this binary; start from an empty queue so
    /// `getArmedDeadline` below can only report the task appended here.
    for (int i = 0; i < 2000 && checker.getArmedDeadline() != 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_EQ(checker.getArmedDeadline(), 0u);

    bool measured = false;
    for (int attempt = 0; attempt < 20 && !measured; ++attempt)
    {
        /// Constructed before the timed window below, so that only `appendTask` runs inside it: a
        /// `QueryStatus` carries a whole default-constructed `Settings`, which on an instrumented
        /// build can outlast the sub-millisecond budget the window depends on. A fresh one per
        /// attempt, since `QueryStatus::is_killed` is sticky.
        auto query = makeQueryStatus("gtest_cancellation_checker_aligned_" + std::to_string(attempt));
        queries.push_back(query);

        /// Sync to just after a millisecond boundary: the residue must be non-zero for truncation
        /// to discard anything, and small so that `appendTask` reads the same millisecond.
        UInt64 now_ns = steadyNowNs();
        int samples = 0;
        while (now_ns % NS_PER_MS == 0 || now_ns % NS_PER_MS > NS_PER_MS / 10)
        {
            ASSERT_LT(++samples, max_boundary_samples)
                << "steady_clock never reported a sub-100us residue; the clock source lacks the "
                   "resolution this test needs";
            now_ns = steadyNowNs();
        }

        /// `(now_ms + timeout) % grid == 0`, plus whole grid steps of headroom so the deadline does
        /// not fire before it is read. The addend must be a multiple of the grid to preserve the
        /// alignment this test depends on. The fractional half-millisecond makes the timeout itself
        /// lossy under truncation, so the case also covers flooring the timeout rather than `now`.
        const UInt64 now_ms = now_ns / NS_PER_MS;
        const UInt64 aligning_timeout = (grid - now_ms % grid) % grid;
        const UInt64 timeout_us = ((aligning_timeout == 0 ? grid : aligning_timeout) + 5 * grid) * 1000 + 500;

        ASSERT_TRUE(checker.appendTask(query, static_cast<Int64>(timeout_us), OverflowMode::THROW));

        if (steadyNowNs() / NS_PER_MS != now_ms)
        {
            /// `appendTask` read a later millisecond, so the solved timeout is no longer aligned for
            /// it and the grid would mask the truncation. Retire this task and retry.
            checker.appendDoneTasks(query);
            for (int i = 0; i < 2000 && checker.getArmedDeadline() != 0; ++i)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            /// Same guarantee the first attempt gets above: an unemptied queue would let the next
            /// attempt read this retired task's deadline and assert against the wrong task.
            ASSERT_EQ(checker.getArmedDeadline(), 0u) << "retired task is still armed";
            continue;
        }

        UInt64 deadline_ms = 0;
        for (int i = 0; i < 2000 && deadline_ms == 0; ++i)
        {
            deadline_ms = checker.getArmedDeadline();
            if (deadline_ms == 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_NE(deadline_ms, 0u);

        /// Valid whether or not `appendTask` read the same nanosecond: it ran no earlier than
        /// `now_ns`, so its exact deadline is no earlier than `now_ns + timeout_us`.
        EXPECT_GE(deadline_ms * NS_PER_MS, now_ns + timeout_us * 1000)
            << "appendTask armed a deadline before the exact timeout";
        measured = true;
    }
    EXPECT_TRUE(measured) << "could not observe appendTask within a single millisecond";
}

/// `appendTask` receives microseconds, so its own test stays green if the conversion at the sole
/// production caller floors the setting to whole milliseconds. The timeout is solved as above, with
/// a fraction large enough that the floored deadline lands a whole millisecond short of the exact
/// one: flooring is then always caught and correct arithmetic never is.
TEST(CancellationChecker, ProcessListPreservesFractionalTimeout)
{
    static constexpr UInt64 NS_PER_MS = 1'000'000;
    static constexpr int max_boundary_samples = 1'000'000;
    const UInt64 grid = CancellationChecker::CANCELLATION_GRID_MS;

    auto & checker = CancellationChecker::getInstance();

    /// `insert` calls `CurrentThread::attachQueryForLog`, which throws unless this thread either has
    /// no `ThreadStatus` at all or has one with a group attached. Run in a dedicated thread and set
    /// up both, so the state matches a real query rather than whatever other tests left behind.
    std::thread body([&]
    {
        ThreadStatus thread_status;
        auto group_context = Context::createCopy(getContext().context);
        group_context->makeQueryContext();
        CurrentThread::attachToGroup(std::make_shared<ThreadGroup>(group_context, 0));
        /// Declared after `thread_status`, so it detaches before that is destroyed: `~ThreadStatus`
        /// requires the group's counters to be finalized from this same thread.
        SCOPE_EXIT({ CurrentThread::detachFromGroupIfNotDetached(); });

        std::thread worker([&] { checker.workerFunction(); });
        SCOPE_EXIT({
            checker.terminateThread();
            worker.join();
        });

        for (int i = 0; i < 2000 && checker.getArmedDeadline() != 0; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        ASSERT_EQ(checker.getArmedDeadline(), 0u);

        ProcessList process_list;

        /// Pays the process-wide lazy initialization of the first insertion, which is far slower
        /// than the window below tolerates. The timeout must clear `appendTask`'s 1 ms guard to warm
        /// the registration too, and stay small because removal leaves an armed deadline armed.
        {
            auto warmup_context = Context::createCopy(getContext().context);
            warmup_context->makeQueryContext();
            warmup_context->setCurrentQueryId("gtest_cancellation_checker_fractional_warmup");
            warmup_context->setSetting("max_execution_time", 0.002);
            process_list.insert(
                "SELECT 1", /*normalized_query_hash*/ 0, /*ast*/ nullptr, warmup_context, steadyNowNs(), /*is_internal*/ false);
        }
        for (int i = 0; i < 2000 && checker.getArmedDeadline() != 0; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        ASSERT_EQ(checker.getArmedDeadline(), 0u) << "warm-up query is still armed";

        bool measured = false;
        for (int attempt = 0; attempt < 20 && !measured; ++attempt)
        {
            /// Built before the window below, leaving only the setting and the insertion inside it:
            /// copying a `Context` deep-copies a whole `Settings`, which an instrumented build can
            /// stretch past the budget the window depends on.
            auto context = Context::createCopy(getContext().context);
            context->makeQueryContext();
            context->setCurrentQueryId("gtest_cancellation_checker_fractional_" + std::to_string(attempt));

            /// The residue must exceed 1 us, so that adding the 999 us fraction crosses into the next
            /// millisecond and a floored timeout is observably short.
            UInt64 now_ns = steadyNowNs();
            int samples = 0;
            while (now_ns % NS_PER_MS <= 1000 || now_ns % NS_PER_MS > NS_PER_MS / 10)
            {
                ASSERT_LT(++samples, max_boundary_samples)
                    << "steady_clock never reported a residue between 1us and 100us";
                now_ns = steadyNowNs();
            }

            const UInt64 deadline_ms_exact = (now_ns + 999'999) / NS_PER_MS;
            const UInt64 aligning = (grid - deadline_ms_exact % grid) % grid;
            const UInt64 timeout_us = ((aligning == 0 ? grid : aligning) + 5 * grid) * 1000 + 999;

            context->setSetting("max_execution_time", static_cast<double>(timeout_us) / 1'000'000);
            ASSERT_EQ(context->getSettingsRef()[Setting::max_execution_time].totalMicroseconds(),
                      static_cast<Int64>(timeout_us));

            auto entry = process_list.insert(
                "SELECT 1", /*normalized_query_hash*/ 0, /*ast*/ nullptr, context, now_ns, /*is_internal*/ false);

            if (steadyNowNs() / NS_PER_MS != now_ns / NS_PER_MS)
            {
                /// `insert` read a later millisecond, so the solved timeout is no longer aligned for it.
                /// Retire the query and wait for the queue to drain: a still-armed deadline from this
                /// attempt would be read by the next one, which would then assert against the wrong task.
                entry.reset();
                for (int i = 0; i < 2000 && checker.getArmedDeadline() != 0; ++i)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                ASSERT_EQ(checker.getArmedDeadline(), 0u) << "retired query is still armed";
                continue;
            }

            UInt64 deadline_ms = 0;
            for (int i = 0; i < 2000 && deadline_ms == 0; ++i)
            {
                deadline_ms = checker.getArmedDeadline();
                if (deadline_ms == 0)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            ASSERT_NE(deadline_ms, 0u) << "ProcessList::insert did not register the query";

            EXPECT_GE(deadline_ms * NS_PER_MS, now_ns + timeout_us * 1000)
                << "the deadline was armed before the exact timeout the setting names";
            measured = true;
        }
        EXPECT_TRUE(measured) << "could not observe ProcessList::insert within a single millisecond";
    });
    body.join();
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
