#include "config.h"

#if USE_NURAFT

#include <atomic>
#include <optional>
#include <vector>

#include <Coordination/Changelog.h>
#include <Coordination/tests/gtest_coordination_common.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <base/scope_guard.h>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int CANNOT_SCHEDULE_TASK;
}

namespace ProfileEvents
{
    extern const Event KeeperLogsReadAheadReadersCreated;
    extern const Event KeeperLogsReadAheadScheduleRejected;
}

/// A fill task of the read-ahead of `Changelog` parks inside its job until its reader is closed, so it keeps
/// a thread of the global pool for an unlimited time. It used to take no slot of the global pool. Then the
/// parked fill tasks together with the permanent threads could hold all the threads of the global pool while
/// a slot was still left, and a `ThreadFromGlobalPool` created after that was queued and never started.
/// Now a fill task takes a slot, so such a thread is rejected with `CANNOT_SCHEDULE_TASK`, and a reader
/// that cannot get a slot is not created: the entries are read directly.
TEST(ChangelogReadAheadFewGlobalThreads, ParkedFillTaskTakesSlotOfGlobalPool)
{
    const std::string changelog_dir = "./logs_readahead_few_global_threads";
    fs::remove_all(changelog_dir);
    fs::create_directory(changelog_dir);
    SCOPE_EXIT({ fs::remove_all(changelog_dir); });

    auto keeper_context = makeKeeperContext(/*use_lsmt_storage=*/ false);
    keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", changelog_dir));

    auto log = getLogger("ChangelogReadAheadFewGlobalThreadsTest");
    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 1000,
        .latest_logs_cache_size_threshold = 1,
    };

    {
        DB::Changelog writer(log, settings, DB::FlushSettings(), DB::ReadAheadSettings{}, keeper_context);
        writer.readChangelogAndInitWriter(0, 0);
        for (uint64_t i = 1; i <= 20; ++i)
            writer.appendEntry(i, getLogEntry("readahead_few_global_threads_" + std::to_string(i), i));
        writer.flush();
    }
    keeper_context->setLastCommitIndex(20);

    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    /// Declared before the changelog, so that the changelog (and its fill tasks) is destroyed first.
    std::atomic<bool> release = false;
    std::vector<ThreadFromGlobalPool> permanent_threads;
    std::optional<ThreadFromGlobalPool> finite_thread;
    SCOPE_EXIT({
        release = true;
        release.notify_all();
        for (auto & thread : permanent_threads)
            thread.join();
        if (finite_thread && finite_thread->joinable())
            finite_thread->join();
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);
    });

    DB::Changelog changelog(
        log, settings, DB::FlushSettings(), DB::ReadAheadSettings{.enabled = true, .serve_wait_timeout_ms = 5000}, keeper_context);
    changelog.readChangelogAndInitWriter(0, 0);

    /// The changelog has started its own permanent threads. Take all the remaining slots of the global pool
    /// with permanent threads, then add exactly one slot (and one thread).
    global_pool.setMaxThreads(16);
    global_pool.setMaxFreeThreads(0);
    global_pool.setQueueSize(1000);

    while (true)
    {
        try
        {
            permanent_threads.emplace_back(ThreadFromGlobalPoolScheduleMode::FailIfNoWorker, [&] { release.wait(false); });
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(e.code(), DB::ErrorCodes::CANNOT_SCHEDULE_TASK) << e.displayText();
            break;
        }
        ASSERT_LT(permanent_threads.size(), 16u);
    }
    global_pool.setMaxThreads(global_pool.getMaxThreads() + 1);

    /// The fill task of the reader takes the last slot, and parks when it reaches the end of the log.
    const uint64_t created_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated];
    auto plan = changelog.getReadAheadPlan(1, 11, 0);
    ASSERT_TRUE(plan.read_ahead_window.has_value());
    auto entries = changelog.serveReadAhead(/*reader_id=*/ 1, plan);
    ASSERT_NE(entries, nullptr);
    ASSERT_EQ(entries->size(), 10u);
    ASSERT_EQ(ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated] - created_before, 1u);

    /// All the threads of the global pool are held for an unlimited time now, so a new thread must be
    /// rejected instead of being queued forever.
    try
    {
        finite_thread.emplace([] {});
        ADD_FAILURE() << "A thread must not be queued while the permanent threads and a parked fill task "
                         "hold all the threads of the global pool";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_SCHEDULE_TASK) << e.displayText();
    }

    /// Another reader cannot get a slot: its entries are read directly.
    const uint64_t rejected_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadScheduleRejected];
    auto other_plan = changelog.getReadAheadPlan(1, 11, 0);
    auto other_entries = changelog.serveReadAhead(/*reader_id=*/ 2, other_plan);
    ASSERT_NE(other_entries, nullptr);
    ASSERT_EQ(other_entries->size(), 10u);
    for (uint64_t i = 0; i < 10; ++i)
        EXPECT_EQ((*other_entries)[i]->get_term(), i + 1);
    EXPECT_GT(ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadScheduleRejected], rejected_before);
}

#endif
