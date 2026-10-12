#include "config.h"

#if USE_NURAFT

#include <Coordination/Changelog.h>
#include <Coordination/tests/gtest_coordination_common.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <base/scope_guard.h>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int CANNOT_SCHEDULE_TASK;
}

/// Regression test: `Changelog` starts several long-lived `ThreadFromGlobalPool`s in its
/// constructor. When one of them fails to start (`CANNOT_SCHEDULE_TASK`, e.g. because
/// `max_thread_pool_size` is exhausted), the constructor must join the already-started threads
/// and propagate the exception. Otherwise unwinding destroys joinable `ThreadFromGlobalPool`
/// members and `~ThreadFromGlobalPoolImpl` aborts the process before the real error is reported.
///
/// The fault injector is probabilistic, so a single run cannot deterministically fail exactly
/// the second or third thread start. Instead, run enough iterations at 50% fault probability:
/// without the cleanup, an iteration where the first thread starts but a later one fails aborts
/// with probability 3/8, so 32 iterations miss the bug with probability under 1e-6.
TEST(ChangelogThreadStartFailure, ConstructorUnwindsCleanlyOnCannotScheduleTask)
{
    /// Not `ChangelogDirTest`: it expects the directory to not exist, but a previous run of this
    /// test may have aborted (that is exactly what it checks for) without running the cleanup.
    const std::string changelog_dir = "./logs_ctor_unwind";
    fs::remove_all(changelog_dir);
    fs::create_directory(changelog_dir);
    SCOPE_EXIT({ fs::remove_all(changelog_dir); });

    auto keeper_context = makeKeeperContext(/*use_lsmt_storage=*/ false);
    keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", changelog_dir));

    CannotAllocateThreadFaultInjector::setFaultProbability(0.5);
    SCOPE_EXIT({ CannotAllocateThreadFaultInjector::setFaultProbability(0.0); });

    for (size_t i = 0; i < 32; ++i)
    {
        try
        {
            DB::Changelog changelog(
                getLogger("ChangelogThreadStartFailureTest"),
                DB::LogFileSettings{.force_sync = false, .compress_logs = false, .rotate_interval = 100},
                DB::FlushSettings{},
                DB::ReadAheadSettings{},
                keeper_context);
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_SCHEDULE_TASK)
                << "constructing Changelog under thread-allocation fault injection must either "
                   "succeed or throw CANNOT_SCHEDULE_TASK, got: " << e.displayText();
        }
    }
}

#endif
