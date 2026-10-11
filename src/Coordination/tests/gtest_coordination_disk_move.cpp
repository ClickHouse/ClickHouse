#include "config.h"

#if USE_NURAFT

#include <atomic>
#include <filesystem>
#include <functional>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <thread>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperContext.h>

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>

#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>

#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>

#include <gtest/gtest.h>

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 disk_move_retries_during_init;
    extern const CoordinationSettingsUInt64 disk_move_retries_after_init;
    extern const CoordinationSettingsUInt64 disk_move_retries_wait_ms;
}

namespace ProfileEvents
{
    extern const Event KeeperDiskMoves;
    extern const Event KeeperDiskMoveMicroseconds;
    extern const Event KeeperDiskMoveFailedAttempts;
    extern const Event KeeperDiskMovesAbandoned;
}

namespace fs = std::filesystem;

namespace
{

constexpr size_t always_fail = std::numeric_limits<size_t>::max();

/// A local disk whose two failure-prone sub-operations of `moveFileBetweenDisks`
/// can be made to fail a chosen number of times. `moveFileBetweenDisks` catches
/// everything, so a plain `std::runtime_error` is enough to drive the retry loop.
class FlakyDiskLocal : public DB::DiskLocal
{
public:
    FlakyDiskLocal(const std::string & name_, const std::string & path_)
        : DB::DiskLocal(name_, path_)
    {
    }

    /// Only the `tmp_` marker is failed here, not the copy target: `IDisk::copyFile`
    /// also writes through `to_disk.writeFile`, and the two must be injectable apart.
    std::unique_ptr<DB::WriteBufferFromFileBase> writeFile(
        const std::string & path, size_t buf_size, DB::WriteMode mode, const DB::WriteSettings & settings) override
    {
        if (fs::path(path).filename().string().starts_with(DB::tmp_keeper_file_prefix))
        {
            ++tmp_write_calls;
            if (tmp_write_failures_left > 0)
            {
                --tmp_write_failures_left;
                throw std::runtime_error("injected failure while creating the temporary marker");
            }
        }

        return DB::DiskLocal::writeFile(path, buf_size, mode, settings);
    }

    void copyFile(
        const std::string & from_file_path,
        DB::IDisk & to_disk,
        const std::string & to_file_path,
        const DB::ReadSettings & read_settings,
        const DB::WriteSettings & write_settings,
        const std::function<void()> & cancellation_hook) override
    {
        ++copy_calls;
        if (copy_failures_left > 0)
        {
            --copy_failures_left;
            throw std::runtime_error("injected failure while copying the file");
        }

        DB::DiskLocal::copyFile(from_file_path, to_disk, to_file_path, read_settings, write_settings, cancellation_hook);
    }

    std::atomic<size_t> tmp_write_failures_left = 0;
    std::atomic<size_t> copy_failures_left = 0;

    std::atomic<size_t> tmp_write_calls = 0;
    std::atomic<size_t> copy_calls = 0;
};

using FlakyDiskLocalPtr = std::shared_ptr<FlakyDiskLocal>;

struct DiskMoveEvents
{
    UInt64 moves = 0;
    UInt64 microseconds = 0;
    UInt64 failed_attempts = 0;
    UInt64 abandoned = 0;
};

/// Thread counters have `global_counters` as their parent, so an increment reaches
/// it whether or not the moving thread has a `ThreadStatus`.
DiskMoveEvents readDiskMoveEvents()
{
    return DiskMoveEvents{
        .moves = ProfileEvents::global_counters[ProfileEvents::KeeperDiskMoves],
        .microseconds = ProfileEvents::global_counters[ProfileEvents::KeeperDiskMoveMicroseconds],
        .failed_attempts = ProfileEvents::global_counters[ProfileEvents::KeeperDiskMoveFailedAttempts],
        .abandoned = ProfileEvents::global_counters[ProfileEvents::KeeperDiskMovesAbandoned],
    };
}

DiskMoveEvents operator-(const DiskMoveEvents & after, const DiskMoveEvents & before)
{
    return DiskMoveEvents{
        .moves = after.moves - before.moves,
        .microseconds = after.microseconds - before.microseconds,
        .failed_attempts = after.failed_attempts - before.failed_attempts,
        .abandoned = after.abandoned - before.abandoned,
    };
}

/// Two local disks with a 1 MiB `changelog.bin` on the source one.
struct DiskMoveFixture
{
    explicit DiskMoveFixture(const std::string & test_dir_)
        : test_dir(test_dir_)
    {
        fs::remove_all(test_dir);
        fs::create_directories(test_dir + "/from");
        fs::create_directories(test_dir + "/to");

        disk_from = std::make_shared<FlakyDiskLocal>("From", test_dir + "/from");
        disk_to = std::make_shared<FlakyDiskLocal>("To", test_dir + "/to");

        auto buf = disk_from->writeFile("changelog.bin", DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, {});
        DB::writeString(std::string(1024 * 1024, 'x'), *buf);
        buf->finalize();

        settings = std::make_shared<DB::CoordinationSettings>();
        /// Nothing here waits for the sleep between retries, it only slows tests down.
        (*settings)[DB::CoordinationSetting::disk_move_retries_wait_ms] = 1;
    }

    ~DiskMoveFixture()
    {
        std::error_code ignored_error;
        fs::remove_all(test_dir, ignored_error);
    }

    DiskMoveFixture(const DiskMoveFixture &) = delete;
    DiskMoveFixture & operator=(const DiskMoveFixture &) = delete;

    DB::KeeperContextPtr makeKeeperContext(DB::KeeperContext::Phase phase) const
    {
        auto keeper_context = std::make_shared<DB::KeeperContext>(/*standalone_keeper_=*/true, settings);
        keeper_context->setServerState(phase);
        return keeper_context;
    }

    std::string test_dir;
    FlakyDiskLocalPtr disk_from;
    FlakyDiskLocalPtr disk_to;
    DB::CoordinationSettingsPtr settings;
};

}

/// Moving a finished changelog or snapshot to the object storage disk is the only thing that frees
/// Keeper's local log disk. The Raft write path is exempt from memory limit exceptions and keeps
/// producing files regardless of memory pressure, so a mover that the memory tracker can refuse
/// turns memory pressure into a full local disk, a failing `fallocate` and a NuRaft fail-stop.
TEST(KeeperDiskMove, MoveIsNotRefusedByTheMemoryTracker)
{
    const std::string test_dir = "./keeper_disk_move_test";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir + "/from");
    fs::create_directories(test_dir + "/to");
    SCOPE_EXIT_SAFE(fs::remove_all(test_dir));

    auto settings = std::make_shared<DB::CoordinationSettings>();
    /// Give up after the first failure so that a refused move fails this test quickly
    /// instead of burning through the whole retry budget.
    (*settings)[DB::CoordinationSetting::disk_move_retries_during_init] = 1;
    (*settings)[DB::CoordinationSetting::disk_move_retries_after_init] = 1;
    (*settings)[DB::CoordinationSetting::disk_move_retries_wait_ms] = 1;
    auto keeper_context = std::make_shared<DB::KeeperContext>(/*standalone_keeper_=*/true, settings);

    DB::DiskPtr disk_from = std::make_shared<DB::DiskLocal>("From", test_dir + "/from");
    DB::DiskPtr disk_to = std::make_shared<DB::DiskLocal>("To", test_dir + "/to");

    {
        auto buf = disk_from->writeFile("changelog.bin", DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, {});
        DB::writeString(std::string(1024 * 1024, 'x'), *buf);
        buf->finalize();
    }

    /// `getMemoryTracker` only reaches `total_memory_tracker` once the main thread status exists.
    DB::MainThreadStatus::getInstance();
    auto log = getLogger("KeeperDiskMoveTest");

    /// The move runs on a thread with no `ThreadStatus`, so every allocation goes straight to
    /// `total_memory_tracker` instead of being batched into this thread's untracked memory, and a
    /// hard limit below the amount already tracked refuses all of them - the incident's state, where
    /// RSS stayed above the limit for over an hour.
    std::thread mover(
        [&]
        {
            const Int64 previous_hard_limit = total_memory_tracker.getHardLimit();
            SCOPE_EXIT_SAFE(total_memory_tracker.setHardLimit(previous_hard_limit));
            total_memory_tracker.setHardLimit(1);

            EXPECT_TRUE(DB::moveFileBetweenDisks(disk_from, "changelog.bin", disk_to, "changelog.bin", {}, log, keeper_context));
        });
    mover.join();

    EXPECT_TRUE(disk_to->existsFile("changelog.bin"));
    EXPECT_FALSE(disk_from->existsFile("changelog.bin"));
}

/// A running server used to retry a failed sub-operation until shutdown, pinning the
/// caller instead of failing it. `disk_move_retries_after_init` must bound it.
TEST(KeeperDiskMove, RuntimeRetriesAreBounded)
{
    DiskMoveFixture fixture("./keeper_disk_move_runtime_bound_test");
    (*fixture.settings)[DB::CoordinationSetting::disk_move_retries_during_init] = 1000;
    (*fixture.settings)[DB::CoordinationSetting::disk_move_retries_after_init] = 3;
    auto keeper_context = fixture.makeKeeperContext(DB::KeeperContext::Phase::RUNNING);

    fixture.disk_to->tmp_write_failures_left = always_fail;

    bool before_file_remove_op_called = false;
    const auto before = readDiskMoveEvents();
    const bool moved = DB::moveFileBetweenDisks(
        fixture.disk_from,
        "changelog.bin",
        fixture.disk_to,
        "changelog.bin",
        [&] { before_file_remove_op_called = true; return true; },
        getLogger("KeeperDiskMoveTest"),
        keeper_context);
    const auto delta = readDiskMoveEvents() - before;

    /// Bounded by the runtime limit, not by the much larger initialization one.
    EXPECT_FALSE(moved);
    EXPECT_EQ(fixture.disk_to->tmp_write_calls.load(), 3u);
    EXPECT_EQ(fixture.disk_from->copy_calls.load(), 0u);

    EXPECT_EQ(delta.moves, 1u);
    EXPECT_EQ(delta.failed_attempts, 3u);
    EXPECT_EQ(delta.abandoned, 1u);

    /// The source file is untouched and the caller's metadata was never repointed at
    /// the target, so the abandoned file is still tracked on the disk it is on.
    EXPECT_FALSE(before_file_remove_op_called);
    EXPECT_TRUE(fixture.disk_from->existsFile("changelog.bin"));
    EXPECT_FALSE(fixture.disk_to->existsFile("changelog.bin"));
    EXPECT_FALSE(fixture.disk_to->existsFile("tmp_changelog.bin"));
}

/// `disk_move_retries_during_init` keeps its old meaning and is what bounds a move
/// while the server is still initializing, even when the runtime limit is larger.
TEST(KeeperDiskMove, InitRetriesAreStillBounded)
{
    DiskMoveFixture fixture("./keeper_disk_move_init_bound_test");
    (*fixture.settings)[DB::CoordinationSetting::disk_move_retries_during_init] = 2;
    (*fixture.settings)[DB::CoordinationSetting::disk_move_retries_after_init] = 1000;
    auto keeper_context = fixture.makeKeeperContext(DB::KeeperContext::Phase::INIT);

    fixture.disk_from->copy_failures_left = always_fail;

    bool before_file_remove_op_called = false;
    const auto before = readDiskMoveEvents();
    const bool moved = DB::moveFileBetweenDisks(
        fixture.disk_from,
        "changelog.bin",
        fixture.disk_to,
        "changelog.bin",
        [&] { before_file_remove_op_called = true; return true; },
        getLogger("KeeperDiskMoveTest"),
        keeper_context);
    const auto delta = readDiskMoveEvents() - before;

    EXPECT_FALSE(moved);
    EXPECT_EQ(fixture.disk_to->tmp_write_calls.load(), 1u);
    EXPECT_EQ(fixture.disk_from->copy_calls.load(), 2u);

    EXPECT_EQ(delta.moves, 1u);
    EXPECT_EQ(delta.failed_attempts, 2u);
    EXPECT_EQ(delta.abandoned, 1u);

    EXPECT_FALSE(before_file_remove_op_called);
    EXPECT_TRUE(fixture.disk_from->existsFile("changelog.bin"));
    EXPECT_FALSE(fixture.disk_to->existsFile("changelog.bin"));
    /// The marker is deliberately left behind: it is what makes the incomplete copy
    /// detectable by the startup scan, which removes both it and its partner file.
    EXPECT_TRUE(fixture.disk_to->existsFile("tmp_changelog.bin"));
}

/// Bounding the retries must not turn a transient failure into an abandoned move.
TEST(KeeperDiskMove, TransientFailuresAreRetriedAndTheMoveSucceeds)
{
    DiskMoveFixture fixture("./keeper_disk_move_transient_test");
    (*fixture.settings)[DB::CoordinationSetting::disk_move_retries_during_init] = 1000;
    (*fixture.settings)[DB::CoordinationSetting::disk_move_retries_after_init] = 10;
    auto keeper_context = fixture.makeKeeperContext(DB::KeeperContext::Phase::RUNNING);

    fixture.disk_to->tmp_write_failures_left = 2;
    fixture.disk_from->copy_failures_left = 1;

    size_t before_file_remove_op_calls = 0;
    const auto before = readDiskMoveEvents();
    const bool moved = DB::moveFileBetweenDisks(
        fixture.disk_from,
        "changelog.bin",
        fixture.disk_to,
        "changelog.bin",
        [&] { ++before_file_remove_op_calls; return true; },
        getLogger("KeeperDiskMoveTest"),
        keeper_context);
    const auto delta = readDiskMoveEvents() - before;

    EXPECT_TRUE(moved);
    EXPECT_EQ(fixture.disk_to->tmp_write_calls.load(), 3u);
    EXPECT_EQ(fixture.disk_from->copy_calls.load(), 2u);

    EXPECT_EQ(delta.moves, 1u);
    EXPECT_EQ(delta.failed_attempts, 3u);
    EXPECT_EQ(delta.abandoned, 0u);
    EXPECT_GT(delta.microseconds, 0u);

    EXPECT_EQ(before_file_remove_op_calls, 1u);
    EXPECT_TRUE(fixture.disk_to->existsFile("changelog.bin"));
    EXPECT_FALSE(fixture.disk_to->existsFile("tmp_changelog.bin"));
    EXPECT_FALSE(fixture.disk_from->existsFile("changelog.bin"));
}

/// A move with nothing to fail must not report a retry or an abandonment.
TEST(KeeperDiskMove, SuccessfulMoveReportsNoRetries)
{
    DiskMoveFixture fixture("./keeper_disk_move_success_test");
    (*fixture.settings)[DB::CoordinationSetting::disk_move_retries_after_init] = 3;
    auto keeper_context = fixture.makeKeeperContext(DB::KeeperContext::Phase::RUNNING);

    const auto before = readDiskMoveEvents();
    const bool moved = DB::moveFileBetweenDisks(
        fixture.disk_from, "changelog.bin", fixture.disk_to, "changelog.bin", {}, getLogger("KeeperDiskMoveTest"), keeper_context);
    const auto delta = readDiskMoveEvents() - before;

    EXPECT_TRUE(moved);
    EXPECT_EQ(delta.moves, 1u);
    EXPECT_EQ(delta.failed_attempts, 0u);
    EXPECT_EQ(delta.abandoned, 0u);
    EXPECT_GT(delta.microseconds, 0u);

    EXPECT_TRUE(fixture.disk_to->existsFile("changelog.bin"));
    EXPECT_FALSE(fixture.disk_to->existsFile("tmp_changelog.bin"));
    EXPECT_FALSE(fixture.disk_from->existsFile("changelog.bin"));
}

#endif
