#include "config.h"

#if USE_NURAFT

#include <filesystem>
#include <thread>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperContext.h>

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>

#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>

#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>

#include <gtest/gtest.h>

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 disk_move_retries_during_init;
    extern const CoordinationSettingsUInt64 disk_move_retries_wait_ms;
}

namespace fs = std::filesystem;

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
    /// The retry loop is bounded only in `Phase::INIT`, which is what a fresh context is in. Give up
    /// after the first failure so that a refused move fails this test instead of retrying forever.
    (*settings)[DB::CoordinationSetting::disk_move_retries_during_init] = 1;
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

            DB::moveFileBetweenDisks(disk_from, "changelog.bin", disk_to, "changelog.bin", {}, log, keeper_context);
        });
    mover.join();

    EXPECT_TRUE(disk_to->existsFile("changelog.bin"));
    EXPECT_FALSE(disk_from->existsFile("changelog.bin"));
}

/// The destination may have a different name and directory than the source, e.g. a rotated
/// changelog or a move into `detached/`. The `tmp_` marker belongs next to the destination, so a
/// completed move must leave neither a marker in the destination directory nor one at the root.
TEST(KeeperDiskMove, MoveToRenamedPathInSubdirectory)
{
    const std::string test_dir = "./keeper_disk_move_renamed_test";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir + "/from");
    fs::create_directories(test_dir + "/to/detached/ts");
    SCOPE_EXIT_SAFE(fs::remove_all(test_dir));

    auto settings = std::make_shared<DB::CoordinationSettings>();
    (*settings)[DB::CoordinationSetting::disk_move_retries_during_init] = 1;
    (*settings)[DB::CoordinationSetting::disk_move_retries_wait_ms] = 1;
    auto keeper_context = std::make_shared<DB::KeeperContext>(/*standalone_keeper_=*/true, settings);

    DB::DiskPtr disk_from = std::make_shared<DB::DiskLocal>("From", test_dir + "/from");
    DB::DiskPtr disk_to = std::make_shared<DB::DiskLocal>("To", test_dir + "/to");

    const std::string content(64 * 1024, 'y');
    {
        auto buf = disk_from->writeFile("changelog_1_100000.bin", DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, {});
        DB::writeString(content, *buf);
        buf->finalize();
    }

    const std::string path_to = "detached/ts/changelog_1_42.bin";
    bool marker_present_before_source_removal = true;
    DB::moveFileBetweenDisks(
        disk_from,
        "changelog_1_100000.bin",
        disk_to,
        path_to,
        [&]
        {
            marker_present_before_source_removal = disk_to->existsFile("detached/ts/tmp_changelog_1_42.bin")
                || disk_to->existsFile("tmp_changelog_1_100000.bin");
            return true;
        },
        getLogger("KeeperDiskMoveTest"),
        keeper_context);

    EXPECT_FALSE(marker_present_before_source_removal);
    EXPECT_FALSE(disk_from->existsFile("changelog_1_100000.bin"));
    ASSERT_TRUE(disk_to->existsFile(path_to));
    EXPECT_EQ(disk_to->getFileSize(path_to), content.size());

    for (const auto & entry : fs::recursive_directory_iterator(test_dir + "/to"))
        EXPECT_FALSE(entry.path().filename().string().starts_with(DB::tmp_keeper_file_prefix)) << entry.path();
}

#endif
