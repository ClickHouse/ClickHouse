#include "config.h"

#if USE_NURAFT

#include <filesystem>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperContext.h>

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>

#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromFileDecorator.h>
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

namespace
{

/// What `moveFileBetweenDisks` asked the destination disk to do, in order.
struct DiskMoveEvents
{
    std::vector<std::string> log;
    size_t syncs = 0;
    /// The 1-based number of the file or directory sync that throws, 0 for none.
    size_t failing_sync = 0;

    void sync(const std::string & what)
    {
        if (++syncs == failing_sync)
            throw std::runtime_error("Injected failure: " + what);
        log.push_back(what);
    }
};

class RecordingWriteBuffer : public DB::WriteBufferFromFileDecorator
{
public:
    RecordingWriteBuffer(std::unique_ptr<DB::WriteBuffer> impl_, DiskMoveEvents & events_, std::string path_)
        : DB::WriteBufferFromFileDecorator(std::move(impl_)), events(events_), path(std::move(path_))
    {
    }

    void sync() override { events.sync("sync file " + path); }

private:
    DiskMoveEvents & events;
    const std::string path;
};

class RecordingSyncGuard : public DB::ISyncGuard
{
public:
    RecordingSyncGuard(DiskMoveEvents & events_, std::string path_) : events(events_), path(std::move(path_)) { }

    void sync() override { events.sync("sync directory " + path); }

private:
    DiskMoveEvents & events;
    const std::string path;
};

/// A local destination disk that records writes, marker removal and syncs, and can fail the copy.
class RecordingDisk : public DB::DiskLocal
{
public:
    RecordingDisk(const std::string & name_, const std::string & path_, DiskMoveEvents & events_)
        : DB::DiskLocal(name_, path_), events(events_)
    {
    }

    std::unique_ptr<DB::WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, DB::WriteMode mode, const DB::WriteSettings & settings) override
    {
        if (fail_copy && !fs::path(path).filename().string().starts_with(DB::tmp_keeper_file_prefix))
            throw std::runtime_error("Injected failure: write " + path);

        events.log.push_back("write " + path);
        return std::make_unique<RecordingWriteBuffer>(DB::DiskLocal::writeFile(path, buf_size, mode, settings), events, path);
    }

    void removeFileIfExists(const String & path) override
    {
        events.log.push_back("remove " + path);
        DB::DiskLocal::removeFileIfExists(path);
    }

    DB::SyncGuardPtr getDirectorySyncGuard(const String & path) const override
    {
        return std::make_unique<RecordingSyncGuard>(events, path);
    }

    bool fail_copy = false;

private:
    DiskMoveEvents & events;
};

/// Moves `changelog_1_100000.bin` to `detached/ts/changelog_1_42.bin` on another disk: the destination
/// has a different name and directory than the source, as for a rotated changelog or a detached one.
struct RenamingDiskMove
{
    static constexpr auto test_dir = "./keeper_disk_move_renamed_test";
    static constexpr auto path_from = "changelog_1_100000.bin";
    static constexpr auto path_to = "detached/ts/changelog_1_42.bin";
    static constexpr auto marker = "detached/ts/tmp_changelog_1_42.bin";
    static constexpr size_t file_size = 64 * 1024;

    DiskMoveEvents events;
    DB::DiskPtr disk_from;
    std::shared_ptr<RecordingDisk> disk_to;
    /// The destination's events at the moment the caller is asked whether the source may be removed.
    std::optional<std::vector<std::string>> events_before_source_removal;

    RenamingDiskMove()
    {
        fs::remove_all(test_dir);
        const std::string dir = test_dir;
        fs::create_directories(dir + "/from");
        fs::create_directories(dir + "/to/detached/ts");

        disk_from = std::make_shared<DB::DiskLocal>("From", dir + "/from");
        disk_to = std::make_shared<RecordingDisk>("To", dir + "/to", events);

        auto buf = disk_from->writeFile(path_from, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, {});
        DB::writeString(std::string(file_size, 'y'), *buf);
        buf->finalize();
    }

    ~RenamingDiskMove() { fs::remove_all(test_dir); }

    void run()
    {
        auto settings = std::make_shared<DB::CoordinationSettings>();
        /// Give up after the first failure, see `MoveIsNotRefusedByTheMemoryTracker`.
        (*settings)[DB::CoordinationSetting::disk_move_retries_during_init] = 1;
        (*settings)[DB::CoordinationSetting::disk_move_retries_wait_ms] = 1;
        auto keeper_context = std::make_shared<DB::KeeperContext>(/*standalone_keeper_=*/true, settings);

        DB::moveFileBetweenDisks(
            disk_from,
            path_from,
            disk_to,
            path_to,
            [&]
            {
                events_before_source_removal = events.log;
                return true;
            },
            getLogger("KeeperDiskMoveTest"),
            keeper_context);
    }
};

}

/// Startup recovery pairs `tmp_<name>` with `<name>` in the same directory of the destination disk,
/// so the marker of an interrupted copy must be named after the destination and live next to it.
TEST(KeeperDiskMove, InterruptedCopyLeavesMarkerNextToDestination)
{
    RenamingDiskMove scenario;
    scenario.disk_to->fail_copy = true;
    scenario.run();

    EXPECT_TRUE(scenario.disk_to->existsFile(RenamingDiskMove::marker));
    EXPECT_FALSE(scenario.disk_to->existsFile("tmp_changelog_1_100000.bin"));
    EXPECT_FALSE(scenario.disk_to->existsFile(RenamingDiskMove::path_to));
    EXPECT_FALSE(scenario.events_before_source_removal.has_value());
    EXPECT_TRUE(scenario.disk_from->existsFile(RenamingDiskMove::path_from));
}

/// The marker, the copy and the marker removal must each be synced, file and directory, before the
/// next step, and the source may be removed only after all of them.
TEST(KeeperDiskMove, SourceIsRemovedOnlyAfterDestinationIsSynced)
{
    size_t syncs = 0;
    {
        RenamingDiskMove scenario;
        scenario.run();

        const std::vector<std::string> expected{
            "write detached/ts/tmp_changelog_1_42.bin",
            "sync file detached/ts/tmp_changelog_1_42.bin",
            "sync directory detached/ts",
            "write detached/ts/changelog_1_42.bin",
            "sync file detached/ts/changelog_1_42.bin",
            "sync directory detached/ts",
            "remove detached/ts/tmp_changelog_1_42.bin",
            "sync directory detached/ts",
        };
        ASSERT_TRUE(scenario.events_before_source_removal.has_value());
        EXPECT_EQ(*scenario.events_before_source_removal, expected);

        EXPECT_FALSE(scenario.disk_from->existsFile(RenamingDiskMove::path_from));
        ASSERT_TRUE(scenario.disk_to->existsFile(RenamingDiskMove::path_to));
        EXPECT_EQ(scenario.disk_to->getFileSize(RenamingDiskMove::path_to), RenamingDiskMove::file_size);
        EXPECT_FALSE(scenario.disk_to->existsFile(RenamingDiskMove::marker));

        syncs = scenario.events.syncs;
    }

    /// A failure of any of these syncs must keep the source.
    for (size_t failing_sync = 1; failing_sync <= syncs; ++failing_sync)
    {
        SCOPED_TRACE(failing_sync);
        RenamingDiskMove scenario;
        scenario.events.failing_sync = failing_sync;
        scenario.run();

        EXPECT_FALSE(scenario.events_before_source_removal.has_value());
        EXPECT_TRUE(scenario.disk_from->existsFile(RenamingDiskMove::path_from));
    }
}

#endif
