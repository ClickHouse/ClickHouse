#include <gtest/gtest.h>

#include <filesystem>

#include <Disks/DiskLocal.h>
#include <Disks/WriteMode.h>
#include <IO/WriteSettings.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/MergeTreeDeduplicationLog.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
}
}

namespace
{

/// A DiskLocal that throws on a chosen `writeFile` call, simulating a transient
/// I/O failure (e.g. an injected memory fault) during deduplication-log rotation.
class DiskThrowingOnNthWrite : public DiskLocal
{
public:
    DiskThrowingOnNthWrite(const String & name_, const String & path_, size_t fail_on_write_)
        : DiskLocal(name_, path_), fail_on_write(fail_on_write_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        ++write_count;
        if (write_count == fail_on_write)
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Injected write failure");
        return DiskLocal::writeFile(path, buf_size, mode, settings);
    }

    size_t write_count = 0;
    const size_t fail_on_write;
};

}

/// Regression test: a failure while rotating the deduplication log (creating the
/// new log file) must not leave `current_writer` pointing at a finalized buffer.
/// Otherwise a subsequent write - e.g. from the background cleanup thread calling
/// dropPart - aborts with the "Cannot write to finalized buffer" logical error.
TEST(MergeTreeDeduplicationLog, RotationFailureKeepsLogUsable)
{
    const std::string work_dir = "tmp/gtest_dedup_log/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    /// writeFile #1 happens while creating the very first log during load().
    /// We inject a failure into writeFile #2, which is the rotation that runs after
    /// records have been written: at that point the previous writer gets finalized
    /// and a new one is created, so the failure reproduces the broken state.
    auto disk = std::make_shared<DiskThrowingOnNthWrite>("faulty", work_dir, /*fail_on_write=*/ 2);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    /// deduplication_window == 1 gives rotate_interval == 2, so the log rotates quickly.
    MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
    log.load();

    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    /// First add does not rotate yet.
    log.addPart({"block1"}, part("all_1_1_0"));

    /// This add reaches rotate_interval and triggers a rotation whose writeFile is injected to fail.
    EXPECT_ANY_THROW(log.addPart({"block2"}, part("all_2_2_0")));

    /// The failed insert must not have left "block2" published: the caller aborts
    /// the insert before the part is committed, so a client retry of the same block
    /// must be accepted, not deduplicated against a part that never became active.
    EXPECT_TRUE(log.addPart({"block2"}, part("all_2_2_0")).empty());

    /// And now that the retry has committed the block, it deduplicates as usual.
    EXPECT_FALSE(log.addPart({"block2"}, part("all_4_4_0")).empty());

    /// After the failed rotation the previous writer must still be live, so this
    /// must not abort with "Cannot write to finalized buffer".
    EXPECT_NO_THROW(log.addPart({"block3"}, part("all_3_3_0")));

    /// Dropping a part writes DROP records too; it must also stay usable.
    EXPECT_NO_THROW(log.dropPart(part("all_3_3_0")));

    std::filesystem::remove_all(work_dir);
}

/// Regression test: block IDs published by an insert that failed on log rotation
/// must not survive a server restart either. The rollback writes compensating DROP
/// records into the still-live writer, so replaying the log on startup must not
/// re-publish them and a retry of the failed insert must be accepted.
TEST(MergeTreeDeduplicationLog, RotationFailureRollsBackPublishedBlockIds)
{
    const std::string work_dir = "tmp/gtest_dedup_log_rollback/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        auto disk = std::make_shared<DiskThrowingOnNthWrite>("faulty", work_dir, /*fail_on_write=*/ 2);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        log.addPart({"block1"}, part("all_1_1_0"));

        /// The rotation fails, so the insert of "block2" is aborted and rolled back.
        EXPECT_ANY_THROW(log.addPart({"block2"}, part("all_2_2_0")));

        /// Finalize the current log as on a graceful shutdown.
        log.shutdown();
    }

    {
        /// "Restart" with a healthy disk: replay the log from disk.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        /// The rolled back "block2" must not have been re-published by the replay:
        /// the retry of the failed insert must be accepted...
        EXPECT_TRUE(log.addPart({"block2"}, part("all_2_2_0")).empty());

        /// ...and only then deduplicate as usual.
        EXPECT_FALSE(log.addPart({"block2"}, part("all_3_3_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}
