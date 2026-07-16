#include <gtest/gtest.h>

#include <filesystem>

#include <Disks/DiskLocal.h>
#include <Disks/WriteMode.h>
#include <IO/SwapHelper.h>
#include <IO/WriteBufferFromFileDecorator.h>
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
    extern const int CANNOT_FSYNC;
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

/// Wraps an already open file writer and throws on a chosen `next()` (flush) call,
/// simulating a transient I/O failure while writing to it - as opposed to failing
/// while opening a new file during rotation. The flush counter is shared across
/// every writer the owning disk creates (passed in by reference), so the fault
/// fires exactly once overall, even though rolling back a failed insert opens a
/// fresh writer via `rotate`.
class FailingOnNthFlushWriteBuffer : public WriteBufferFromFileDecorator
{
public:
    FailingOnNthFlushWriteBuffer(std::unique_ptr<WriteBufferFromFileBase> impl_, size_t & flush_count_, size_t fail_on_flush_)
        : WriteBufferFromFileDecorator(std::move(impl_)), flush_count(flush_count_), fail_on_flush(fail_on_flush_)
    {
    }

private:
    void nextImpl() override
    {
        ++flush_count;
        if (flush_count == fail_on_flush)
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Injected write failure");
        /// Same delegation `WriteBufferFromFileDecorator::nextImpl` does, inlined
        /// here because that method is private in the base class.
        SwapHelper swap(*this, *impl);
        impl->next();
    }

    size_t & flush_count;
    const size_t fail_on_flush;
};

/// A DiskLocal whose writer throws on a chosen `next()` (flush) call, simulating a
/// transient I/O failure while writing a record to the currently open deduplication
/// log file. The count is shared across all writers this disk creates (including
/// ones opened by `rotate` while rolling back a failed insert), so the fault fires
/// exactly once for the disk's lifetime.
class DiskThrowingOnNthFlush : public DiskLocal
{
public:
    DiskThrowingOnNthFlush(const String & name_, const String & path_, size_t fail_on_flush_)
        : DiskLocal(name_, path_), fail_on_flush(fail_on_flush_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        return std::make_unique<FailingOnNthFlushWriteBuffer>(DiskLocal::writeFile(path, buf_size, mode, settings), flush_count, fail_on_flush);
    }

    size_t flush_count = 0;
    const size_t fail_on_flush;
};

/// Wraps an already open file writer and throws on a chosen `sync()` call,
/// simulating an fsync failure while finalizing the previous log file during
/// rotation - after all the writes to it succeeded. The counter is shared across
/// every writer the owning disk creates, so the fault fires exactly once overall.
class FailingOnNthSyncWriteBuffer : public WriteBufferFromFileDecorator
{
public:
    FailingOnNthSyncWriteBuffer(std::unique_ptr<WriteBufferFromFileBase> impl_, size_t & sync_count_, size_t fail_on_sync_)
        : WriteBufferFromFileDecorator(std::move(impl_)), sync_count(sync_count_), fail_on_sync(fail_on_sync_)
    {
    }

    void sync() override
    {
        ++sync_count;
        if (sync_count == fail_on_sync)
            throw Exception(ErrorCodes::CANNOT_FSYNC, "Injected sync failure");
        WriteBufferFromFileDecorator::sync();
    }

private:
    size_t & sync_count;
    const size_t fail_on_sync;
};

/// A DiskLocal whose writer throws on a chosen `sync()` call. The deduplication
/// log syncs a writer only when rotating away from it, so this simulates losing
/// the just-written records of the previous log file to an fsync failure while
/// the rotation itself (creating the new file) succeeds.
class DiskThrowingOnNthSync : public DiskLocal
{
public:
    DiskThrowingOnNthSync(const String & name_, const String & path_, size_t fail_on_sync_)
        : DiskLocal(name_, path_), fail_on_sync(fail_on_sync_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        return std::make_unique<FailingOnNthSyncWriteBuffer>(DiskLocal::writeFile(path, buf_size, mode, settings), sync_count, fail_on_sync);
    }

    size_t sync_count = 0;
    const size_t fail_on_sync;
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

/// Regression test: a failure while writing one of the ADD records to the
/// currently open log file (as opposed to a failure while rotating to a new
/// file) cancels `current_writer`, which then refuses further writes. The
/// rollback must rotate to a fresh writer before it can persist the compensating
/// DROP records for the block IDs that were already published.
TEST(MergeTreeDeduplicationLog, WriteFailureRollsBackPublishedBlockIds)
{
    const std::string work_dir = "tmp/gtest_dedup_log_write_failure/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    /// A large deduplication window keeps the log from rotating on its own, so the
    /// same writer stays open across the calls below and the injected failure hits
    /// a write into the already open file, not the creation of a new one.
    auto disk = std::make_shared<DiskThrowingOnNthFlush>("faulty", work_dir, /*fail_on_flush=*/ 3);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 10, format_version, disk);
    log.load();

    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    /// Flush #1: succeeds.
    log.addPart({"block1"}, part("all_1_1_0"));

    /// Flush #2 (for "block2") succeeds, flush #3 (for "block3") is injected to fail:
    /// "block2" was already published when the insert of "block3" aborts it.
    EXPECT_ANY_THROW(log.addPart({"block2", "block3"}, part("all_2_2_0")));

    /// Both must be retryable: "block2" because it got rolled back, "block3"
    /// because it never got published in the first place.
    EXPECT_TRUE(log.addPart({"block2"}, part("all_2_2_0")).empty());
    EXPECT_TRUE(log.addPart({"block3"}, part("all_2_2_0")).empty());

    /// The log must still be usable (the rollback rotated to a fresh writer).
    EXPECT_NO_THROW(log.addPart({"block2", "block3"}, part("all_2_2_0")));

    /// And now that the retry has committed, it deduplicates as usual.
    EXPECT_FALSE(log.addPart({"block2"}, part("all_4_4_0")).empty());

    std::filesystem::remove_all(work_dir);
}

/// Regression test: a failed insert must not evict unrelated, already-active
/// block IDs from the in-memory deduplication map. `LimitedOrderedHashMap::insert`
/// evicts the oldest entry once the map is at capacity, and that eviction is not
/// undone by rolling back only the block IDs the failed call itself published, so
/// publishing into the map must be deferred until the whole insert has durably
/// succeeded (including the rotation that follows the writes).
TEST(MergeTreeDeduplicationLog, RotationFailureDoesNotEvictUnrelatedBlockIds)
{
    const std::string work_dir = "tmp/gtest_dedup_log_no_evict/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    /// writeFile #1 happens while creating the very first log during load().
    /// A window of 1 means the map holds a single entry and rotate_interval == 2,
    /// so the ADD record for "block2" below reaches the rotation that writeFile #2
    /// is injected to fail.
    auto disk = std::make_shared<DiskThrowingOnNthWrite>("faulty", work_dir, /*fail_on_write=*/ 2);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
    log.load();

    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    /// Publishes "block1" into the (now full) map.
    log.addPart({"block1"}, part("all_1_1_0"));

    /// The ADD record for "block2" is written successfully, but the rotation that
    /// follows it is injected to fail. In the buggy version, "block2" would have
    /// already been inserted into the full map by this point, evicting "block1".
    EXPECT_ANY_THROW(log.addPart({"block2"}, part("all_2_2_0")));

    /// "block1" must still be deduplicated: the failed insert of "block2" must not
    /// have evicted it from the map before the insert was known to succeed.
    EXPECT_FALSE(log.addPart({"block1"}, part("all_3_3_0")).empty());

    std::filesystem::remove_all(work_dir);
}

/// Regression test: block IDs rolled back after a write failure (as opposed to a
/// rotation failure) must not survive a server restart either.
TEST(MergeTreeDeduplicationLog, WriteFailureRollsBackPublishedBlockIdsAfterRestart)
{
    const std::string work_dir = "tmp/gtest_dedup_log_write_failure_rollback/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        auto disk = std::make_shared<DiskThrowingOnNthFlush>("faulty", work_dir, /*fail_on_flush=*/ 3);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 10, format_version, disk);
        log.load();

        log.addPart({"block1"}, part("all_1_1_0"));

        /// "block2" gets published, then the write for "block3" fails and rolls
        /// "block2" back.
        EXPECT_ANY_THROW(log.addPart({"block2", "block3"}, part("all_2_2_0")));

        /// Finalize the current log as on a graceful shutdown.
        log.shutdown();
    }

    {
        /// "Restart" with a healthy disk: replay the log from disk.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 10, format_version, disk);
        log.load();

        /// The rolled back "block2" must not have been re-published by the replay:
        /// the retry of the failed insert must be accepted...
        EXPECT_TRUE(log.addPart({"block2", "block3"}, part("all_2_2_0")).empty());

        /// ...and only then deduplicate as usual.
        EXPECT_FALSE(log.addPart({"block2"}, part("all_3_3_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}

/// Regression test: a failure to finalize or sync the previous log file during
/// rotation means the ADD records just written to it may never have reached
/// durable storage, so the insert must fail (and roll back) rather than report
/// success. Otherwise the part is committed, but after a restart the
/// deduplication log has forgotten it, and a retry of the same insert is
/// wrongly accepted and duplicates the data.
TEST(MergeTreeDeduplicationLog, RotationSyncFailureFailsInsert)
{
    const std::string work_dir = "tmp/gtest_dedup_log_sync_failure/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    /// The deduplication log syncs a writer only in `rotate`, and the first
    /// rotation with a live previous writer is the one triggered by the second
    /// add below, so sync #1 is exactly the finalization of the log file holding
    /// the ADD record for "block2".
    auto disk = std::make_shared<DiskThrowingOnNthSync>("faulty", work_dir, /*fail_on_sync=*/ 1);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    /// deduplication_window == 1 gives rotate_interval == 2, so the log rotates quickly.
    MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
    log.load();

    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    /// First add does not rotate yet.
    log.addPart({"block1"}, part("all_1_1_0"));

    /// The ADD record for "block2" is written, but syncing the old log file
    /// during the rotation that follows fails: the record may be lost, so the
    /// insert must fail instead of being treated as durably recorded.
    EXPECT_ANY_THROW(log.addPart({"block2"}, part("all_2_2_0")));

    /// The failed insert must not have published "block2": the caller aborts
    /// the insert before the part is committed, so a client retry of the same
    /// block must be accepted, not deduplicated against a part that never
    /// became active.
    EXPECT_TRUE(log.addPart({"block2"}, part("all_2_2_0")).empty());

    /// And now that the retry has committed the block, it deduplicates as usual.
    EXPECT_FALSE(log.addPart({"block2"}, part("all_4_4_0")).empty());

    /// The log must still be usable: the rotation switched over to the new
    /// writer before propagating the failure.
    EXPECT_NO_THROW(log.addPart({"block3"}, part("all_3_3_0")));

    std::filesystem::remove_all(work_dir);
}

/// Regression test: the rollback of an insert that failed on syncing the old
/// log file during rotation must survive a server restart. The compensating
/// DROP records go to the newly opened log file, so replaying the logs must not
/// re-publish the rolled back block IDs even when the original ADD records did
/// reach the disk (in this test the injected failure is only in `sync`, so
/// they always do).
TEST(MergeTreeDeduplicationLog, RotationSyncFailureRollsBackAfterRestart)
{
    const std::string work_dir = "tmp/gtest_dedup_log_sync_failure_rollback/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        auto disk = std::make_shared<DiskThrowingOnNthSync>("faulty", work_dir, /*fail_on_sync=*/ 1);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        log.addPart({"block1"}, part("all_1_1_0"));

        /// Syncing the old log file fails during the rotation, so the insert of
        /// "block2" is aborted and rolled back.
        EXPECT_ANY_THROW(log.addPart({"block2"}, part("all_2_2_0")));

        /// Finalize the current log as on a graceful shutdown.
        log.shutdown();
    }

    {
        /// "Restart" with a healthy disk: replay the log from disk.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        /// The rolled back "block2" must not have been re-published by the
        /// replay: the retry of the failed insert must be accepted...
        EXPECT_TRUE(log.addPart({"block2"}, part("all_2_2_0")).empty());

        /// ...and only then deduplicate as usual.
        EXPECT_FALSE(log.addPart({"block2"}, part("all_3_3_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}
