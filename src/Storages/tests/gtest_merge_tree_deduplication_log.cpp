#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <map>

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

/// Wraps an already open file writer and throws on every sync() at or after the Nth,
/// simulating a disk that keeps failing to fsync the deduplication log (e.g. a
/// failing device), so that each rotation is rolled back and leaves more cancelled
/// records - and another log file - behind. The counter is shared across every writer
/// the owning disk creates.
class FailingFromNthSyncWriteBuffer : public WriteBufferFromFileDecorator
{
public:
    FailingFromNthSyncWriteBuffer(std::unique_ptr<WriteBufferFromFileBase> impl_, size_t & sync_count_, size_t fail_from_sync_)
        : WriteBufferFromFileDecorator(std::move(impl_)), sync_count(sync_count_), fail_from_sync(fail_from_sync_)
    {
    }

    void sync() override
    {
        ++sync_count;
        if (sync_count >= fail_from_sync)
            throw Exception(ErrorCodes::CANNOT_FSYNC, "Injected sync failure");
        WriteBufferFromFileDecorator::sync();
    }

private:
    size_t & sync_count;
    const size_t fail_from_sync;
};

/// A DiskLocal whose writers throw on every sync() at or after the Nth.
class DiskThrowingFromNthSync : public DiskLocal
{
public:
    DiskThrowingFromNthSync(const String & name_, const String & path_, size_t fail_from_sync_)
        : DiskLocal(name_, path_), fail_from_sync(fail_from_sync_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        return std::make_unique<FailingFromNthSyncWriteBuffer>(DiskLocal::writeFile(path, buf_size, mode, settings), sync_count, fail_from_sync);
    }

    size_t sync_count = 0;
    const size_t fail_from_sync;
};

/// Read the raw records of every deduplication log file under `logs_root`, in
/// chronological (log-number) order, without any of the rollback-pairing logic -
/// the way every server version reads them off the disk.
std::vector<MergeTreeDeduplicationLogRecord> readAllRecordsRaw(const std::string & logs_root)
{
    std::map<size_t, std::filesystem::path> logs;
    for (const auto & entry : std::filesystem::directory_iterator(logs_root))
    {
        const std::string stem = entry.path().stem();
        logs.emplace(std::stoull(stem.substr(stem.find_last_of('_') + 1)), entry.path());
    }

    std::vector<MergeTreeDeduplicationLogRecord> records;
    for (const auto & [log_number, path] : logs)
    {
        std::ifstream in(path);
        std::string line;
        while (std::getline(in, line))
        {
            const size_t first_tab = line.find('\t');
            const size_t second_tab = line.find('\t', first_tab + 1);
            EXPECT_NE(first_tab, std::string::npos) << "malformed record: " << line;
            EXPECT_NE(second_tab, std::string::npos) << "malformed record: " << line;
            MergeTreeDeduplicationLogRecord record;
            record.operation = static_cast<MergeTreeDeduplicationOp>(std::stoi(line.substr(0, first_tab)));
            record.part_name = line.substr(first_tab + 1, second_tab - first_tab - 1);
            record.block_id = line.substr(second_tab + 1);
            records.push_back(std::move(record));
        }
    }
    return records;
}

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

/// Regression test (after-restart variant of RotationFailureDoesNotEvictUnrelatedBlockIds):
/// a failed insert must not evict an unrelated, already-active block ID after a
/// server restart either. The compensating record for the rolled-back insert
/// replays after the ADD it undoes, so if it were a plain DROP the transient ADD
/// would still evict the oldest committed block from the bounded map on replay,
/// even though the failed insert never took effect in memory. It carries a
/// reserved part-name marker instead, so replay recognizes it and drops the
/// (ADD, DROP) pair entirely, never consuming a deduplication-window slot for it.
TEST(MergeTreeDeduplicationLog, RotationFailureDoesNotEvictUnrelatedBlockIdsAfterRestart)
{
    const std::string work_dir = "tmp/gtest_dedup_log_no_evict_restart/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        /// writeFile #1 happens while creating the very first log during load().
        /// A window of 1 means the map holds a single entry and rotate_interval == 2,
        /// so the ADD record for "block2" reaches the rotation that writeFile #2 is
        /// injected to fail.
        auto disk = std::make_shared<DiskThrowingOnNthWrite>("faulty", work_dir, /*fail_on_write=*/ 2);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        /// Publishes "block1" into the (now full) map and durably logs its ADD.
        log.addPart({"block1"}, part("all_1_1_0"));

        /// The ADD record for "block2" is written successfully, but the rotation
        /// that follows it is injected to fail, so the insert is rolled back.
        EXPECT_ANY_THROW(log.addPart({"block2"}, part("all_2_2_0")));

        /// Finalize the current log as on a graceful shutdown.
        log.shutdown();
    }

    {
        /// "Restart" with a healthy disk: replay the log from disk.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        /// "block1" must still be deduplicated after the restart: replaying the
        /// rolled-back "block2" must not have evicted it from the one-slot map.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_3_3_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}

/// Regression test (after-restart variant for the sync-failure rollback path):
/// when the ADD records of a failed insert did reach the disk (only the fsync of
/// the previous log file during rotation failed), replaying them on restart must
/// still not evict an unrelated, already-active block ID from the bounded map.
TEST(MergeTreeDeduplicationLog, RotationSyncFailureDoesNotEvictUnrelatedBlockIdsAfterRestart)
{
    const std::string work_dir = "tmp/gtest_dedup_log_no_evict_sync_restart/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        auto disk = std::make_shared<DiskThrowingOnNthSync>("faulty", work_dir, /*fail_on_sync=*/ 1);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        log.addPart({"block1"}, part("all_1_1_0"));

        /// The ADD record for "block2" is written and flushed, but syncing the old
        /// log file during the rotation fails, so the insert is rolled back with a
        /// rollback record written into the newly opened log file.
        EXPECT_ANY_THROW(log.addPart({"block2"}, part("all_2_2_0")));

        log.shutdown();
    }

    {
        /// "Restart" with a healthy disk: replay the logs from disk.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        /// "block1" must still be deduplicated: the rolled-back "block2" ADD (which
        /// did reach the disk) must be cancelled out on replay rather than evicting
        /// "block1" from the one-slot map.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_3_3_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}

/// Regression test: a failed multi-block insert whose rollback writes
/// compensating records must not shrink the retained log history across a
/// restart. Replaying the log correctly cancels the rolled-back record pairs, but
/// log retention (dropOutdatedLogs) sums per-file record counts to decide which
/// older logs are redundant. If those counts still include the cancelled pairs,
/// the first restart over-counts the rolled-back records, rotates, and drops the
/// older log that holds the committed block IDs - so a second restart replays
/// only the rollback-only log and forgets the committed inserts, wrongly
/// accepting their retries. The counts must therefore be recomputed from only the
/// surviving records, both in memory (after the failed insert) and after each
/// replay.
TEST(MergeTreeDeduplicationLog, RotationSyncFailureRetainsCommittedLogsAfterTwoRestarts)
{
    const std::string work_dir = "tmp/gtest_dedup_log_retention_two_restarts/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        /// deduplication_window == 2 gives rotate_interval == 4. The first rotation
        /// with a live previous writer is the one triggered by the four-block insert
        /// below, so sync #1 is the finalization of the log file holding all the
        /// committed and rolled-back ADD records.
        auto disk = std::make_shared<DiskThrowingOnNthSync>("faulty", work_dir, /*fail_on_sync=*/ 1);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        /// Two committed inserts fill the deduplication window.
        log.addPart({"block1"}, part("all_1_1_0"));
        log.addPart({"block2"}, part("all_2_2_0"));

        /// A four-block insert writes its ADD records, then the rotation that
        /// follows fails to sync the previous log file, so the insert is rolled back
        /// with four rollback records written into the freshly opened log file.
        EXPECT_ANY_THROW(log.addPart({"block3", "block4", "block5", "block6"}, part("all_3_3_0")));

        log.shutdown();
    }

    {
        /// First "restart" with a healthy disk: replay the logs from disk.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();
        log.shutdown();
    }

    {
        /// Second "restart": the log file holding "block1" / "block2" must not have
        /// been dropped by the first restart's retention pass, or the committed
        /// inserts are forgotten here and their retries wrongly accepted.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        /// Both committed blocks must still be deduplicated after two restarts.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_4_4_0")).empty());
        EXPECT_FALSE(log.addPart({"block2"}, part("all_5_5_0")).empty());

        /// The rolled-back blocks must still be retryable (never committed).
        EXPECT_TRUE(log.addPart({"block3"}, part("all_3_3_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}

/// Regression test: log rotation and compaction must be driven by the RAW number
/// of records physically written to a file, not by how many survive rollback-pair
/// elimination. A rolled-back operation writes records - its ADD/DROP records and
/// the compensating records that cancel them - that reconstruct nothing on replay,
/// so they consume no deduplication-window slot and must not count towards
/// retention (dropOutdatedLogs). But they are still bytes on disk, so if they also
/// did not count towards rotation, a log dominated by rolled-back pairs would never
/// reach the rotation threshold and could grow without bound (and load would then
/// have to materialize O(number of failures) records). Rotation must therefore use
/// the raw count and keep rotating such a log.
TEST(MergeTreeDeduplicationLog, RotationCountsRolledBackRecordsAsRawGrowth)
{
    const std::string work_dir = "tmp/gtest_dedup_log_raw_rotation/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const std::string logs_dir = work_dir + "dedup_logs";
    auto count_logs = [&]() -> size_t
    {
        return std::distance(std::filesystem::directory_iterator(logs_dir), std::filesystem::directory_iterator());
    };

    /// Fail only the first sync, which is the rotation triggered by the second add.
    auto disk = std::make_shared<DiskThrowingOnNthSync>("faulty", work_dir, /*fail_on_sync=*/ 1);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    /// deduplication_window == 1 gives rotate_interval == 2.
    MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
    log.load();

    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    /// load() created the first log file.
    ASSERT_EQ(count_logs(), 1u);

    /// First add does not rotate yet (one record, rotate_interval is two).
    log.addPart({"block1"}, part("all_1_1_0"));

    /// The ADD for "block2" reaches rotate_interval and triggers a rotation whose
    /// sync of the previous file fails: the insert is rolled back, and the
    /// compensating record is written into the freshly opened second log file. That
    /// file now holds one record that reconstructs nothing on replay.
    EXPECT_ANY_THROW(log.addPart({"block2"}, part("all_2_2_0")));
    EXPECT_EQ(count_logs(), 2u);

    /// A single committed insert now brings the second log file's RAW size to
    /// rotate_interval (its rollback record plus this ADD), so it must rotate into a
    /// third file. Counting only surviving records, the rollback record would not
    /// count, the file would stay below the threshold, no rotation would happen, and
    /// only two files would exist here - letting a rollback-heavy log grow unbounded.
    log.addPart({"block3"}, part("all_3_3_0"));
    EXPECT_EQ(count_logs(), 3u);

    std::filesystem::remove_all(work_dir);
}

/// Regression test: dropPart must apply to the in-memory map all-or-nothing, just
/// like addPart. It writes a DROP record per covered block id and then, since
/// rotate can now rethrow a failure to finalize or fsync the previous log file
/// (needed so addPart can trust that boundary), the rotation that follows may
/// throw partway through a multi-block drop. Erasing the block ids one at a time
/// before that boundary (as the code used to) left the map in a partial state -
/// the first covered block erased, the rest still published - which the caller
/// (StorageMergeTree::dropPartNoWaitNoThrow) never retries. Instead the whole drop
/// must fail atomically: every covered block id stays published, so a failed drop
/// never wrongly forgets a block id (which would let a later retry duplicate data).
TEST(MergeTreeDeduplicationLog, DropPartRotationSyncFailureIsAllOrNothing)
{
    const std::string work_dir = "tmp/gtest_dedup_log_drop_all_or_nothing/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    /// The deduplication log syncs a writer only in rotate, and no rotation with a
    /// live previous writer happens during the setup below, so sync #1 is exactly
    /// the finalization of the log file triggered by the drop.
    auto disk = std::make_shared<DiskThrowingOnNthSync>("faulty", work_dir, /*fail_on_sync=*/ 1);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    /// deduplication_window == 2 gives rotate_interval == 4.
    MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
    log.load();

    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    /// Bring the current log to three records (one short of rotate_interval) while
    /// leaving only "block1" and "block2" in the two-slot map: the throwaway insert
    /// gets evicted by "block2" but its ADD record still counts. That way the DROP
    /// of "block1" is the record that reaches rotate_interval and triggers the
    /// rotation - so the old, one-block-at-a-time code would rotate (and fail) right
    /// after erasing "block1" but before handling "block2".
    log.addPart({"throwaway"}, part("all_5_5_0"));
    log.addPart({"block1"}, part("all_1_1_0"));
    log.addPart({"block2"}, part("all_2_2_0"));

    /// Drop a range covering both "block1" and "block2". The rotation that the DROP
    /// records trigger fails to sync the previous log file and rethrows.
    EXPECT_ANY_THROW(log.dropPart(part("all_0_9_999")));

    /// The drop failed as a whole, so neither block id may have been erased: a retry
    /// of either must still be deduplicated. The old code erased "block1" before the
    /// rotation threw, so it would wrongly accept a retry of "block1" here.
    EXPECT_FALSE(log.addPart({"block1"}, part("all_6_6_0")).empty());
    EXPECT_FALSE(log.addPart({"block2"}, part("all_7_7_0")).empty());

    /// The log must still be usable: the rotation switched over to the new writer
    /// before propagating the failure, so a fresh insert must not abort.
    EXPECT_NO_THROW(log.addPart({"block8"}, part("all_8_8_0")));

    std::filesystem::remove_all(work_dir);
}

/// Regression test: dropPart must stay all-or-nothing across a restart when one
/// of its DROP records fails to write partway through a multi-block drop.
/// writeRecord flushes every record, so when the write of the second DROP fails,
/// the first DROP is already durable while no block id has been erased from the
/// in-memory map (the live, all-published state) - and the caller
/// (StorageMergeTree::dropPartNoWaitNoThrow) never retries the drop. Without a
/// rollback, replaying that one-record prefix on startup erases only the first
/// block id: a retry of it is then wrongly accepted (duplicating data) while the
/// sibling block still deduplicates. The rollback must write a compensating
/// CANCEL for the durable DROP prefix - to a fresh writer, since the failed
/// write cancelled the current one - so a replay keeps every covered block id
/// published, matching the live map.
TEST(MergeTreeDeduplicationLog, DropPartWriteFailureIsAllOrNothingAfterRestart)
{
    const std::string work_dir = "tmp/gtest_dedup_log_drop_write_failure/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        /// A large deduplication window keeps the log from rotating on its own, so
        /// the same writer stays open across the calls below and the injected
        /// failure hits a write into the already open file. Flushes #1 and #2 are
        /// the ADD records of the two committed inserts; the drop below then writes
        /// its DROP records in the map's insertion order, so flush #3 is the DROP
        /// for "block1" (succeeds, durable) and flush #4 is the DROP for "block2"
        /// (injected to fail, cancelling the writer mid-batch).
        auto disk = std::make_shared<DiskThrowingOnNthFlush>("faulty", work_dir, /*fail_on_flush=*/ 4);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 10, format_version, disk);
        log.load();

        log.addPart({"block1"}, part("all_1_1_0"));
        log.addPart({"block2"}, part("all_2_2_0"));

        /// Drop a range covering both "block1" and "block2"; the write of the
        /// second DROP record fails after the first one is already durable.
        EXPECT_ANY_THROW(log.dropPart(part("all_0_9_999")));

        /// The drop failed as a whole, so neither block id may have been erased:
        /// a retry of either must still be deduplicated.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_3_3_0")).empty());
        EXPECT_FALSE(log.addPart({"block2"}, part("all_4_4_0")).empty());

        /// The log must still be usable (the rollback rotated to a fresh writer;
        /// a canceled writer silently discards writes, so without the rotation
        /// this record would never become durable).
        EXPECT_NO_THROW(log.addPart({"block3"}, part("all_5_5_0")));

        /// Finalize the current log as on a graceful shutdown.
        log.shutdown();
    }

    {
        /// "Restart" with a healthy disk: replay the logs from disk.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 10, format_version, disk);
        log.load();

        /// Both covered block ids must still be deduplicated after the restart:
        /// the durable DROP prefix for "block1" must have been cancelled out by
        /// the rollback's compensating record rather than erase "block1" alone.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_6_6_0")).empty());
        EXPECT_FALSE(log.addPart({"block2"}, part("all_7_7_0")).empty());

        /// The insert written after the failed drop survives the restart too.
        EXPECT_FALSE(log.addPart({"block3"}, part("all_8_8_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}

/// The rollback records the failure paths above leave on disk must stay safe to
/// replay for servers from BEFORE these records existed - a downgrade and restart
/// with such records already in the logs. An old server replays every record as
/// `DROP` = erase and anything else = insert (parsing the part name). The
/// rollback of a failed insert is therefore encoded as a DROP (with a reserved
/// part-name marker only newer servers interpret): an old server then erases the
/// never-committed block id, so a client retry of the failed insert is accepted -
/// encoding it as an op unknown to old servers would replay as an insert that
/// keeps the block id published and silently drops the retry's data. The rollback
/// of a failed drop is a CANCEL carrying the real, parseable part name: an old
/// server replays it as the insert that restores the block id, which is exactly
/// the rollback's net effect.
TEST(MergeTreeDeduplicationLog, RollbackRecordsReplaySafelyOnOlderServers)
{
    const std::string work_dir = "tmp/gtest_dedup_log_downgrade/";

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        std::filesystem::remove_all(work_dir);
        std::filesystem::create_directories(work_dir);

        /// A failed insert: the ADD record for "block2" reaches the disk, but the
        /// fsync of the previous log file during the rotation that follows fails,
        /// so the insert is rolled back.
        auto disk = std::make_shared<DiskThrowingOnNthSync>("faulty", work_dir, /*fail_on_sync=*/ 1);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();
        log.addPart({"block1"}, part("all_1_1_0"));
        EXPECT_ANY_THROW(log.addPart({"block2"}, part("all_2_2_0")));
        log.shutdown();

        /// An old server replaying these logs must not consider the rolled-back,
        /// never-committed "block2" published: it would wrongly deduplicate - and
        /// silently drop - a client retry of the failed insert. (Losing "block1"
        /// to the transient ADD's window slot matches what the old server's own
        /// code produced on this failure path; only keeping "block2" published
        /// would be a new, data-dropping regression.)
        LimitedOrderedHashMap<MergeTreePartInfo> map(/*max_size=*/ 1);
        for (const auto & record : readAllRecordsRaw(work_dir + "dedup_logs"))
        {
            if (record.operation == MergeTreeDeduplicationOp::DROP)
                map.erase(record.block_id);
            else
                map.insert(record.block_id, MergeTreePartInfo::fromPartName(record.part_name, format_version));
        }
        EXPECT_FALSE(map.contains("block2"));
    }

    {
        std::filesystem::remove_all(work_dir);
        std::filesystem::create_directories(work_dir);

        /// A failed drop: the DROP record for "block1" is durable when the write
        /// of the DROP for "block2" fails (flushes #1-#2 are the two ADD records,
        /// #3 the first DROP, #4 - injected to fail - the second), so the whole
        /// drop is rolled back and both block ids stay published.
        auto disk = std::make_shared<DiskThrowingOnNthFlush>("faulty", work_dir, /*fail_on_flush=*/ 4);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 10, format_version, disk);
        log.load();
        log.addPart({"block1"}, part("all_1_1_0"));
        log.addPart({"block2"}, part("all_2_2_0"));
        EXPECT_ANY_THROW(log.dropPart(part("all_0_9_999")));
        log.shutdown();

        /// An old server replaying these logs must still consider both block ids
        /// published, matching the rolled-back (all-or-nothing) live state: the
        /// CANCEL replays there as an insert with the real part name, restoring
        /// "block1" after the durable DROP prefix erased it.
        LimitedOrderedHashMap<MergeTreePartInfo> map(/*max_size=*/ 10);
        for (const auto & record : readAllRecordsRaw(work_dir + "dedup_logs"))
        {
            if (record.operation == MergeTreeDeduplicationOp::DROP)
                map.erase(record.block_id);
            else
                map.insert(record.block_id, MergeTreePartInfo::fromPartName(record.part_name, format_version));
        }
        EXPECT_TRUE(map.contains("block1"));
        EXPECT_TRUE(map.contains("block2"));
    }

    std::filesystem::remove_all(work_dir);
}

/// Unit test for the LimitedOrderedHashMap primitive addPart relies on to publish
/// block IDs exception-safely. `insertWithoutEviction` must add an entry without
/// dropping any existing one - so a rollback needs only the non-allocating `erase` -
/// and `trimToMaxSize` must then evict the oldest entries down to the limit, together
/// reproducing exactly what a plain evicting `insert` does but with every allocation
/// moved to the first, still-rollback-able step. The index keys are string_views into
/// the queue nodes, so the test also checks that lookups keep working across all of
/// these operations.
TEST(MergeTreeDeduplicationLog, LimitedOrderedHashMapInsertWithoutEvictionThenTrim)
{
    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    LimitedOrderedHashMap<MergeTreePartInfo> map(/*max_size=*/ 2);

    /// Fill to capacity with an evicting insert.
    EXPECT_TRUE(map.insert("block1", part("all_1_1_0")));
    EXPECT_TRUE(map.insert("block2", part("all_2_2_0")));
    EXPECT_EQ(map.size(), 2u);

    /// insertWithoutEviction adds a third entry WITHOUT dropping the oldest, so the
    /// map temporarily exceeds its limit and every key - including the oldest - is
    /// still present and looked up correctly.
    EXPECT_TRUE(map.insertWithoutEviction("block3", part("all_3_3_0")));
    EXPECT_EQ(map.size(), 3u);
    EXPECT_TRUE(map.contains("block1"));
    EXPECT_TRUE(map.contains("block2"));
    EXPECT_TRUE(map.contains("block3"));
    EXPECT_EQ(map.get("block1"), part("all_1_1_0"));

    /// A key already present is not inserted again.
    EXPECT_FALSE(map.insertWithoutEviction("block3", part("all_9_9_0")));
    EXPECT_EQ(map.size(), 3u);

    /// trimToMaxSize drops the oldest entries (FIFO) down to the limit.
    map.trimToMaxSize();
    EXPECT_EQ(map.size(), 2u);
    EXPECT_FALSE(map.contains("block1"));
    EXPECT_TRUE(map.contains("block2"));
    EXPECT_TRUE(map.contains("block3"));

    /// Erasing a published entry cannot fail and leaves the rest intact.
    EXPECT_TRUE(map.erase("block2"));
    EXPECT_FALSE(map.erase("block2"));
    EXPECT_EQ(map.size(), 1u);
    EXPECT_TRUE(map.contains("block3"));

    /// A plain insert still evicts to keep within the limit.
    EXPECT_TRUE(map.insert("block4", part("all_4_4_0")));
    EXPECT_EQ(map.size(), 2u);
    EXPECT_TRUE(map.insert("block5", part("all_5_5_0")));
    EXPECT_EQ(map.size(), 2u);
    EXPECT_FALSE(map.contains("block3"));
    EXPECT_TRUE(map.contains("block4"));
    EXPECT_TRUE(map.contains("block5"));
    EXPECT_EQ(map.get("block5"), part("all_5_5_0"));
}

/// Regression test for the success path of the exception-safe publication: after the
/// durable writes succeed, addPart must still enforce the deduplication window by
/// evicting the oldest block IDs, exactly as the old evicting insert did. A block
/// pushed out of the window stops deduplicating, while the ones still within it keep
/// doing so - so splitting publication into insertWithoutEviction + trimToMaxSize
/// must not change the observable windowing behavior.
TEST(MergeTreeDeduplicationLog, AddPartEnforcesWindowOnSuccess)
{
    const std::string work_dir = "tmp/gtest_dedup_log_window_success/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
    log.load();

    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    log.addPart({"block1"}, part("all_1_1_0"));
    log.addPart({"block2"}, part("all_2_2_0"));

    /// Both are within the two-slot window, so their retries deduplicate (and, being
    /// deduplicated, do not change the window).
    EXPECT_FALSE(log.addPart({"block1"}, part("all_9_9_0")).empty());
    EXPECT_FALSE(log.addPart({"block2"}, part("all_9_9_0")).empty());

    /// A third distinct block pushes the oldest ("block1") out of the window.
    log.addPart({"block3"}, part("all_3_3_0"));

    /// "block2" and "block3" are still within the window and deduplicate.
    EXPECT_FALSE(log.addPart({"block2"}, part("all_9_9_0")).empty());
    EXPECT_FALSE(log.addPart({"block3"}, part("all_9_9_0")).empty());

    /// "block1" was evicted by the trim after "block3" committed, so its retry is
    /// accepted again rather than deduplicated.
    EXPECT_TRUE(log.addPart({"block1"}, part("all_4_4_0")).empty());

    std::filesystem::remove_all(work_dir);
}

/// Regression test: rolled-back operations leave (ADD, rollback) record pairs that
/// cancel out on replay but that dropOutdatedLogs cannot reclaim - the rollback
/// record sits in a newer file while the record it cancels sits in an older file
/// still retained for other, live block ids, and retention only drops an oldest
/// prefix. Under repeated transient failures these pairs, and the files holding
/// them, would accumulate without bound, so every restart would replay O(number of
/// failures) records. compact() must rewrite the live state into a single fresh log
/// file so that both the retained files and the replay stay bounded by the
/// deduplication window - while preserving the live deduplication state exactly.
TEST(MergeTreeDeduplicationLog, RepeatedRollbacksAreCompactedAwayOnRestart)
{
    const std::string work_dir = "tmp/gtest_dedup_log_compaction/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const std::string logs_dir = work_dir + "dedup_logs";
    auto count_logs = [&]() -> size_t
    {
        return std::distance(std::filesystem::directory_iterator(logs_dir), std::filesystem::directory_iterator());
    };

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        /// A disk that fails every fsync. deduplication_window == 1 gives
        /// rotate_interval == 2, so the second record written into a file triggers a
        /// rotation, whose fsync of the previous file fails and rolls the insert back
        /// with a compensating record written into the freshly opened file.
        auto disk = std::make_shared<DiskThrowingFromNthSync>("faulty", work_dir, /*fail_from_sync=*/ 1);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        /// One committed insert.
        log.addPart({"block1"}, part("all_1_1_0"));

        /// Several failed inserts. Each writes an ADD that trips the rotation, whose
        /// fsync fails; the insert is rolled back, leaving an (ADD, rollback) pair and
        /// a new log file behind. dropOutdatedLogs keeps them all: they hold no live
        /// coverage of their own, yet cancel an ADD in the still-retained "block1"
        /// file, so they are neither the oldest nor droppable.
        for (int i = 2; i <= 8; ++i)
        {
            const std::string suffix = std::to_string(i) + "_" + std::to_string(i) + "_0";
            EXPECT_ANY_THROW(log.addPart({"block" + std::to_string(i)}, part("all_" + suffix)));
        }

        /// The garbage really did pile up: many small files, none reclaimable.
        EXPECT_GT(count_logs(), 4u);

        log.shutdown();
    }

    {
        /// Restart with a healthy disk. load() replays every file, reconstructs the
        /// single live block, and then compacts the accumulated garbage away.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        /// Compaction rewrote the live state into a single fresh log file; without it
        /// every rolled-back file would still be here and load would keep replaying
        /// all of them on every future restart.
        EXPECT_EQ(count_logs(), 1u);

        /// The committed block still deduplicates, and a never-committed block is
        /// still retryable - compaction preserved the live state exactly.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_9_9_0")).empty());
        EXPECT_TRUE(log.addPart({"block2"}, part("all_10_10_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}

/// The same repeated-rollback growth must also be bounded on a disk without append
/// support (e.g. `s3_plain_rewritable`), the very regime the original bug reproduced
/// on. There every operation rotates into a fresh file, so the accumulated rollback
/// pairs cannot be reopened-and-appended-over; compaction must instead write the live
/// snapshot to a fresh durable file and start the next operation in another fresh file.
/// Without the non-append compaction path these files (and the load-time replay) would
/// grow with the number of failures on every restart.
TEST(MergeTreeDeduplicationLog, RepeatedRollbacksAreCompactedAwayOnRestartWithoutAppendSupport)
{
    const std::string work_dir = "tmp/gtest_dedup_log_compaction_no_append/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const std::string logs_dir = work_dir + "dedup_logs";
    auto count_logs = [&]() -> size_t
    {
        return std::distance(std::filesystem::directory_iterator(logs_dir), std::filesystem::directory_iterator());
    };

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        /// A disk that fails every fsync at or after the second. Without append support
        /// every operation rotates and thus fsyncs the previous file, so the first
        /// insert's rotation (fsync #1) succeeds and commits it, while the second and
        /// every later insert's rotation (fsync #2+) fails and rolls the insert back,
        /// leaving an (ADD, rollback) pair and a new file behind.
        auto disk = std::make_shared<DiskThrowingFromNthSync>("faulty", work_dir, /*fail_from_sync=*/ 2);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.simulateDiskWithoutWritingWithAppendSupportForTests();
        log.load();

        /// One committed insert.
        log.addPart({"block1"}, part("all_1_1_0"));

        /// Several failed inserts. Each rolls back and leaves a new log file behind that
        /// dropOutdatedLogs cannot reclaim (its rollback record cancels an ADD in the
        /// still-retained "block1" file, so it holds no live coverage of its own).
        for (int i = 2; i <= 8; ++i)
        {
            const std::string suffix = std::to_string(i) + "_" + std::to_string(i) + "_0";
            EXPECT_ANY_THROW(log.addPart({"block" + std::to_string(i)}, part("all_" + suffix)));
        }

        /// The garbage really did pile up: many small files, none reclaimable.
        EXPECT_GT(count_logs(), 4u);

        log.shutdown();
    }

    {
        /// Restart with a healthy disk, still without append support. load() replays every
        /// file, reconstructs the single live block, and then compacts the accumulated
        /// garbage away by rewriting the live state into a fresh durable snapshot file and
        /// starting the next operation in another fresh file.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.simulateDiskWithoutWritingWithAppendSupportForTests();
        log.load();

        /// Compaction left just the snapshot file plus the fresh, empty file the next
        /// operation writes to - not one file per past failure.
        EXPECT_EQ(count_logs(), 2u);

        /// The committed block still deduplicates, and a never-committed block is still
        /// retryable - compaction preserved the live state exactly.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_9_9_0")).empty());
        EXPECT_TRUE(log.addPart({"block2"}, part("all_10_10_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}
