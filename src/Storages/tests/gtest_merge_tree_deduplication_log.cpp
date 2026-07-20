#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <unordered_set>

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
    extern const int CANNOT_UNLINK;
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

/// Wraps an already open file writer and fails once on a chosen sync() and once on a
/// chosen flush (next()). Reproduces a rotation failure (the sync of the previous file)
/// followed by a failure partway through writing the compensating rollback records (the
/// flush), so the rollback itself is interrupted after persisting only some of its
/// records. Both counters are shared across every writer the owning disk creates, so
/// each fault fires exactly once overall.
class FailingOnNthSyncAndNthFlushWriteBuffer : public WriteBufferFromFileDecorator
{
public:
    FailingOnNthSyncAndNthFlushWriteBuffer(
        std::unique_ptr<WriteBufferFromFileBase> impl_,
        size_t & sync_count_, size_t fail_on_sync_,
        size_t & flush_count_, size_t fail_on_flush_)
        : WriteBufferFromFileDecorator(std::move(impl_))
        , sync_count(sync_count_), fail_on_sync(fail_on_sync_)
        , flush_count(flush_count_), fail_on_flush(fail_on_flush_)
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
    void nextImpl() override
    {
        ++flush_count;
        if (flush_count == fail_on_flush)
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Injected write failure");
        /// Same delegation `WriteBufferFromFileDecorator::nextImpl` does, inlined here
        /// because that method is private in the base class.
        SwapHelper swap(*this, *impl);
        impl->next();
    }

    size_t & sync_count;
    const size_t fail_on_sync;
    size_t & flush_count;
    const size_t fail_on_flush;
};

/// A DiskLocal whose writers fail once on the Nth sync() and once on the Mth flush.
class DiskThrowingOnNthSyncAndNthFlush : public DiskLocal
{
public:
    DiskThrowingOnNthSyncAndNthFlush(const String & name_, const String & path_, size_t fail_on_sync_, size_t fail_on_flush_)
        : DiskLocal(name_, path_), fail_on_sync(fail_on_sync_), fail_on_flush(fail_on_flush_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        return std::make_unique<FailingOnNthSyncAndNthFlushWriteBuffer>(
            DiskLocal::writeFile(path, buf_size, mode, settings), sync_count, fail_on_sync, flush_count, fail_on_flush);
    }

    size_t sync_count = 0;
    const size_t fail_on_sync;
    size_t flush_count = 0;
    const size_t fail_on_flush;
};

/// A DiskLocal that reproduces a compaction whose snapshot is written and made durable
/// but whose completion then fails: reopening the just-written snapshot for appending
/// throws, and removing the orphan snapshot during the failure cleanup throws as well.
/// The append failure is keyed on a file this disk itself has already rewritten - which
/// is exactly the compaction snapshot being reopened, and never the pre-existing file
/// `load` reopens for appending - so all other writes (the snapshot itself and the empty
/// overwrite the cleanup falls back to) go through and the log stays usable.
class DiskFailingSnapshotReopenAndRemove : public DiskLocal
{
public:
    DiskFailingSnapshotReopenAndRemove(const String & name_, const String & path_)
        : DiskLocal(name_, path_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        if (mode == WriteMode::Append && rewritten.contains(path))
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Injected snapshot-reopen failure");
        if (mode == WriteMode::Rewrite)
            rewritten.insert(path);
        return DiskLocal::writeFile(path, buf_size, mode, settings);
    }

    void removeFileIfExists(const String &) override
    {
        throw Exception(ErrorCodes::CANNOT_UNLINK, "Injected remove failure");
    }

    std::unordered_set<String> rewritten;
};

/// A DiskLocal that cannot unlink files - both `removeFile` and `removeFileIfExists`
/// throw - but still lets files be (re)written. It reproduces a disk on which
/// compaction cannot delete the old, superseded log files, so it must instead
/// neutralize them by overwriting them with an empty log (which replays as a no-op).
class DiskFailingAllRemovals : public DiskLocal
{
public:
    DiskFailingAllRemovals(const String & name_, const String & path_)
        : DiskLocal(name_, path_)
    {
    }

    void removeFile(const String &) override
    {
        throw Exception(ErrorCodes::CANNOT_UNLINK, "Injected remove failure");
    }

    void removeFileIfExists(const String &) override
    {
        throw Exception(ErrorCodes::CANNOT_UNLINK, "Injected remove failure");
    }
};

/// A DiskLocal whose writer fails once on a chosen flush (like DiskThrowingOnNthFlush)
/// and that additionally fails once on a chosen `writeFile` call. It reproduces a
/// double fault: a record write fails - canceling the writer - and the rotation the
/// rollback then attempts to reopen a fresh writer fails as well, so the operation
/// ends with `current_writer` still canceled. Both counters are shared across the
/// disk's lifetime, so each fault fires exactly once and the disk then recovers.
class DiskThrowingOnNthFlushAndNthWrite : public DiskLocal
{
public:
    DiskThrowingOnNthFlushAndNthWrite(const String & name_, const String & path_, size_t fail_on_flush_, size_t fail_on_write_)
        : DiskLocal(name_, path_), fail_on_flush(fail_on_flush_), fail_on_write(fail_on_write_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        ++write_count;
        if (write_count == fail_on_write)
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Injected write failure");
        return std::make_unique<FailingOnNthFlushWriteBuffer>(DiskLocal::writeFile(path, buf_size, mode, settings), flush_count, fail_on_flush);
    }

    size_t flush_count = 0;
    const size_t fail_on_flush;
    size_t write_count = 0;
    const size_t fail_on_write;
};

/// A DiskLocal that makes a compaction fail after its snapshot is durable and then
/// defeats the failure cleanup completely - until it is healed. While `broken`:
/// reopening a file this disk has already rewritten (the just-written compaction
/// snapshot, and never the pre-existing file `load` reopens) throws, removing files
/// throws, and rewriting an already-rewritten file (the cleanup's empty-overwrite of
/// that same snapshot) throws too, so the orphan snapshot can neither be removed nor
/// emptied. Setting `broken = false` simulates the disk recovering.
class DiskFailingCompactionCleanupCompletely : public DiskLocal
{
public:
    DiskFailingCompactionCleanupCompletely(const String & name_, const String & path_)
        : DiskLocal(name_, path_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        if (broken && rewritten.contains(path))
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Injected write failure");
        if (mode == WriteMode::Rewrite)
            rewritten.insert(path);
        return DiskLocal::writeFile(path, buf_size, mode, settings);
    }

    void removeFile(const String & path) override
    {
        if (broken)
            throw Exception(ErrorCodes::CANNOT_UNLINK, "Injected remove failure");
        DiskLocal::removeFile(path);
    }

    void removeFileIfExists(const String & path) override
    {
        if (broken)
            throw Exception(ErrorCodes::CANNOT_UNLINK, "Injected remove failure");
        DiskLocal::removeFileIfExists(path);
    }

    bool broken = true;
    std::unordered_set<String> rewritten;
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

/// Regression test (after-restart variant of DropPartRotationSyncFailureIsAllOrNothing):
/// when a part drop fails on the fsync of the previous log file and rolls back, the
/// rollback keeps the block id published live and writes a compensating CANCEL. On the
/// fsync-failure path the DROP records the CANCEL undoes may never have reached durable
/// storage, so after a restart the log can hold the committed ADD and the rollback CANCEL
/// but not the DROP in between. Replay must NOT let that CANCEL cancel the committed ADD:
/// the failed drop left the block id published, so a retry must still be deduplicated.
/// This is why applyRecords pairs a CANCEL only with a preceding real DROP of the same
/// block id, never with an ADD.
///
/// The state is constructed directly on disk because the fault-injection disk cannot
/// reproduce it: DiskLocal keeps every flushed record even when the fsync is made to
/// throw, so the DROP would still be present after a "restart". Losing the un-fsynced
/// DROP file while keeping the earlier, already-durable ADD file and the later CANCEL
/// file is exactly the reachable state after an fsync failure followed by a crash.
TEST(MergeTreeDeduplicationLog, DropPartRotationSyncFailureIsAllOrNothingAfterRestart)
{
    const std::string work_dir = "tmp/gtest_dedup_log_drop_sync_failure_restart/";
    const std::string logs_dir = work_dir + "dedup_logs/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(logs_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    auto write_log = [&](size_t number, const std::string & contents)
    {
        std::ofstream out(logs_dir + "deduplication_log_" + std::to_string(number) + ".txt");
        out << contents;
    };

    /// Log 1: the committed ADD of "block1" (its own fsync had succeeded earlier).
    write_log(1, "1\tall_1_1_0\tblock1\n");
    /// Log 2: only the rollback CANCEL of the failed drop survived. The DROP it undoes
    /// lived in the file whose fsync failed and was lost on the crash, so it is absent.
    write_log(2, "3\tall_0_9_999\tblock1\n");

    /// Replay on restart with a healthy disk.
    auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
    MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 10, format_version, disk);
    log.load();

    /// The drop failed as a whole, so "block1" stays published: a retry must still be
    /// deduplicated. Pairing the CANCEL with the committed ADD (as matching by block id
    /// alone would) forgets "block1" and wrongly accepts this retry.
    EXPECT_FALSE(log.addPart({"block1"}, part("all_6_6_0")).empty());

    std::filesystem::remove_all(work_dir);
}

/// Regression test: a rollback CANCEL must pair with the exact DROP generation it
/// undoes - same block id AND same part name - not just the same block id. A block
/// id can be reused across part generations (committed as partA, dropped, committed
/// again as partB). When dropping the second generation fails on the fsync path and
/// the DROP it wrote never becomes durable while the rollback CANCEL does, replay
/// sees ADD partA, DROP partA, ADD partB, CANCEL partB. Pairing the CANCEL by block
/// id alone consumes the older generation's committed DROP, so the surviving stream
/// is (ADD partA, ADD partB) and the map keeps the stale partA (the second insert is
/// a duplicate no-op) - after which dropping partB no longer covers the block id and
/// a legitimate reinsert is wrongly deduplicated.
///
/// The state is constructed directly on disk for the same reason as the test above:
/// the fault-injection disk cannot lose a flushed-but-not-fsynced record.
TEST(MergeTreeDeduplicationLog, DropPartRollbackCancelMatchesExactDropGeneration)
{
    const std::string work_dir = "tmp/gtest_dedup_log_cancel_exact_generation/";
    const std::string logs_dir = work_dir + "dedup_logs/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(logs_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    auto write_log = [&](size_t number, const std::string & contents)
    {
        std::ofstream out(logs_dir + "deduplication_log_" + std::to_string(number) + ".txt");
        out << contents;
    };

    /// Log 1: the first generation of "block1" was committed as all_1_1_0 and
    /// dropped; the block id was then reused and committed again as all_2_2_0.
    write_log(1,
        "1\tall_1_1_0\tblock1\n"
        "2\tall_1_1_0\tblock1\n"
        "1\tall_2_2_0\tblock1\n");
    /// Log 2: dropping all_2_2_0 failed on the fsync of the previous log file and
    /// rolled back. Only the rollback CANCEL survived the crash - the DROP of
    /// all_2_2_0 it undoes lived in the file whose fsync failed and was lost.
    write_log(2, "3\tall_2_2_0\tblock1\n");

    /// Replay on restart with a healthy disk.
    auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
    MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 10, format_version, disk);
    log.load();

    /// The failed drop rolled back, so "block1" must still be published (as the
    /// current generation all_2_2_0): a retry of it must be deduplicated.
    EXPECT_FALSE(log.addPart({"block1"}, part("all_3_3_0")).empty());

    /// Dropping the current generation must clear the block id. With the stale
    /// first generation in the map (the by-block-id-only pairing), the drop does
    /// not cover it, "block1" stays published, and this legitimate reinsert after
    /// the drop is wrongly deduplicated.
    log.dropPart(part("all_2_2_1"));
    EXPECT_TRUE(log.addPart({"block1"}, part("all_4_4_0")).empty());

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

/// Regression test: when compaction cannot remove some of the old, superseded log
/// files, it must not leave them behind with their contents intact. Replaying a
/// lingering pre-snapshot file preserves the SET of block ids the snapshot holds but
/// not their FIFO order (that file may replay to a stale intermediate order, and the
/// snapshot's ADDs on top do not refresh the position of an already-present key), and
/// that order decides which block is evicted next - so the next insert after a restart
/// could evict a different committed block than the live process would. Compaction must
/// instead neutralize an un-removable old file by emptying it, so an empty log replays
/// as a no-op wherever it sits and the snapshot alone determines the reloaded state and
/// its order. Here the disk refuses every unlink, forcing the empty-overwrite path.
TEST(MergeTreeDeduplicationLog, CompactionNeutralizesUnremovableOldLogs)
{
    const std::string work_dir = "tmp/gtest_dedup_log_compaction_unremovable/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const std::string logs_dir = work_dir + "dedup_logs";
    auto log_files = [&]() -> std::map<size_t, std::filesystem::path>
    {
        std::map<size_t, std::filesystem::path> files;
        for (const auto & entry : std::filesystem::directory_iterator(logs_dir))
        {
            const std::string stem = entry.path().stem();
            files.emplace(std::stoull(stem.substr(stem.find_last_of('_') + 1)), entry.path());
        }
        return files;
    };

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        /// Accumulate rollback garbage the same way RepeatedRollbacksAreCompactedAwayOnRestart
        /// does: one committed block plus several failed inserts, each leaving an
        /// (ADD, rollback) pair and a new log file that dropOutdatedLogs cannot reclaim.
        auto disk = std::make_shared<DiskThrowingFromNthSync>("faulty", work_dir, /*fail_from_sync=*/ 1);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();
        log.addPart({"block1"}, part("all_1_1_0"));
        for (int i = 2; i <= 8; ++i)
        {
            const std::string suffix = std::to_string(i) + "_" + std::to_string(i) + "_0";
            EXPECT_ANY_THROW(log.addPart({"block" + std::to_string(i)}, part("all_" + suffix)));
        }
        EXPECT_GT(log_files().size(), 4u);
        log.shutdown();
    }

    {
        /// Restart on a disk that can rewrite files but cannot unlink them. load() replays
        /// everything, reconstructs the single live block, and compacts - but the removal
        /// of every old file fails, so each must be neutralized by overwriting it empty.
        auto disk = std::make_shared<DiskFailingAllRemovals>("no-unlink", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        /// The old files could not be deleted, so they linger - but every one of them
        /// except the highest-numbered snapshot must have been emptied. A non-empty
        /// pre-snapshot file would resurrect the reconstructed FIFO order incorrectly on
        /// the next restart.
        const auto files = log_files();
        ASSERT_FALSE(files.empty());
        const size_t snapshot_number = files.rbegin()->first;
        for (const auto & [number, path] : files)
        {
            if (number == snapshot_number)
                continue;
            EXPECT_EQ(std::filesystem::file_size(path), 0u) << "pre-snapshot log " << path << " was not neutralized";
        }

        /// The live state is still exact: the committed block deduplicates. Probing a
        /// deduplicated block writes no record, so it neither mutates the map (with
        /// deduplication_window == 1 a committing insert would evict block1) nor the
        /// emptied files asserted above.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_9_9_0")).empty());
    }

    {
        /// A final healthy restart replays the emptied files (no-ops) and the snapshot,
        /// so the state stays correct even though the old files were never physically
        /// removed: the committed block still deduplicates.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();
        EXPECT_FALSE(log.addPart({"block1"}, part("all_11_11_0")).empty());
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

/// Regression test: when a failed insert's rollback is itself interrupted partway -
/// some compensating records reach disk, then a write throws - the per-file effective
/// record count must reflect exactly what a replay of that partially written stream
/// reconstructs. Otherwise retention (dropOutdatedLogs) over-counts the log holding the
/// rolled-back ADD records - as if none of them had been cancelled - and can drop an
/// older log that still holds a committed block id, so a restart forgets that committed
/// insert and wrongly accepts its retry. Decrementing the effective count once per
/// successfully written compensating record (rather than once after the whole rollback
/// loop) keeps it in step with the partially written stream. Without append support
/// every operation lands in its own file, so a committed block can sit in an older file
/// than a later failed insert while both stay in the window.
TEST(MergeTreeDeduplicationLog, PartialRollbackKeepsEffectiveCountInStepWithReplay)
{
    const std::string work_dir = "tmp/gtest_dedup_log_partial_rollback/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        /// Without append support every operation rotates and fsyncs the previous file.
        /// The committed insert of "block1" is fsync #1 (succeeds). The three-block
        /// failed insert writes three ADD records (flushes #2..#4), and its rotation is
        /// fsync #2, which fails and rolls the insert back. The rollback then writes a
        /// compensating record per ADD (flushes #5, #6, then #7 which fails), so only the
        /// first two ADDs get cancelled durably; the third stays published on disk. That
        /// interrupted rollback is exactly the scenario the effective count must survive.
        auto disk = std::make_shared<DiskThrowingOnNthSyncAndNthFlush>(
            "faulty", work_dir, /*fail_on_sync=*/ 2, /*fail_on_flush=*/ 7);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.simulateDiskWithoutWritingWithAppendSupportForTests();
        log.load();

        /// One committed insert, alone in its own log file. The window has room for it
        /// plus the one straggler the interrupted rollback leaves published.
        log.addPart({"block1"}, part("all_1_1_0"));

        /// A three-block insert whose rotation fails, and whose rollback is then
        /// interrupted after writing only two of its three compensating records.
        EXPECT_ANY_THROW(log.addPart({"block2", "block3", "block4"}, part("all_2_2_0")));

        /// A no-op window resize forces a rotation and a retention pass while the
        /// (possibly mis-counted) effective count is live. If the failed insert's log is
        /// over-counted, dropOutdatedLogs drops the older log file holding "block1" here.
        log.setDeduplicationWindowSize(2);

        log.shutdown();
    }

    {
        /// Restart with a healthy disk. If retention wrongly dropped the "block1" file
        /// above, replaying what is left forgets "block1" and its retry is accepted.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.simulateDiskWithoutWritingWithAppendSupportForTests();
        log.load();

        /// The committed block must still be deduplicated after the restart.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_9_9_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}

/// Regression test: on a disk without append support, restarts must not leak empty log
/// files. Every rotation - including the one in load - starts a fresh file there, and
/// dropOutdatedLogs can only reclaim an oldest prefix, never a zero-record file sitting
/// after the file that holds the live state. So without removing the empty tail files
/// on load, each restart with no new operations would leave one more empty file behind
/// and make the next load replay O(number of restarts) files. The retained file count
/// must stay bounded across repeated restarts.
TEST(MergeTreeDeduplicationLog, RestartsDoNotLeakEmptyLogsWithoutAppendSupport)
{
    const std::string work_dir = "tmp/gtest_dedup_log_restart_leak/";
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
        /// One committed insert on a disk without append support. It lands in its own
        /// file and the following rotation opens a fresh empty file, so the process ends
        /// with the data file plus one empty file.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.simulateDiskWithoutWritingWithAppendSupportForTests();
        log.load();
        log.addPart({"block1"}, part("all_1_1_0"));
        log.shutdown();
    }

    /// Restart several times without doing anything. Each restart must reopen the same
    /// bounded set of files (the data file plus a single fresh empty writer file), never
    /// accumulate one more empty file per restart.
    for (int i = 0; i < 4; ++i)
    {
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.simulateDiskWithoutWritingWithAppendSupportForTests();
        log.load();

        /// The data file plus exactly one fresh, empty writer file - not one more empty
        /// file for every restart so far.
        EXPECT_EQ(count_logs(), 2u);

        /// The committed block still deduplicates - the retained data file was kept.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_2_2_0")).empty());

        log.shutdown();
    }

    std::filesystem::remove_all(work_dir);
}

/// Regression test: when a compaction's snapshot is written and made durable but the
/// compaction then fails to switch over to it (reopening the snapshot throws), the
/// failure cleanup must not leave that durable snapshot behind at a log number higher
/// than the file the server keeps appending to. A compaction snapshot is written one
/// past current_log_number, so an orphaned snapshot outranks every older file; on the
/// next restart load replays it last - after the older files that by then may hold newer
/// committed block ids - and its stale ADD records would resurrect evicted block ids and
/// forget committed ones. If the snapshot cannot even be removed during cleanup it must
/// be overwritten with an empty file, which replays as a no-op regardless of position.
TEST(MergeTreeDeduplicationLog, CompactionCleanupFailureNeutralizesOrphanSnapshot)
{
    const std::string work_dir = "tmp/gtest_dedup_log_compaction_orphan/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const std::string logs_dir = work_dir + "dedup_logs";
    auto highest_log_record_count = [&]() -> size_t
    {
        /// The number of records in the highest-numbered log file - the slot a compaction
        /// snapshot occupies. A neutralized orphan snapshot is empty; a surviving one is not.
        std::map<size_t, std::filesystem::path> logs;
        for (const auto & entry : std::filesystem::directory_iterator(logs_dir))
        {
            const std::string stem = entry.path().stem();
            logs.emplace(std::stoull(stem.substr(stem.find_last_of('_') + 1)), entry.path());
        }
        if (logs.empty())
            return 0;
        std::ifstream in(logs.rbegin()->second);
        size_t count = 0;
        std::string line;
        while (std::getline(in, line))
            ++count;
        return count;
    };

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        /// First accumulate the unreclaimable rollback garbage that makes the next restart
        /// want to compact, exactly as in the compaction test above.
        auto disk = std::make_shared<DiskThrowingFromNthSync>("faulty", work_dir, /*fail_from_sync=*/ 1);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        log.addPart({"block1"}, part("all_1_1_0"));
        for (int i = 2; i <= 8; ++i)
        {
            const std::string suffix = std::to_string(i) + "_" + std::to_string(i) + "_0";
            EXPECT_ANY_THROW(log.addPart({"block" + std::to_string(i)}, part("all_" + suffix)));
        }
        EXPECT_GT(highest_log_record_count(), 0u);

        log.shutdown();
    }

    {
        /// Restart on a disk that lets the compaction write and fsync its snapshot, then
        /// fails to reopen it for appending and fails to remove it during cleanup. load
        /// reconstructs the single live block and triggers compaction, which writes the
        /// snapshot durably and then aborts at the reopen.
        auto disk = std::make_shared<DiskFailingSnapshotReopenAndRemove>("faulty-compaction", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        /// The orphaned snapshot occupies the highest log number. Without neutralization it
        /// stays behind holding the live state ({"block1"}), outranking every older file, so
        /// a later committed block written to a lower-numbered file would be clobbered by it
        /// on the next replay. The fix overwrites it with an empty file, so the highest log
        /// file holds no records.
        EXPECT_EQ(highest_log_record_count(), 0u);

        log.shutdown();
    }

    {
        /// A healthy restart must still reconstruct the live state exactly.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        EXPECT_FALSE(log.addPart({"block1"}, part("all_9_9_0")).empty());
        EXPECT_TRUE(log.addPart({"block2"}, part("all_10_10_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}

/// Regression test: a canceled writer must be healed before the next operation, not
/// only while rolling back the operation that canceled it. A failed record write
/// cancels `current_writer`; the rollback then rotates to a fresh writer, but that
/// rotation can fail too (the disk is still down). Without healing at the start of
/// the next operation, the first retry after the disk recovers throws "Cannot write
/// to canceled buffer" before reaching any recovery code - only its own rollback
/// reopens a writer - so the first retry always fails, breaking the contract that
/// a failed operation can simply be retried.
TEST(MergeTreeDeduplicationLog, CanceledWriterIsHealedBeforeNextOperation)
{
    const std::string work_dir = "tmp/gtest_dedup_log_canceled_writer_heal/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        /// writeFile #1 creates the first log during load(). flush #1 is the first ADD
        /// record, which fails and cancels the writer; the rollback's rotate() then
        /// issues writeFile #2, which fails as well, leaving the writer canceled after
        /// the operation. Both faults fire once, so the disk has recovered by the retry.
        /// The window is large enough that no rotation interferes.
        auto disk = std::make_shared<DiskThrowingOnNthFlushAndNthWrite>(
            "faulty", work_dir, /*fail_on_flush=*/ 1, /*fail_on_write=*/ 2);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 10, format_version, disk);
        log.load();

        /// The double fault: the ADD write fails and the rollback cannot reopen a writer.
        EXPECT_ANY_THROW(log.addPart({"block1"}, part("all_1_1_0")));

        /// The disk has recovered, so the FIRST retry must already succeed: the
        /// operation heals the canceled writer up front instead of tripping over it.
        EXPECT_TRUE(log.addPart({"block1"}, part("all_1_1_0")).empty());

        /// And the retried block deduplicates as usual.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_2_2_0")).empty());

        log.shutdown();
    }

    {
        /// The retried insert's ADD record went to a live writer, so it must have
        /// reached the disk and must survive a restart.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 10, format_version, disk);
        log.load();

        EXPECT_FALSE(log.addPart({"block1"}, part("all_3_3_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}

/// Regression test: when a failed compaction leaves an orphan file behind that can
/// neither be removed nor overwritten with an empty file, the log must fail closed
/// instead of carrying on. The orphan snapshot sits at a HIGHER log number than the
/// file the server keeps appending to, so any record committed after it would be
/// clobbered on the next replay by the stale snapshot replaying last - silent wrong
/// deduplication after a restart. New operations must therefore refuse to write until
/// a retry manages to neutralize the orphan, and succeed again once the disk recovers.
TEST(MergeTreeDeduplicationLog, OrphanNeutralizationFailureFailsClosedUntilHealed)
{
    const std::string work_dir = "tmp/gtest_dedup_log_orphan_fail_closed/";
    std::filesystem::remove_all(work_dir);
    std::filesystem::create_directories(work_dir);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        /// Accumulate the unreclaimable rollback garbage that makes the next restart
        /// want to compact, exactly as in the compaction tests above.
        auto disk = std::make_shared<DiskThrowingFromNthSync>("faulty", work_dir, /*fail_from_sync=*/ 1);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 1, format_version, disk);
        log.load();

        log.addPart({"block1"}, part("all_1_1_0"));
        for (int i = 2; i <= 8; ++i)
        {
            const std::string suffix = std::to_string(i) + "_" + std::to_string(i) + "_0";
            EXPECT_ANY_THROW(log.addPart({"block" + std::to_string(i)}, part("all_" + suffix)));
        }

        log.shutdown();
    }

    {
        /// Restart on a disk that lets the compaction write and fsync its snapshot, then
        /// fails the reopen for appending AND the whole failure cleanup: the orphan
        /// snapshot can neither be removed nor emptied, so it survives at the highest
        /// log number with the stale live state ({"block1"}) inside. The window is
        /// bumped to 2 (rotate_interval 4) so the inserts below do not trigger a
        /// rotation of their own - a rotation would rewrite the orphan's very slot and
        /// mask the hazard by accident.
        auto disk = std::make_shared<DiskFailingCompactionCleanupCompletely>("faulty-cleanup", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        /// Fail closed: while the stale orphan is on disk, committing a new block would
        /// append it to a lower-numbered file, and once the window rolled over, the
        /// orphan's replay would clobber the state on the next restart (see the last
        /// section). The insert must fail - retryably - not carry on.
        EXPECT_ANY_THROW(log.addPart({"block9"}, part("all_9_9_0")));

        /// Reading is unaffected: the committed block still deduplicates (probing a
        /// duplicate writes no record).
        EXPECT_FALSE(log.addPart({"block1"}, part("all_10_10_0")).empty());

        /// The disk recovers. The first operation retries the neutralization, removes
        /// the orphan, and the inserts go through. Without the fail-closed behavior the
        /// first of these would have been committed while the disk was still broken, and
        /// this retry would wrongly report it as a duplicate.
        disk->broken = false;
        EXPECT_TRUE(log.addPart({"block9"}, part("all_11_11_0")).empty());
        EXPECT_TRUE(log.addPart({"block10"}, part("all_12_12_0")).empty());

        log.shutdown();
    }

    {
        /// A healthy restart reconstructs the state the live process had: committing
        /// "block9" and "block10" filled the window of 2 and evicted "block1", and no
        /// stale snapshot is left to clobber that. Had the inserts been accepted while
        /// the orphan was still on disk, its stale ADD of "block1" would replay last
        /// here, re-inserting "block1" and evicting "block9" - whose retry would then
        /// wrongly be accepted, duplicating the data.
        auto disk = std::make_shared<DiskLocal>("healthy", work_dir);
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        EXPECT_FALSE(log.addPart({"block9"}, part("all_13_13_0")).empty());
        EXPECT_FALSE(log.addPart({"block10"}, part("all_14_14_0")).empty());
        EXPECT_TRUE(log.addPart({"block2"}, part("all_15_15_0")).empty());
    }

    std::filesystem::remove_all(work_dir);
}
