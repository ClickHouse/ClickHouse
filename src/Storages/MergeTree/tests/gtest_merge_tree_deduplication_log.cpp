#include <gtest/gtest.h>

#include <filesystem>

#include <Disks/DiskLocal.h>
#include <IO/SwapHelper.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <Storages/MergeTree/MergeTreeDeduplicationLog.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/Exception.h>

#include <unistd.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int FAULT_INJECTED;
}

namespace
{

/// A local disk that fails a chosen `writeFile` call, simulating a transient I/O error.
class DiskFailingOnNthWriteFile : public DiskLocal
{
public:
    DiskFailingOnNthWriteFile(const String & path_, size_t fail_on_call_)
        : DiskLocal("faulty", path_), fail_on_call(fail_on_call_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        if (++calls == fail_on_call)
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure while opening {}", path);
        return DiskLocal::writeFile(path, buf_size, mode, settings);
    }

private:
    size_t calls = 0;
    const size_t fail_on_call;
};

/// A writer that fails a chosen flush the way a real I/O error does: `WriteBuffer::next` moves the
/// cursor back, cancels the buffer and rethrows, so the buffer rejects every later write.
class WriteBufferFailingOnNthFlush : public WriteBufferFromFileDecorator
{
public:
    WriteBufferFailingOnNthFlush(std::unique_ptr<WriteBufferFromFileBase> impl_, size_t fail_on_flush_)
        : WriteBufferFromFileDecorator(std::move(impl_)), fail_on_flush(fail_on_flush_)
    {
    }

private:
    void nextImpl() override
    {
        if (++flushes == fail_on_flush)
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure while flushing {}", getFileName());

        /// The body of `WriteBufferFromFileDecorator::nextImpl`, which is private.
        SwapHelper swap(*this, *impl);
        impl->next();
    }

    size_t flushes = 0;
    const size_t fail_on_flush;
};

/// A local disk whose first writer fails a chosen flush. Later writers are healthy, so the log can recover.
/// It can also fail a chosen `writeFile` call outright, which makes an unwanted attempt to open a log
/// file observable. `fail_write_file_on_call` counts every call and 0 means no such failure.
class DiskFailingOnNthFlush : public DiskLocal
{
public:
    DiskFailingOnNthFlush(const String & path_, size_t fail_on_flush_, size_t fail_write_file_on_call_ = 0)
        : DiskLocal("faulty_flush", path_), fail_on_flush(fail_on_flush_), fail_write_file_on_call(fail_write_file_on_call_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        if (++write_file_calls == fail_write_file_on_call)
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure while opening {}", path);

        auto impl = DiskLocal::writeFile(path, buf_size, mode, settings);
        if (std::exchange(inject, false))
            return std::make_unique<WriteBufferFailingOnNthFlush>(std::move(impl), fail_on_flush);
        return impl;
    }

private:
    bool inject = true;
    size_t write_file_calls = 0;
    const size_t fail_on_flush;
    const size_t fail_write_file_on_call;
};

/// A writer that publishes nothing when it is canceled. On an object-storage disk the metadata of the
/// path is created by the callback that `finalize` runs, and cancellation never runs it, so the path a
/// canceled writer was opened for stays absent. Removing the local file it created is the stand-in.
class WriteBufferUnpublishingOnCancel : public WriteBufferFailingOnNthFlush
{
public:
    WriteBufferUnpublishingOnCancel(std::unique_ptr<WriteBufferFromFileBase> impl_, size_t fail_on_flush_, std::string file_name_)
        : WriteBufferFailingOnNthFlush(std::move(impl_), fail_on_flush_), file_name(std::move(file_name_))
    {
    }

private:
    void cancelImpl() noexcept override
    {
        WriteBufferFromFileDecorator::cancelImpl();

        std::error_code ignored;
        std::filesystem::remove(file_name, ignored);
    }

    const std::string file_name;
};

/// A local disk whose first writer fails a chosen flush and publishes nothing when it is canceled.
/// Later writers are healthy, so the log can recover.
class DiskUnpublishingOnCancel : public DiskLocal
{
public:
    DiskUnpublishingOnCancel(const String & path_, size_t fail_on_flush_)
        : DiskLocal("unpublishing_on_cancel", path_), fail_on_flush(fail_on_flush_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) override
    {
        auto impl = DiskLocal::writeFile(path, buf_size, mode, settings);
        if (!std::exchange(inject, false))
            return impl;

        auto file_name = impl->getFileName();
        return std::make_unique<WriteBufferUnpublishingOnCancel>(std::move(impl), fail_on_flush, std::move(file_name));
    }

private:
    bool inject = true;
    const size_t fail_on_flush;
};

/// The error code matters: a `LOGICAL_ERROR` would mean the defect fired instead of the injection.
template <typename Operation>
void expectInjectedFailure(Operation && operation)
{
    try
    {
        operation();
        FAIL() << "the injected flush failure did not propagate";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::FAULT_INJECTED) << e.displayText();
    }
}

}

/// A failure while opening the next log file during rotation must neither fail the operation that
/// triggered the rotation, nor leave the log unusable. Before the fix, the operation failed although
/// its records were already written, and `rotate` had already finalized the current writer, so the
/// next write to the deduplication log failed with the logical error "Cannot write to finalized buffer".
TEST(MergeTreeDeduplicationLog, RotationFailureKeepsLogUsable)
{
    const auto disk_path = std::filesystem::temp_directory_path() / ("clickhouse_gtest_dedup_log_" + std::to_string(getpid()));
    std::filesystem::remove_all(disk_path);
    std::filesystem::create_directories(disk_path);

    /// The first `writeFile` opens the first log during `load`. The second one is the rotation
    /// after `rotate_interval` (two times the window) records were written, and it fails.
    auto disk = std::make_shared<DiskFailingOnNthWriteFile>(disk_path.string() + "/", 2);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        log.addPart({"block1"}, part("all_1_1_0"));
        log.addPart({"block2"}, part("all_2_2_0"));
        log.addPart({"block3"}, part("all_3_3_0"));

        /// The fourth record triggers the rotation, whose `writeFile` fails. The insert must succeed anyway.
        EXPECT_TRUE(log.addPart({"block4"}, part("all_4_4_0")).empty());
        EXPECT_FALSE(log.addPart({"block4"}, part("all_5_5_0")).empty());

        /// The log must still accept records, both drops and adds, and the next rotation must succeed.
        EXPECT_NO_THROW(log.dropPart(part("all_4_4_0")));
        EXPECT_TRUE(log.addPart({"block4"}, part("all_5_5_0")).empty());
        EXPECT_TRUE(log.addPart({"block5"}, part("all_6_6_0")).empty());
        EXPECT_FALSE(log.addPart({"block4"}, part("all_7_7_0")).empty());
        EXPECT_FALSE(log.addPart({"block5"}, part("all_7_7_0")).empty());
    }

    /// The records written after the failed rotation must have reached the disk.
    {
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        EXPECT_FALSE(log.addPart({"block4"}, part("all_7_7_0")).empty());
        EXPECT_FALSE(log.addPart({"block5"}, part("all_7_7_0")).empty());
        EXPECT_TRUE(log.addPart({"block3"}, part("all_7_7_0")).empty());
    }

    std::filesystem::remove_all(disk_path);
}

/// A failed flush cancels the writer of the deduplication log, and a canceled buffer rejects every later
/// write. Before the fix, the next record hit the logical error "Cannot write to canceled buffer", which
/// aborts the process in debug and sanitizer builds, and in a release build made every following insert
/// into the table fail until the server was restarted.
TEST(MergeTreeDeduplicationLog, FlushFailureKeepsLogUsable)
{
    const auto disk_path
        = std::filesystem::temp_directory_path() / ("clickhouse_gtest_dedup_log_flush_" + std::to_string(getpid()));
    std::filesystem::remove_all(disk_path);
    std::filesystem::create_directories(disk_path);

    /// `load` opens the first log, and that writer fails its second flush. `writeRecord` flushes once per
    /// record and the records are far smaller than the buffer, so the second record is the one that fails.
    auto disk = std::make_shared<DiskFailingOnNthFlush>(disk_path.string() + "/", 2);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        EXPECT_TRUE(log.addPart({"block1"}, part("all_1_1_0")).empty());

        /// The flush of the second record fails, which cancels the writer.
        expectInjectedFailure([&] { log.addPart({"block2"}, part("all_2_2_0")); });

        /// The log must still accept records, and must still deduplicate them.
        EXPECT_TRUE(log.addPart({"block3"}, part("all_3_3_0")).empty());
        EXPECT_FALSE(log.addPart({"block3"}, part("all_4_4_0")).empty());

        /// The other entry point that writes records keeps working too.
        EXPECT_NO_THROW(log.dropPart(part("all_1_1_0")));
        EXPECT_TRUE(log.addPart({"block1"}, part("all_5_5_0")).empty());
    }

    {
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        /// The record written after the recovery is on the disk.
        EXPECT_FALSE(log.addPart({"block3"}, part("all_6_6_0")).empty());
        /// The record whose flush failed is not, so a retry of that insert is not deduplicated away.
        EXPECT_TRUE(log.addPart({"block2"}, part("all_6_6_0")).empty());
    }

    std::filesystem::remove_all(disk_path);
}

/// The operation that follows the failed flush can be a `DROP PARTITION` rather than another insert, so
/// `dropPart` is an independent entry into the canceled writer: it reaches it with no `addPart` having
/// replaced it first.
TEST(MergeTreeDeduplicationLog, CanceledWriterRecoversThroughDropPart)
{
    const auto disk_path
        = std::filesystem::temp_directory_path() / ("clickhouse_gtest_dedup_log_droppart_" + std::to_string(getpid()));
    std::filesystem::remove_all(disk_path);
    std::filesystem::create_directories(disk_path);

    /// `load` opens the first log, and that writer fails its second flush, which is the second record.
    auto disk = std::make_shared<DiskFailingOnNthFlush>(disk_path.string() + "/", 2);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        EXPECT_TRUE(log.addPart({"block1"}, part("all_1_1_0")).empty());

        /// The flush of the second record fails, which cancels the writer.
        expectInjectedFailure([&] { log.addPart({"block2"}, part("all_2_2_0")); });

        /// The dropped part covers the part of `block1`, so this writes a record through the dead writer.
        EXPECT_NO_THROW(log.dropPart(part("all_1_1_0")));
        EXPECT_TRUE(log.addPart({"block3"}, part("all_3_3_0")).empty());
    }

    {
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        /// The drop record reached the disk, so `block1` is not deduplicated against the dropped part.
        EXPECT_TRUE(log.addPart({"block1"}, part("all_4_4_0")).empty());
        /// The record added through the replaced writer reached the disk as well.
        EXPECT_FALSE(log.addPart({"block3"}, part("all_4_4_0")).empty());
    }

    std::filesystem::remove_all(disk_path);
}

/// A drop that covers no retained block ID writes no record, so it must neither open a log file nor
/// fail, even when the writer of the deduplication log is dead.
TEST(MergeTreeDeduplicationLog, DropPartWithNothingToWriteDoesNotRecover)
{
    const auto disk_path
        = std::filesystem::temp_directory_path() / ("clickhouse_gtest_dedup_log_dropnomatch_" + std::to_string(getpid()));
    std::filesystem::remove_all(disk_path);
    std::filesystem::create_directories(disk_path);

    /// `load` opens the first log, and that writer fails its second flush. The second `writeFile` call
    /// fails outright, so an attempt to open a log file after the cancellation is observable.
    auto disk = std::make_shared<DiskFailingOnNthFlush>(disk_path.string() + "/", 2, 2);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    {
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        EXPECT_TRUE(log.addPart({"block1"}, part("all_1_1_0")).empty());

        /// The flush of the second record fails, which cancels the writer.
        expectInjectedFailure([&] { log.addPart({"block2"}, part("all_2_2_0")); });

        /// `all_9_9_0` covers nothing in the deduplication map, so no record is needed and no log is opened.
        EXPECT_NO_THROW(log.dropPart(part("all_9_9_0")));

        /// The recovery is only deferred: the next operation that does have a record to write is the one
        /// that opens the log file whose `writeFile` call fails, and it reports that failure.
        expectInjectedFailure([&] { log.addPart({"block3"}, part("all_3_3_0")); });

        /// A failed recovery is retried, so the log still ends up usable.
        EXPECT_TRUE(log.addPart({"block3"}, part("all_3_3_0")).empty());
        /// The drop wrote nothing, so it left the deduplication state alone.
        EXPECT_FALSE(log.addPart({"block1"}, part("all_4_4_0")).empty());
    }

    std::filesystem::remove_all(disk_path);
}

/// Rotation records the path of the new log before anything is written to it, and a writer that is
/// canceled instead of finalized publishes no path on an object-storage disk. Retention removes the logs
/// it has recorded with the overload that throws on a missing path, and erases the entry only after the
/// removal returns, so before the fix the first pass that reached such an entry threw before the erase and
/// every later pass threw on the same entry, dropping no log again until the server was restarted.
TEST(MergeTreeDeduplicationLog, CanceledWriterDoesNotWedgeRetention)
{
    const auto disk_path
        = std::filesystem::temp_directory_path() / ("clickhouse_gtest_dedup_log_retention_" + std::to_string(getpid()));
    std::filesystem::remove_all(disk_path);
    std::filesystem::create_directories(disk_path);

    /// `load` opens the first log, and that writer fails its second flush, which is the second record.
    auto disk = std::make_shared<DiskUnpublishingOnCancel>(disk_path.string() + "/", 2);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    auto part = [&](const String & name) { return MergeTreePartInfo::fromPartName(name, format_version); };

    const auto second_log_path = disk_path / "dedup_logs" / "deduplication_log_2.txt";

    {
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        EXPECT_TRUE(log.addPart({"block1"}, part("all_1_1_0")).empty());

        /// The flush of the second record fails, which cancels the writer of the first log and leaves that
        /// log recorded under a path that holds nothing.
        expectInjectedFailure([&] { log.addPart({"block2"}, part("all_2_2_0")); });

        /// Three records through the recovered writer are more than the deduplication window, so the next
        /// retention pass selects the first log for removal.
        EXPECT_TRUE(log.addPart({"block3"}, part("all_3_3_0")).empty());
        EXPECT_TRUE(log.addPart({"block4"}, part("all_4_4_0")).empty());
        EXPECT_TRUE(log.addPart({"block5"}, part("all_5_5_0")).empty());

        /// `ALTER ... MODIFY SETTING` reaches retention outside the `try` that a write goes through, so a
        /// failure there is reported rather than logged and retried forever.
        EXPECT_NO_THROW(log.setDeduplicationWindowSize(1));

        /// The log still accepts records and still deduplicates them.
        EXPECT_TRUE(log.addPart({"block6"}, part("all_6_6_0")).empty());
        EXPECT_FALSE(log.addPart({"block6"}, part("all_7_7_0")).empty());

        /// And retention keeps making progress: this record fills the current log, and the rotation it
        /// triggers has to remove the log that has fallen out of the window by now.
        EXPECT_TRUE(log.addPart({"block7"}, part("all_8_8_0")).empty());
        EXPECT_FALSE(std::filesystem::exists(second_log_path));
    }

    std::filesystem::remove_all(disk_path);
}

/// The failed flush can also be the last operation on the deduplication log before the table is dropped
/// or the server stops, so no later record can replace the canceled writer. `finalize` rejects a canceled
/// buffer as well, with the logical error "Cannot finalize buffer after cancellation."
TEST(MergeTreeDeduplicationLog, CanceledWriterDoesNotBreakShutdown)
{
    const auto disk_path
        = std::filesystem::temp_directory_path() / ("clickhouse_gtest_dedup_log_shutdown_" + std::to_string(getpid()));
    std::filesystem::remove_all(disk_path);
    std::filesystem::create_directories(disk_path);

    auto disk = std::make_shared<DiskFailingOnNthFlush>(disk_path.string() + "/", 1);

    const MergeTreeDataFormatVersion format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;

    {
        MergeTreeDeduplicationLog log("dedup_logs", /*deduplication_window=*/ 2, format_version, disk);
        log.load();

        expectInjectedFailure([&] {
            log.addPart({"block1"}, MergeTreePartInfo::fromPartName("all_1_1_0", format_version));
        });

        EXPECT_NO_THROW(log.shutdown());
    }

    std::filesystem::remove_all(disk_path);
}
