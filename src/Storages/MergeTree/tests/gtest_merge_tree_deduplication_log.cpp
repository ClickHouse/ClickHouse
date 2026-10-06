#include <gtest/gtest.h>

#include <filesystem>

#include <Disks/DiskLocal.h>
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
