#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <IO/Operators.h>
#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Common/Exception.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

namespace fs = std::filesystem;

/// Creates a temporary local directory and wraps it with `DiskLocal`.
struct MutationEntryFixture
{
    fs::path base_path;
    DiskPtr disk;

    MutationEntryFixture()
    {
        /// Use `<process-id>_<object-address>` to create a unique directory name.
        auto unique_id = std::to_string(::getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        base_path = fs::current_path() / "tmp" / ("merge_tree_mutation_entry_gtest_" + unique_id);
        fs::create_directories(base_path);
        disk = std::make_shared<DiskLocal>("mutation_entry_test_disk_" + unique_id, base_path.string());
    }

    ~MutationEntryFixture()
    {
        disk.reset();
        std::error_code ec;
        fs::remove_all(base_path, ec);
    }

    /// Writes a mutation file with optional tail fields.
    void writeMutationFile(const String & file_name, const std::vector<String> & tail_fields = {}) const
    {
        auto out = disk->writeFile(file_name);
        *out << "format version: 1\n";
        *out << "create time: 2026-05-16 11:41:49\n";
        *out << "commands: (UPDATE value = value + 1 WHERE id < 10)\n";
        for (const auto & field : tail_fields)
            *out << field << "\n";
        out->finalize();
        out->sync();
    }

    MergeTreeMutationEntry load(const String & file_name) const
    {
        return MergeTreeMutationEntry(disk, "", file_name);
    }
};

/// Returns a fixed timestamp used by parser tests.
time_t sample_date_time()
{
    return makeDateTime(DateLUT::serverTimezoneInstance(), 2026, 5, 16, 11, 42, 0);
}

}

TEST(MergeTreeMutationEntry, LoadsOldNonTransactionalFileWithoutFinishTime)
{
    MutationEntryFixture fixture;
    fixture.writeMutationFile("mutation_1.txt");

    auto entry = fixture.load("mutation_1.txt");

    EXPECT_EQ(entry.block_number, 1);
    EXPECT_EQ(entry.finish_time, 0);
    EXPECT_EQ(entry.tid, Tx::NonTransactionalTID);
    EXPECT_EQ(entry.csn, Tx::NonTransactionalCSN);
    EXPECT_EQ(entry.commands->toString(false), "(UPDATE value = value + 1 WHERE id < 10)");
}

TEST(MergeTreeMutationEntry, LoadsAppendedFinishTime)
{
    MutationEntryFixture fixture;
    fixture.writeMutationFile("mutation_2.txt", {"finish time: 2026-05-16 11:42:00"});

    auto entry = fixture.load("mutation_2.txt");

    EXPECT_EQ(entry.block_number, 2);
    EXPECT_EQ(entry.finish_time, sample_date_time());
    EXPECT_EQ(entry.tid, Tx::NonTransactionalTID);
    EXPECT_EQ(entry.csn, Tx::NonTransactionalCSN);
}

TEST(MergeTreeMutationEntry, LoadsTransactionalTailFields)
{
    MutationEntryFixture fixture;
    fixture.writeMutationFile(
        "mutation_3.txt",
        {
            "tid: (10, 33, 00000000-0000-0000-0000-000000000001)",
            "csn: 42",
            "finish time: 2026-05-16 11:42:00",
        });

    auto entry = fixture.load("mutation_3.txt");

    EXPECT_EQ(entry.finish_time, sample_date_time());
    EXPECT_EQ(entry.tid.start_csn, 10);
    EXPECT_EQ(entry.tid.local_tid, 33);
    EXPECT_EQ(entry.csn, 42);
}

TEST(MergeTreeMutationEntry, LoadsTailFieldsInAnyOrder)
{
    MutationEntryFixture fixture;
    fixture.writeMutationFile(
        "mutation_4.txt",
        {
            "finish time: 2026-05-16 11:42:00",
            "tid: (10, 33, 00000000-0000-0000-0000-000000000001)",
            "csn: 42",
        });

    auto entry = fixture.load("mutation_4.txt");

    EXPECT_EQ(entry.finish_time, sample_date_time());
    EXPECT_EQ(entry.tid.start_csn, 10);
    EXPECT_EQ(entry.tid.local_tid, 33);
    EXPECT_EQ(entry.csn, 42);
}

TEST(MergeTreeMutationEntry, UnknownTailFieldThrowsBadArgumentsWithoutConsumingFieldName)
{
    MutationEntryFixture fixture;
    fixture.writeMutationFile("mutation_5.txt", {"invalid time: 2026-05-16 11:42:00"});

    try
    {
        (void)fixture.load("mutation_5.txt");
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(std::string(e.message()).find("invalid time: 2026-05-16 11:42:00"), std::string::npos);
    }
}
