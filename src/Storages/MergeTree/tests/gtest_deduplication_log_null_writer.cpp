#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Storages/MergeTree/MergeTreeDeduplicationLog.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/Exception.h>
#include <base/defines.h>   /// DEBUG_OR_SANITIZER_BUILD

#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
constexpr auto FORMAT_VERSION = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;

/// B37 regression: a `MergeTreeDeduplicationLog` whose `current_writer` is null (the disk could not
/// host the append-mode log -- see `MergeTreeDeduplicationLog::load()`'s early-return path for a
/// `DiskObjectStorage` whose metadata storage type is neither `Plain` nor `ContentAddressed`) used to
/// be dereferenced unconditionally by `addPart`/`dropPart`: a release-build `chassert` is a no-op, so
/// this was a null-pointer dereference (segfault) rather than a handled error. The fix makes both
/// throw a `LOGICAL_ERROR` `DB::Exception` instead.
///
/// There is no way to drive this from a stateless SQL test: every disk type that reaches production
/// either materializes `logs_dir` (so `load()` takes the normal `rotate()` path and sets a writer) or
/// is one of the two types (`Plain`, `ContentAddressed`) `load()` explicitly special-cases to still get
/// a writer. So this test constructs the log directly and never calls `load()` -- `current_writer`
/// simply stays at its default-constructed null value, which is the exact precondition the guard in
/// `addPart`/`dropPart` exists for.
struct DeduplicationLogNullWriterFixture : public ::testing::Test
{
    std::filesystem::path base_path;
    DiskPtr disk;
    std::unique_ptr<MergeTreeDeduplicationLog> log;

    void SetUp() override
    {
        const auto unique = std::to_string(::getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        base_path = std::filesystem::temp_directory_path() / ("dedup_log_null_writer_gtest_" + unique);
        std::filesystem::create_directories(base_path);
        disk = std::make_shared<DiskLocal>("test_disk_" + unique, base_path.string());

        /// deduplication_window != 0 so addPart/dropPart don't bail out on the "deduplication is off"
        /// fast path before ever reaching the null-writer guard. `load()` is deliberately NOT called:
        /// that is what leaves `current_writer` null.
        log = std::make_unique<MergeTreeDeduplicationLog>("deduplication_logs", /*deduplication_window_=*/4, FORMAT_VERSION, disk);
    }

    void TearDown() override
    {
        log.reset();
        std::error_code ec;
        std::filesystem::remove_all(base_path, ec);
    }
};

}

#if defined(DEBUG_OR_SANITIZER_BUILD)
/// gtest runs *DeathTest suites before others; reuse the same fixture via an alias so the death arm
/// gets the same null-writer precondition.
using DeduplicationLogNullWriterDeathTest = DeduplicationLogNullWriterFixture;
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST_F(DeduplicationLogNullWriterFixture, AddPartThrowsLogicalErrorInsteadOfCrashing)
{
    /// LOGICAL_ERROR "no writer" is a broken-invariant guard (addPart on a null current_writer). Under
    /// abort_on_logical_error it aborts at construction instead of being catchable -- the DeathTest
    /// below proves the abort in those builds.
    auto part_info = MergeTreePartInfo::fromPartName("all_0_0_0", FORMAT_VERSION);

    EXPECT_THROW(
        {
            try
            {
                log->addPart({"block-1"}, part_info);
            }
            catch (const Exception & e)
            {
                EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
                EXPECT_NE(e.message().find("no writer"), std::string::npos);
                throw;
            }
        },
        Exception);

    /// The object stays alive and usable after the guard fires: it isn't left half-corrupted by the
    /// failed call, and repeating the same call (still no writer) throws again, cleanly, rather than
    /// crashing or behaving differently the second time.
    EXPECT_THROW(log->addPart({"block-1"}, part_info), Exception);
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST_F(DeduplicationLogNullWriterDeathTest, AddPartAborts)
{
    auto part_info = MergeTreePartInfo::fromPartName("all_0_0_0", FORMAT_VERSION);
    EXPECT_DEATH({ log->addPart({"block-1"}, part_info); }, "no writer");
}
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST_F(DeduplicationLogNullWriterFixture, DropPartThrowsLogicalErrorInsteadOfCrashing)
{
    auto part_info = MergeTreePartInfo::fromPartName("all_0_0_0", FORMAT_VERSION);

    EXPECT_THROW(
        {
            try
            {
                log->dropPart(part_info);
            }
            catch (const Exception & e)
            {
                EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
                EXPECT_NE(e.message().find("no writer"), std::string::npos);
                throw;
            }
        },
        Exception);
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST_F(DeduplicationLogNullWriterDeathTest, DropPartAborts)
{
    auto part_info = MergeTreePartInfo::fromPartName("all_0_0_0", FORMAT_VERSION);
    EXPECT_DEATH({ log->dropPart(part_info); }, "no writer");
}
#endif
