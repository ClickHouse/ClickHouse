#include <Core/Field.h>
#include <Core/Range.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <set>

/// The part-level minmax index is materialized as a prefix of its column list, never as an arbitrary subset:
/// the columns are grouped into segments appended in a fixed order (see the note on
/// `MergeTreePartMinMaxIndexColumns` in `Core/SettingsEnums.h`), and every reader expresses how much of the
/// index a part carries as a single width. These tests pin the two places where `MinMaxIndex::store` has to
/// stop rather than skip ahead to keep that true (and show what the second one protects against), and the one
/// place where it has to skip: a file the caller has already carried over must not be written through.

namespace
{
using namespace DB;
namespace fs = std::filesystem;

struct MinMaxIndexStoreFixture : public ::testing::Test
{
    fs::path tmp_dir;
    DiskPtr disk;
    std::shared_ptr<SingleDiskVolume> volume;
    MergeTreeSettingsPtr settings;

    void SetUp() override
    {
        tmp_dir = fs::temp_directory_path()
            / ("gtest_minmax_index_store_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_"
               + std::to_string(reinterpret_cast<uintptr_t>(this)));
        fs::remove_all(tmp_dir);
        fs::create_directories(tmp_dir);
        disk = std::make_shared<DiskLocal>("test_disk", tmp_dir.string());
        volume = std::make_shared<SingleDiskVolume>("test_vol", disk);
        settings = std::make_shared<const MergeTreeSettings>();
    }

    void TearDown() override
    {
        volume.reset();
        disk.reset();
        fs::remove_all(tmp_dir);
    }

    /// Stores `index` into a fresh part directory and returns the names of the files it left behind.
    std::set<String> store(const String & part_dir, const IMergeTreeDataPart::MinMaxIndex & index, const NamesAndTypesList & columns) const
    {
        fs::create_directories(tmp_dir / part_dir);
        DataPartStorageOnDiskFull part_storage(volume, "", part_dir);

        MergeTreeDataPartChecksums checksums;
        auto written_files = index.store(columns, part_storage, checksums, settings);
        for (auto & file : written_files)
            file->finalize();

        std::set<String> file_names;
        for (const auto & entry : fs::directory_iterator(tmp_dir / part_dir))
            file_names.insert(entry.path().filename().string());

        /// Whatever landed on disk must be exactly what the checksums advertise.
        std::set<String> checksum_names;
        for (const auto & [name, _] : checksums.files)
            checksum_names.insert(name);
        EXPECT_EQ(file_names, checksum_names);

        return file_names;
    }
};

/// The shape of the index of a table with one partition key column under
/// `part_minmax_index_columns = 'with_block_number_offset'`.
NamesAndTypesList threeColumns()
{
    return {
        {"p", std::make_shared<DataTypeUInt64>()},
        {"_block_number", std::make_shared<DataTypeUInt64>()},
        {"_block_offset", std::make_shared<DataTypeUInt64>()},
    };
}

TEST_F(MinMaxIndexStoreFixture, StoresEveryColumnWhenEveryRangeIsKnown)
{
    IMergeTreeDataPart::MinMaxIndex index;
    index.hyperrectangle = {
        Range(Field(UInt64(7)), true, Field(UInt64(7)), true),
        Range(Field(UInt64(1)), true, Field(UInt64(3)), true),
        Range(Field(UInt64(0)), true, Field(UInt64(8)), true),
    };
    index.initialized = true;

    EXPECT_EQ(
        store("all_1_3_1_5", index, threeColumns()),
        (std::set<String>{"minmax_p.idx", "minmax__block_number.idx", "minmax__block_offset.idx"}));
}

TEST_F(MinMaxIndexStoreFixture, DoesNotWriteThroughAFileTheCallerCarriedOver)
{
    const NamesAndTypesList columns = {{"p", std::make_shared<DataTypeUInt64>()}};

    /// The source part's index, stored the usual way.
    IMergeTreeDataPart::MinMaxIndex source_index;
    source_index.hyperrectangle = {Range(Field(UInt64(7)), true, Field(UInt64(7)), true)};
    source_index.initialized = true;
    ASSERT_EQ(store("all_1_1_0", source_index, columns), (std::set<String>{"minmax_p.idx"}));

    const fs::path source_file = tmp_dir / "all_1_1_0" / "minmax_p.idx";
    String content_before;
    {
        ReadBufferFromFile in(source_file.string());
        readStringUntilEOF(content_before, in);
    }

    /// A mutation that does not rewrite the whole part hardlinks the source part's files into the new part
    /// and records them in the new part's checksums before the index is stored, so the file in the new part
    /// shares its inode with the source part.
    fs::create_directories(tmp_dir / "all_1_1_0_2");
    fs::create_hard_link(source_file, tmp_dir / "all_1_1_0_2" / "minmax_p.idx");

    MergeTreeDataPartChecksums checksums;
    checksums.addFile("minmax_p.idx", fs::file_size(source_file), {});

    /// The index the mutation goes on to store may differ from what is in the file - a repair rewrites the
    /// inherited range in memory. Writing it out through the hardlink would corrupt the source part: the
    /// file that is already in the checksums has to be left alone.
    IMergeTreeDataPart::MinMaxIndex inherited;
    inherited.hyperrectangle = {Range(Field(UInt64(1)), true, Field(UInt64(100)), true)};
    inherited.initialized = true;

    DataPartStorageOnDiskFull part_storage(volume, "", "all_1_1_0_2");
    auto written_files = inherited.store(columns, part_storage, checksums, settings);
    for (auto & file : written_files)
        file->finalize();

    EXPECT_TRUE(written_files.empty());

    String content_after;
    {
        ReadBufferFromFile in(source_file.string());
        readStringUntilEOF(content_after, in);
    }
    EXPECT_EQ(content_after, content_before);
}

TEST_F(MinMaxIndexStoreFixture, StopsAtTheFirstUnknownRangeInsteadOfSkippingIt)
{
    /// `_block_number` is unknown - the whole universe is what `load` gives a column whose file is missing -
    /// while `_block_offset` after it is known. Only the prefix that ends before the unknown column may be
    /// materialized: writing `minmax__block_offset.idx` here would leave the part with a hole that the width
    /// the readers work with cannot describe.
    IMergeTreeDataPart::MinMaxIndex index;
    index.hyperrectangle = {
        Range(Field(UInt64(7)), true, Field(UInt64(7)), true),
        Range::createWholeUniverse(),
        Range(Field(UInt64(0)), true, Field(UInt64(8)), true),
    };
    index.initialized = true;

    EXPECT_EQ(store("all_1_3_1_5", index, threeColumns()), (std::set<String>{"minmax_p.idx"}));
}

TEST_F(MinMaxIndexStoreFixture, StopsWhenTheIndexIsNarrowerThanTheColumnList)
{
    /// An index built before the column list was extended knows nothing about the columns that were added -
    /// they are not materialized for this part yet.
    IMergeTreeDataPart::MinMaxIndex index;
    index.hyperrectangle = {Range(Field(UInt64(7)), true, Field(UInt64(7)), true)};
    index.initialized = true;

    EXPECT_EQ(store("all_1_3_1_5", index, threeColumns()), (std::set<String>{"minmax_p.idx"}));
}

TEST_F(MinMaxIndexStoreFixture, StopsBeforeANullableColumnThatFollowsAnUnknownRange)
{
    const NamesAndTypesList columns = {
        {"p", std::make_shared<DataTypeUInt64>()},
        {"n", makeNullable(std::make_shared<DataTypeUInt64>())},
    };

    IMergeTreeDataPart::MinMaxIndex index;
    index.hyperrectangle = {Range::createWholeUniverse(), Range::createWholeUniverse()};
    index.initialized = true;

    EXPECT_TRUE(store("all_1_3_1_5", index, columns).empty());

    /// Why stopping matters and skipping the unknown column would not be enough: the guard that recognises
    /// an unknown range only fires for a column that cannot hold `NULL`, so a `Nullable` column after it is
    /// serialized as it is - and an infinite bound is written out as `NULL`.
    IMergeTreeDataPart::MinMaxIndex nullable_only;
    nullable_only.hyperrectangle = {Range::createWholeUniverse()};
    nullable_only.initialized = true;

    const NamesAndTypesList nullable_column = {{"n", makeNullable(std::make_shared<DataTypeUInt64>())}};
    ASSERT_EQ(store("all_1_3_1_6", nullable_only, nullable_column), (std::set<String>{"minmax_n.idx"}));

    ReadBufferFromFile in((tmp_dir / "all_1_3_1_6" / "minmax_n.idx").string());
    auto serialization = nullable_column.front().type->getDefaultSerialization();
    Field min_val;
    Field max_val;
    serialization->deserializeBinary(min_val, in, {});
    serialization->deserializeBinary(max_val, in, {});

    EXPECT_TRUE(min_val.isNull());
    EXPECT_TRUE(max_val.isNull());

    /// `load` maps a `NULL` bound to `+inf`, so the whole universe comes back as the range of a part that
    /// holds nothing but `NULL`s, and a predicate on that column would prune the part away from queries its
    /// rows belong to. The prefix rule is what keeps this state unreachable.
    if (min_val.isNull())
        min_val = POSITIVE_INFINITY;
    if (max_val.isNull())
        max_val = POSITIVE_INFINITY;

    const Range reloaded(min_val, true, max_val, true);
    EXPECT_TRUE(reloaded.left.isPositiveInfinity());
    EXPECT_TRUE(reloaded.right.isPositiveInfinity());
    EXPECT_FALSE(reloaded.intersectsRange(Range(Field(UInt64(0)), true, Field(UInt64(100)), true)));
}

}
