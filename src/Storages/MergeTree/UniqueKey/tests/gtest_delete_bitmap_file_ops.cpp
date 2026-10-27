#include <gtest/gtest.h>

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/tests/gtest_part_storage_fixture.h>

#include <fstream>
#include <memory>
#include <string>
#include <vector>

using namespace DB;

/// `DeleteBitmapFileOps` over a `DiskLocal`-backed part: `carryBitmap`, `tryReadBitmap`, `enumerateFiles`.

/// ---------- enumerateFiles ----------

TEST(DeleteBitmapFileOpsTest, EnumerateFilesEmptyDirectoryReturnsEmpty)
{
    PartStorageFixture fx{"file_ops"};
    EXPECT_TRUE(DeleteBitmapFileOps::enumerateFiles(*fx.storage).empty());
}

TEST(DeleteBitmapFileOpsTest, EnumerateFilesIgnoresUnrelatedFiles)
{
    PartStorageFixture fx{"file_ops"};
    /// Plant unrelated files alongside the part directory contents.
    {
        std::ofstream f1(fx.partFile("columns.txt").string());
        f1 << "x";
        std::ofstream f2(fx.partFile("delete_bitmap_5_for_all_1_1_0.rbm.bak").string());
        f2 << "y"; /// a suffix after `.rbm` is not a bitmap
    }

    DeleteBitmap bm;
    bm.add(0);
    writeCarried(*fx.storage, /*version=*/3, "all_1_1_0", bm);

    auto entries = DeleteBitmapFileOps::enumerateFiles(*fx.storage);
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].version, 3u);
    EXPECT_EQ(entries[0].fileName(), "delete_bitmap_3_for_all_1_1_0.rbm");
}

/// ---------- tolerant reads ----------

TEST(DeleteBitmapFileOpsTest, ReadMissingVersionReportsAbsence)
{
    /// Null, not a throw: the caller has to tell "no such version" from a read that failed, and
    /// only the store knows whether the index promised this one.
    PartStorageFixture fx{"file_ops"};
    EXPECT_EQ(DeleteBitmapFileOps::tryReadBitmap(*fx.storage, {99, "all_1_1_0"}), nullptr);
}

/// ---------- the carried name ----------

TEST(DeleteBitmapFileOpsTest, ACarriedBitmapRoundTripsUnderItsOwnVersion)
{
    PartStorageFixture fx{"file_ops"};

    DeleteBitmap carried;
    carried.add(2);
    carried.add(9);
    writeCarried(*fx.storage, /*version=*/12, "all_1_1_0", carried);

    auto loaded = DeleteBitmapFileOps::tryReadBitmap(*fx.storage, {12, "all_1_1_0"});
    ASSERT_NE(loaded, nullptr);
    EXPECT_TRUE(loaded->contains(2));
    EXPECT_TRUE(loaded->contains(9));

    /// Version and target both address it: neither alone finds the file.
    EXPECT_EQ(DeleteBitmapFileOps::tryReadBitmap(*fx.storage, {11, "all_1_1_0"}), nullptr);
    EXPECT_EQ(DeleteBitmapFileOps::tryReadBitmap(*fx.storage, {0, "all_1_1_0"}), nullptr);

    /// And it enumerates as a sidecar that knows its own version, not as one taking its holder's.
    const auto files = DeleteBitmapFileOps::enumerateFiles(*fx.storage);
    ASSERT_EQ(files.size(), 1u);
    EXPECT_TRUE(files[0].isCarried());
    EXPECT_EQ(files[0].version, 12u);
    EXPECT_EQ(files[0].target, "all_1_1_0");
    EXPECT_EQ(files[0].toString(), "12_for_all_1_1_0");
}
