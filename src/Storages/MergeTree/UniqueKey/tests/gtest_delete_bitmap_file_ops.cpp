#include <gtest/gtest.h>

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/tests/gtest_part_storage_fixture.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

using namespace DB;

/// UNIQUE KEY gtests for `DeleteBitmapFileOps` — atomic write, read,
/// version discovery, and `delete_bitmap_{N}.rbm` enumeration over a
/// `DiskLocal`-backed `IDataPartStorage`.

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
        std::ofstream f2(fx.partFile("delete_bitmap_5_for_all_1_1_0.rbm.tmp").string());
        f2 << "y"; /// `.tmp` sibling — not a finalized version
    }

    DeleteBitmap bm;
    bm.add(0);
    writeCarried(*fx.storage, /*version=*/3, "all_1_1_0", bm);

    auto entries = DeleteBitmapFileOps::enumerateFiles(*fx.storage);
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].version, 3u);
    EXPECT_EQ(entries[0].fileName(), "delete_bitmap_3_for_all_1_1_0.rbm");
}

/// ---------- write / read ----------

TEST(DeleteBitmapFileOpsTest, WriteAndReadRoundtrip)
{
    PartStorageFixture fx{"file_ops"};

    DeleteBitmap in;
    in.add(3);
    in.add(7);
    in.add(12345);
    writeCarried(*fx.storage, /*version=*/5, "all_1_1_0", in);

    EXPECT_TRUE(std::filesystem::exists(fx.partFile("delete_bitmap_5_for_all_1_1_0.rbm")));
    EXPECT_FALSE(std::filesystem::exists(fx.partFile("delete_bitmap_5_for_all_1_1_0.rbm.tmp")));

    auto loaded = DeleteBitmapFileOps::tryReadBitmap(*fx.storage, {5, "all_1_1_0"});
    ASSERT_NE(loaded, nullptr);
    EXPECT_EQ(loaded->cardinality(), 3u);
    EXPECT_TRUE(loaded->contains(3));
    EXPECT_TRUE(loaded->contains(7));
    EXPECT_TRUE(loaded->contains(12345));
}

TEST(DeleteBitmapFileOpsTest, ReadMissingVersionReportsAbsence)
{
    /// Null, not a throw: the caller has to tell "no such version" from a read that failed, and
    /// only the store knows whether the index promised this one.
    PartStorageFixture fx{"file_ops"};
    EXPECT_EQ(DeleteBitmapFileOps::tryReadBitmap(*fx.storage, {99, "all_1_1_0"}), nullptr);
}


TEST(DeleteBitmapFileOpsTest, WriteClearsStaleTmpLeftover)
{
    /// Simulate a crash-left-over `.tmp` from a previous run. A fresh
    /// `carryBitmap` must not fail because of it — the
    /// implementation `removeFileIfExists` the tmp before opening.
    PartStorageFixture fx{"file_ops"};

    /// Plant a stale `.tmp` (fixture already created the part directory).
    {
        std::ofstream stale(fx.partFile("delete_bitmap_4_for_all_1_1_0.rbm.tmp").string());
        stale << "garbage";
    }
    ASSERT_TRUE(std::filesystem::exists(fx.partFile("delete_bitmap_4_for_all_1_1_0.rbm.tmp")));

    DeleteBitmap bm;
    bm.add(11);
    writeCarried(*fx.storage, 4, "all_1_1_0", bm);

    EXPECT_TRUE(std::filesystem::exists(fx.partFile("delete_bitmap_4_for_all_1_1_0.rbm")));
    EXPECT_FALSE(std::filesystem::exists(fx.partFile("delete_bitmap_4_for_all_1_1_0.rbm.tmp")));

    auto loaded = DeleteBitmapFileOps::tryReadBitmap(*fx.storage, {4, "all_1_1_0"});
    ASSERT_NE(loaded, nullptr);
    EXPECT_TRUE(loaded->contains(11));
}

TEST(DeleteBitmapFileOpsTest, OverwriteSameVersionIsIdempotent)
{
    /// `replaceFile` semantics: calling `carryBitmap` twice for the
    /// same version overwrites the previous file. Idempotent retry on flaky
    /// I/O lands the final bitmap.
    PartStorageFixture fx{"file_ops"};

    DeleteBitmap first;
    first.add(1);
    writeCarried(*fx.storage, 2, "all_1_1_0", first);

    DeleteBitmap second;
    second.add(1);
    second.add(2);
    writeCarried(*fx.storage, 2, "all_1_1_0", second);

    auto loaded = DeleteBitmapFileOps::tryReadBitmap(*fx.storage, {2, "all_1_1_0"});
    ASSERT_NE(loaded, nullptr);
    EXPECT_EQ(loaded->cardinality(), 2u);
    EXPECT_TRUE(loaded->contains(1));
    EXPECT_TRUE(loaded->contains(2));
}

/// ---------- tolerant reads ----------


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

TEST(DeleteBitmapFileOpsTest, RemoveReportsWhetherTheFileWasThere)
{
    PartStorageFixture fx{"file_ops"};

    DeleteBitmap bm;
    bm.add(1);
    const DeleteBitmapFileOps::BitmapFile file{3, "all_1_1_0"};
    writeCarried(*fx.storage, file.version, file.target, bm);

    EXPECT_TRUE(DeleteBitmapFileOps::removeBitmapFile(*fx.storage, file));
    EXPECT_FALSE(std::filesystem::exists(fx.partFile(file.fileName())));
    /// The gc counts what it unlinked, so a version whose file is already gone must report false
    EXPECT_FALSE(DeleteBitmapFileOps::removeBitmapFile(*fx.storage, file));
}
