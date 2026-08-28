#include <gtest/gtest.h>

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/tests/gtest_part_storage_fixture.h>

#include <algorithm>
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
        std::ofstream f2(fx.partFile("delete_bitmap_5.rbm.tmp").string());
        f2 << "y"; /// `.tmp` sibling — not a finalized version
    }

    DeleteBitmap bm;
    bm.add(0);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, /*version=*/3, bm);

    auto entries = DeleteBitmapFileOps::enumerateFiles(*fx.storage);
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].version, 3u);
    EXPECT_EQ(entries[0].name, "delete_bitmap_3.rbm");
}

/// ---------- write / read ----------

TEST(DeleteBitmapFileOpsTest, WriteAndReadRoundtrip)
{
    PartStorageFixture fx{"file_ops"};

    DeleteBitmap in;
    in.add(3);
    in.add(7);
    in.add(12345);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, /*version=*/5, in);

    EXPECT_TRUE(std::filesystem::exists(fx.partFile("delete_bitmap_5.rbm")));
    EXPECT_FALSE(std::filesystem::exists(fx.partFile("delete_bitmap_5.rbm.tmp")));

    auto loaded = DeleteBitmapFileOps::readBitmapFile(*fx.storage, DeleteBitmap::fileNameForCSN(5));
    ASSERT_NE(loaded, nullptr);
    EXPECT_EQ(loaded->cardinality(), 3u);
    EXPECT_TRUE(loaded->contains(3));
    EXPECT_TRUE(loaded->contains(7));
    EXPECT_TRUE(loaded->contains(12345));
}

TEST(DeleteBitmapFileOpsTest, ReadMissingVersionThrows)
{
    PartStorageFixture fx{"file_ops"};
    EXPECT_ANY_THROW({
        auto _ = DeleteBitmapFileOps::readBitmapFile(*fx.storage, DeleteBitmap::fileNameForCSN(99));
    });
}

TEST(DeleteBitmapFileOpsTest, TmpFileAbsentAfterWrite)
{
    /// Atomic-rename semantics: after `writeBitmapToStorage` returns, the
    /// final file exists and the `.tmp` sibling does not.
    PartStorageFixture fx{"file_ops"};
    DeleteBitmap bm;
    bm.add(42);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 9, bm);

    EXPECT_TRUE(std::filesystem::exists(fx.partFile("delete_bitmap_9.rbm")));
    EXPECT_FALSE(std::filesystem::exists(fx.partFile("delete_bitmap_9.rbm.tmp")));
}

TEST(DeleteBitmapFileOpsTest, WriteClearsStaleTmpLeftover)
{
    /// Simulate a crash-left-over `.tmp` from a previous run. A fresh
    /// `writeBitmapToStorage` must not fail because of it — the
    /// implementation `removeFileIfExists` the tmp before opening.
    PartStorageFixture fx{"file_ops"};

    /// Plant a stale `.tmp` (fixture already created the part directory).
    {
        std::ofstream stale(fx.partFile("delete_bitmap_4.rbm.tmp").string());
        stale << "garbage";
    }
    ASSERT_TRUE(std::filesystem::exists(fx.partFile("delete_bitmap_4.rbm.tmp")));

    DeleteBitmap bm;
    bm.add(11);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 4, bm);

    EXPECT_TRUE(std::filesystem::exists(fx.partFile("delete_bitmap_4.rbm")));
    EXPECT_FALSE(std::filesystem::exists(fx.partFile("delete_bitmap_4.rbm.tmp")));

    auto loaded = DeleteBitmapFileOps::readBitmapFile(*fx.storage, DeleteBitmap::fileNameForCSN(4));
    EXPECT_TRUE(loaded->contains(11));
}

TEST(DeleteBitmapFileOpsTest, OverwriteSameVersionIsIdempotent)
{
    /// `replaceFile` semantics: calling `writeBitmapToStorage` twice for the
    /// same version overwrites the previous file. Idempotent retry on flaky
    /// I/O lands the final bitmap.
    PartStorageFixture fx{"file_ops"};

    DeleteBitmap first;
    first.add(1);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 2, first);

    DeleteBitmap second;
    second.add(1);
    second.add(2);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 2, second);

    auto loaded = DeleteBitmapFileOps::readBitmapFile(*fx.storage, DeleteBitmap::fileNameForCSN(2));
    EXPECT_EQ(loaded->cardinality(), 2u);
    EXPECT_TRUE(loaded->contains(1));
    EXPECT_TRUE(loaded->contains(2));
}

/// ---------- tolerant reads ----------

TEST(DeleteBitmapFileOpsTest, TryReadIsNullForAMissingFile)
{
    PartStorageFixture fx{"file_ops"};
    EXPECT_EQ(DeleteBitmapFileOps::tryReadVersion(*fx.storage, 7), nullptr);
    EXPECT_EQ(DeleteBitmapFileOps::tryReadStagedFor(*fx.storage, "all_1_1_0"), nullptr);

    DeleteBitmap bm;
    bm.add(4);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 7, bm);

    auto loaded = DeleteBitmapFileOps::tryReadVersion(*fx.storage, 7);
    ASSERT_NE(loaded, nullptr);
    EXPECT_TRUE(loaded->contains(4));
}

/// ---------- settleStagedFile ----------

TEST(DeleteBitmapFileOpsTest, SettleStagedFilePublishesThenUnlinks)
{
    PartStorageFixture owner;
    PartStorageFixture target{"file_ops_target"};

    DeleteBitmap staged;
    staged.add(2);
    staged.add(9);
    DeleteBitmapFileOps::writeStagedBitmapToStorage(*owner.storage, "all_1_1_0", staged);
    const String staged_name = DeleteBitmap::fileNameForStagedTarget("all_1_1_0");

    DeleteBitmapFileOps::settleStagedFile(*owner.storage, staged_name, *target.storage, /*version=*/12);

    auto published = DeleteBitmapFileOps::tryReadVersion(*target.storage, 12);
    ASSERT_NE(published, nullptr);
    EXPECT_TRUE(published->contains(2));
    EXPECT_TRUE(published->contains(9));
    /// The staged copy is the durable record until the target has the bytes, and not after
    EXPECT_FALSE(std::filesystem::exists(owner.partFile(staged_name)));
}

TEST(DeleteBitmapFileOpsTest, SettleStagedFileLeavesAVersionTheTargetAlreadyHas)
{
    /// A settle retried past a crash that published but did not unlink. Re-publishing would be
    /// harmless here, but a target version is immutable once written, so prove it is not touched.
    PartStorageFixture owner;
    PartStorageFixture target{"file_ops_target"};

    DeleteBitmap published;
    published.add(5);
    DeleteBitmapFileOps::writeBitmapToStorage(*target.storage, 12, published);

    DeleteBitmap staged;
    staged.add(77);
    DeleteBitmapFileOps::writeStagedBitmapToStorage(*owner.storage, "all_1_1_0", staged);
    const String staged_name = DeleteBitmap::fileNameForStagedTarget("all_1_1_0");

    DeleteBitmapFileOps::settleStagedFile(*owner.storage, staged_name, *target.storage, /*version=*/12);

    auto loaded = DeleteBitmapFileOps::tryReadVersion(*target.storage, 12);
    ASSERT_NE(loaded, nullptr);
    EXPECT_EQ(loaded->cardinality(), 1u);
    EXPECT_TRUE(loaded->contains(5));
    EXPECT_FALSE(std::filesystem::exists(owner.partFile(staged_name)));
}

TEST(DeleteBitmapFileOpsTest, RemoveVersionReportsWhetherTheFileWasThere)
{
    PartStorageFixture fx{"file_ops"};

    DeleteBitmap bm;
    bm.add(1);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 3, bm);

    EXPECT_TRUE(DeleteBitmapFileOps::removeVersion(*fx.storage, 3));
    EXPECT_FALSE(std::filesystem::exists(fx.partFile(DeleteBitmap::fileNameForCSN(3))));
    /// The gc counts what it unlinked, so a version whose file is already gone must report false
    EXPECT_FALSE(DeleteBitmapFileOps::removeVersion(*fx.storage, 3));
}
