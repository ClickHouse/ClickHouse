#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>

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

namespace
{
    /// Reusable tempdir-backed part-storage fixture.
    struct PartStorageFixture
    {
        std::filesystem::path base_path;
        std::string part_dir;
        DiskPtr disk;
        VolumePtr volume;
        MutableDataPartStoragePtr storage;

        PartStorageFixture()
        {
            auto base = std::filesystem::temp_directory_path();
            auto unique_id = std::to_string(::getpid()) + "_"
                + std::to_string(reinterpret_cast<uintptr_t>(this));
            base_path = base / ("bitmap_file_ops_gtest_" + unique_id);
            std::filesystem::create_directories(base_path);
            part_dir = "part";
            std::filesystem::create_directories(base_path / part_dir);

            disk = std::make_shared<DiskLocal>("test_disk_" + unique_id, base_path.string());
            volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
            storage = std::make_shared<DataPartStorageOnDiskFull>(volume, /*root_path=*/"", part_dir);
        }

        ~PartStorageFixture()
        {
            std::error_code ec;
            std::filesystem::remove_all(base_path, ec);
        }

        std::filesystem::path partFile(const std::string & name) const
        {
            return base_path / part_dir / name;
        }
    };
}

/// ---------- enumerateFiles / pickHighest ----------

TEST(DeleteBitmapFileOpsTest, EnumerateFilesEmptyDirectoryReturnsEmpty)
{
    PartStorageFixture fx;
    EXPECT_TRUE(DeleteBitmapFileOps::enumerateFiles(*fx.storage).empty());
}

TEST(DeleteBitmapFileOpsTest, EnumerateFilesIgnoresUnrelatedFiles)
{
    PartStorageFixture fx;
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
    EXPECT_EQ(entries[0].first, 3u);
    EXPECT_EQ(entries[0].second, "delete_bitmap_3.rbm");
}

TEST(DeleteBitmapFileOpsTest, PickHighestSelectsLargestBlockNumber)
{
    std::vector<std::pair<UInt64, std::string>> files{
        {3, "delete_bitmap_3.rbm"},
        {7, "delete_bitmap_7.rbm"},
        {5, "delete_bitmap_5.rbm"},
    };
    auto chosen = DeleteBitmapFileOps::pickHighest(files);
    ASSERT_TRUE(chosen.has_value());
    EXPECT_EQ(chosen->first, 7u);
}

TEST(DeleteBitmapFileOpsTest, PickHighestEmptyReturnsNullopt)
{
    EXPECT_FALSE(DeleteBitmapFileOps::pickHighest({}).has_value());
}

/// ---------- write / read / current version ----------

TEST(DeleteBitmapFileOpsTest, WriteAndReadRoundtrip)
{
    PartStorageFixture fx;

    DeleteBitmap in;
    in.add(3);
    in.add(7);
    in.add(12345);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, /*version=*/5, in);

    /// File appears under the expected name.
    EXPECT_TRUE(std::filesystem::exists(fx.partFile("delete_bitmap_5.rbm")));
    /// No `.tmp` leftover.
    EXPECT_FALSE(std::filesystem::exists(fx.partFile("delete_bitmap_5.rbm.tmp")));

    auto loaded = DeleteBitmapFileOps::readBitmapFromStorage(*fx.storage, 5);
    ASSERT_NE(loaded, nullptr);
    EXPECT_EQ(loaded->cardinality(), 3u);
    EXPECT_TRUE(loaded->contains(3));
    EXPECT_TRUE(loaded->contains(7));
    EXPECT_TRUE(loaded->contains(12345));
}

TEST(DeleteBitmapFileOpsTest, GetCurrentVersionHighestWins)
{
    PartStorageFixture fx;
    EXPECT_EQ(DeleteBitmapFileOps::getCurrentVersionFromStorage(*fx.storage), 0u);

    DeleteBitmap bm;
    bm.add(1);

    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 3, bm);
    EXPECT_EQ(DeleteBitmapFileOps::getCurrentVersionFromStorage(*fx.storage), 3u);

    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 7, bm);
    EXPECT_EQ(DeleteBitmapFileOps::getCurrentVersionFromStorage(*fx.storage), 7u);

    /// Out-of-order write does not change "highest".
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 5, bm);
    EXPECT_EQ(DeleteBitmapFileOps::getCurrentVersionFromStorage(*fx.storage), 7u);
}

TEST(DeleteBitmapFileOpsTest, GetCurrentVersionEmptyStorageReturnsZero)
{
    /// Zero-state invariant: a part with no `.rbm` files must report
    /// version 0.
    PartStorageFixture fx;
    EXPECT_EQ(DeleteBitmapFileOps::getCurrentVersionFromStorage(*fx.storage), 0u);
}

TEST(DeleteBitmapFileOpsTest, ReadMissingVersionThrows)
{
    PartStorageFixture fx;
    EXPECT_ANY_THROW({
        auto _ = DeleteBitmapFileOps::readBitmapFromStorage(*fx.storage, 99);
    });
}

TEST(DeleteBitmapFileOpsTest, TmpFileAbsentAfterWrite)
{
    /// Atomic-rename semantics: after `writeBitmapToStorage` returns, the
    /// final file exists and the `.tmp` sibling does not.
    PartStorageFixture fx;
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
    PartStorageFixture fx;

    /// Plant a stale `.tmp` (fixture already created the part directory).
    {
        std::ofstream stale(fx.partFile("delete_bitmap_4.rbm.tmp").string());
        stale << "garbage";
    }
    ASSERT_TRUE(std::filesystem::exists(fx.partFile("delete_bitmap_4.rbm.tmp")));

    DeleteBitmap bm;
    bm.add(11);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 4, bm);

    /// Successful write: final file present, tmp gone.
    EXPECT_TRUE(std::filesystem::exists(fx.partFile("delete_bitmap_4.rbm")));
    EXPECT_FALSE(std::filesystem::exists(fx.partFile("delete_bitmap_4.rbm.tmp")));

    auto loaded = DeleteBitmapFileOps::readBitmapFromStorage(*fx.storage, 4);
    EXPECT_TRUE(loaded->contains(11));
}

TEST(DeleteBitmapFileOpsTest, OverwriteSameVersionIsIdempotent)
{
    /// `replaceFile` semantics: calling `writeBitmapToStorage` twice for the
    /// same version overwrites the previous file. Idempotent retry on flaky
    /// I/O lands the final bitmap.
    PartStorageFixture fx;

    DeleteBitmap first;
    first.add(1);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 2, first);

    DeleteBitmap second;
    second.add(1);
    second.add(2);
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, 2, second);

    auto loaded = DeleteBitmapFileOps::readBitmapFromStorage(*fx.storage, 2);
    EXPECT_EQ(loaded->cardinality(), 2u);
    EXPECT_TRUE(loaded->contains(1));
    EXPECT_TRUE(loaded->contains(2));
}
