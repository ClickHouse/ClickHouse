#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/MergeTreeBitmapStore.h>
#include <Common/CurrentMetrics.h>

#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>

namespace CurrentMetrics
{
    extern const Metric DeleteBitmapCacheBytes;
    extern const Metric DeleteBitmapCacheEntries;
}

using namespace DB;

namespace
{
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
            auto unique_id = std::to_string(::getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this));
            base_path = base / ("bitmap_store_gtest_" + unique_id);
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

        std::filesystem::path partFile(const std::string & name) const { return base_path / part_dir / name; }

        void writeBitmap(UInt64 csn, UInt64 row)
        {
            DeleteBitmap bm;
            bm.add(row);
            WriteBufferFromFile out(partFile(DeleteBitmap::fileNameForCsn(csn)).string());
            bm.serialize(out);
            out.finalize();
        }
    };

    DeleteBitmapCache makeCache(size_t bytes = 8 * 1024 * 1024)
    {
        return DeleteBitmapCache(
            "LRU",
            CurrentMetrics::DeleteBitmapCacheBytes,
            CurrentMetrics::DeleteBitmapCacheEntries,
            /*max_size_in_bytes=*/bytes,
            /*size_ratio=*/0.5);
    }
}

TEST(MergeTreeBitmapStoreTest, ReadBitmapEmptyDirReturnsZero)
{
    PartStorageFixture fx;
    MergeTreeBitmapStore store{/*cache=*/nullptr};
    auto [bm, v] = store.readBitmap(*fx.storage, /*snapshot_csn=*/100, "p");
    ASSERT_NE(bm, nullptr);
    EXPECT_TRUE(bm->empty());
    EXPECT_EQ(v, 0u);
}

TEST(MergeTreeBitmapStoreTest, ReadBitmapPicksMaxLeqSnapshot)
{
    PartStorageFixture fx;
    fx.writeBitmap(3, 30);
    fx.writeBitmap(7, 70);
    fx.writeBitmap(10, 100);

    MergeTreeBitmapStore store{/*cache=*/nullptr};

    /// snapshot above the last installed csn → newest.
    {
        auto [bm, v] = store.readBitmap(*fx.storage, /*snapshot_csn=*/15, "p");
        EXPECT_EQ(v, 10u);
        EXPECT_TRUE(bm->contains(100));
    }
    /// snapshot between two csns → predecessor.
    {
        auto [bm, v] = store.readBitmap(*fx.storage, /*snapshot_csn=*/8, "p");
        EXPECT_EQ(v, 7u);
        EXPECT_TRUE(bm->contains(70));
        EXPECT_FALSE(bm->contains(30));
    }
    /// snapshot equals an installed csn → that csn (boundary inclusive).
    {
        auto [bm, v] = store.readBitmap(*fx.storage, /*snapshot_csn=*/7, "p");
        EXPECT_EQ(v, 7u);
    }
    /// snapshot below every installed csn → empty.
    {
        auto [bm, v] = store.readBitmap(*fx.storage, /*snapshot_csn=*/2, "p");
        EXPECT_EQ(v, 0u);
        EXPECT_TRUE(bm->empty());
    }
}

TEST(MergeTreeBitmapStoreTest, ReadBitmapUsesCache)
{
    PartStorageFixture fx;
    fx.writeBitmap(/*csn=*/11, /*row=*/77);

    auto cache = makeCache();
    MergeTreeBitmapStore store{&cache};

    auto [first, _v1] = store.readBitmap(*fx.storage, /*snapshot_csn=*/100, "p");
    auto [second, _v2] = store.readBitmap(*fx.storage, /*snapshot_csn=*/100, "p");
    EXPECT_EQ(first.get(), second.get());
}

TEST(MergeTreeBitmapStoreTest, GcObsoleteRemovesVbWhenVnextLeqOldest)
{
    PartStorageFixture fx;
    fx.writeBitmap(3, 30);
    fx.writeBitmap(5, 50);
    fx.writeBitmap(7, 70);
    fx.writeBitmap(10, 100);

    MergeTreeBitmapStore store{/*cache=*/nullptr};
    /// (3,5) and (5,7) qualify; (7,10) keeps 7 because V_next=10 > 7.
    /// 10 is the newest committed → never V_b.
    EXPECT_EQ(store.gcObsoleteBitmaps(*fx.storage, "p", /*committed=*/10, /*oldest=*/7), 2u);

    auto survivors = DeleteBitmapFileOps::enumerateFiles(*fx.storage);
    ASSERT_EQ(survivors.size(), 2u);
    std::sort(survivors.begin(), survivors.end(),
              [](const auto & a, const auto & b) { return a.version < b.version; });
    EXPECT_EQ(survivors[0].version, 7u);
    EXPECT_EQ(survivors[1].version, 10u);
}

TEST(MergeTreeBitmapStoreTest, GcObsoleteRespectsCommittedFilter)
{
    /// In-flight bitmap (csn > committed_csn) must not act as V_next —
    /// otherwise GC would unlink its committed predecessor.
    PartStorageFixture fx;
    fx.writeBitmap(3, 30);
    fx.writeBitmap(5, 50);
    fx.writeBitmap(100, 1000);

    MergeTreeBitmapStore store{/*cache=*/nullptr};
    EXPECT_EQ(store.gcObsoleteBitmaps(
        *fx.storage, "p",
        /*committed=*/5, /*oldest=*/std::numeric_limits<UInt64>::max()), 1u);

    auto survivors = DeleteBitmapFileOps::enumerateFiles(*fx.storage);
    ASSERT_EQ(survivors.size(), 2u);
    std::sort(survivors.begin(), survivors.end(),
              [](const auto & a, const auto & b) { return a.version < b.version; });
    EXPECT_EQ(survivors[0].version, 5u);
    EXPECT_EQ(survivors[1].version, 100u);
}

TEST(MergeTreeBitmapStoreTest, DropPartErasesInMemoryStateAndCache)
{
    PartStorageFixture fx;
    fx.writeBitmap(/*csn=*/9, /*row=*/90);

    auto cache = makeCache();
    MergeTreeBitmapStore store{&cache};

    auto [bm_first, _v_first] = store.readBitmap(*fx.storage, /*snapshot_csn=*/100, "p");
    store.dropPart("p");
    /// Idempotent / unknown part_id is harmless.
    store.dropPart("p");
    store.dropPart("never-seen");

    /// After drop, the next read re-populates from disk; the cached
    /// shared_ptr was invalidated so this is a fresh object.
    auto [bm_after, _v_after] = store.readBitmap(*fx.storage, /*snapshot_csn=*/100, "p");
    EXPECT_NE(bm_after.get(), bm_first.get());
}

TEST(MergeTreeBitmapStoreTest, CorruptFileSurfacesError)
{
    PartStorageFixture fx;
    auto file_path = fx.partFile(DeleteBitmap::fileNameForCsn(1));
    {
        WriteBufferFromFile out(file_path.string());
        DeleteBitmap bm;
        bm.add(1);
        bm.serialize(out);
        out.finalize();
    }
    String contents;
    {
        ReadBufferFromFile rb(file_path.string());
        while (!rb.eof())
        {
            char buf[4096];
            size_t n = rb.read(buf, sizeof(buf));
            contents.append(buf, n);
        }
    }
    ASSERT_GT(contents.size(), 16u);
    contents[contents.size() / 2] ^= 0x7E;
    {
        WriteBufferFromFile out(file_path.string());
        out.write(contents.data(), contents.size());
        out.finalize();
    }

    MergeTreeBitmapStore store{/*cache=*/nullptr};
    EXPECT_ANY_THROW({
        auto _ = store.readBitmap(*fx.storage, /*snapshot_csn=*/100, "p");
    });
}
