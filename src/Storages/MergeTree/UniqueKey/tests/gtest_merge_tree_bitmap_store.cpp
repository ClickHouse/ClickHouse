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
    };

    DeleteBitmapCachePtr makeCache(size_t bytes = 8 * 1024 * 1024)
    {
        return std::make_shared<DeleteBitmapCache>(
            "LRU",
            CurrentMetrics::DeleteBitmapCacheBytes,
            CurrentMetrics::DeleteBitmapCacheEntries,
            /*max_size_in_bytes=*/bytes,
            /*size_ratio=*/0.5);
    }

    DeleteBitmap bitmapWithRow(UInt64 row)
    {
        DeleteBitmap bm;
        bm.add(row);
        return bm;
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
    MergeTreeBitmapStore store{/*cache=*/nullptr};
    store.installBitmap(*fx.storage, "p", "part", 3, bitmapWithRow(30));
    store.installBitmap(*fx.storage, "p", "part", 7, bitmapWithRow(70));
    store.installBitmap(*fx.storage, "p", "part", 10, bitmapWithRow(100));

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
    auto cache = makeCache();
    MergeTreeBitmapStore store{cache};
    store.installBitmap(*fx.storage, "p", "part", /*csn=*/11, bitmapWithRow(77));

    auto [first, _v1] = store.readBitmap(*fx.storage, /*snapshot_csn=*/100, "p");
    auto [second, _v2] = store.readBitmap(*fx.storage, /*snapshot_csn=*/100, "p");
    EXPECT_EQ(first.get(), second.get());
}

TEST(MergeTreeBitmapStoreTest, GcObsoleteRemovesVbWhenVnextLeqOldest)
{
    PartStorageFixture fx;
    MergeTreeBitmapStore store{/*cache=*/nullptr};
    store.installBitmap(*fx.storage, "p", "part", 3, bitmapWithRow(30));
    store.installBitmap(*fx.storage, "p", "part", 5, bitmapWithRow(50));
    store.installBitmap(*fx.storage, "p", "part", 7, bitmapWithRow(70));
    store.installBitmap(*fx.storage, "p", "part", 10, bitmapWithRow(100));

    /// (3,5) and (5,7) qualify; (7,10) keeps 7 because V_next=10 > 7.
    /// 10 is the newest committed → never V_b.
    EXPECT_EQ(store.gcObsoleteBitmaps(*fx.storage, "p", /*committed_csn=*/10, /*oldest_snapshot_csn=*/7), 2u);

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
    MergeTreeBitmapStore store{/*cache=*/nullptr};
    store.installBitmap(*fx.storage, "p", "part", 3, bitmapWithRow(30));
    store.installBitmap(*fx.storage, "p", "part", 5, bitmapWithRow(50));
    store.installBitmap(*fx.storage, "p", "part", 100, bitmapWithRow(1000));

    EXPECT_EQ(store.gcObsoleteBitmaps(
        *fx.storage, "p",
        /*committed_csn=*/5, /*oldest_snapshot_csn=*/std::numeric_limits<UInt64>::max()), 1u);

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
    auto cache = makeCache();
    MergeTreeBitmapStore store{cache};
    store.installBitmap(*fx.storage, "p", "part", /*csn=*/9, bitmapWithRow(90));

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

TEST(MergeTreeBitmapStoreTest, DropPartEvictsCacheAfterInstallInvalidatedVersionIndex)
{
    PartStorageFixture fx;
    auto cache = makeCache();
    MergeTreeBitmapStore store{cache};

    /// Install v3 and read it so the content cache holds an entry for (p, 3).
    store.installBitmap(*fx.storage, "p", "part", /*csn=*/3, bitmapWithRow(30));
    auto [bm3, v3] = store.readBitmap(*fx.storage, /*snapshot_csn=*/3, "p");
    ASSERT_EQ(v3, 3u);
    ASSERT_NE(cache->get(DeleteBitmapCache::makeKey("p", 3)), nullptr);

    /// Installing a newer version invalidates the store's in-memory version index for "p".
    store.installBitmap(*fx.storage, "p", "part", /*csn=*/7, bitmapWithRow(70));

    /// dropPart must still evict the cached (p, 3) entry even though the version index is gone.
    /// Regression: dropPart used to early-return when the index entry was absent, leaving a stale
    /// bitmap that a reused disk:path identity could read.
    store.dropPart("p");
    EXPECT_EQ(cache->get(DeleteBitmapCache::makeKey("p", 3)), nullptr);
}

/// `installBitmap` rejects bad input (non-monotonic csn, or csn 0) with a LOGICAL_ERROR, which
/// aborts in debug/sanitizer builds and throws otherwise. Death tests are unreliable under the
/// sanitizer fork-in-threaded-context, so exercise the rejection only on the throw path
/// (coverage/release lanes) and skip where it aborts.
#ifdef DEBUG_OR_SANITIZER_BUILD
#define EXPECT_INSTALL_REJECTS(stmt) GTEST_SKIP() << "installBitmap LOGICAL_ERROR aborts in debug/sanitizer builds"
#else
#define EXPECT_INSTALL_REJECTS(stmt) EXPECT_THROW(stmt, DB::Exception)
#endif

TEST(MergeTreeBitmapStoreTest, InstallBitmapMonotonicityRejected)
{
    PartStorageFixture fx;
    MergeTreeBitmapStore store{/*cache=*/nullptr};
    store.installBitmap(*fx.storage, "p", "part", /*csn=*/10, bitmapWithRow(1));

    EXPECT_INSTALL_REJECTS(store.installBitmap(*fx.storage, "p", "part", /*csn=*/10, bitmapWithRow(2)));
    EXPECT_INSTALL_REJECTS(store.installBitmap(*fx.storage, "p", "part", /*csn=*/5, bitmapWithRow(3)));
}

TEST(MergeTreeBitmapStoreTest, InstallBitmapRejectsCsnZero)
{
    /// CSN 0 is the no-bitmap sentinel `readBitmap` returns, so it must never name a real bitmap.
    PartStorageFixture fx;
    MergeTreeBitmapStore store{/*cache=*/nullptr};
    EXPECT_INSTALL_REJECTS(store.installBitmap(*fx.storage, "p", "part", /*csn=*/0, bitmapWithRow(1)));
}

/// Write delete_bitmap_5.rbm / _8.rbm via one store, leaving a fresh store with a cold (empty)
/// version index — the post-restart state. Each test below builds on this helper.
namespace
{
    void seedVersions5And8(const PartStorageFixture & fx)
    {
        MergeTreeBitmapStore store1{/*cache=*/nullptr};
        store1.installBitmap(*fx.storage, "p", "part", /*csn=*/5, bitmapWithRow(50));
        store1.installBitmap(*fx.storage, "p", "part", /*csn=*/8, bitmapWithRow(80));
    }
}

TEST(MergeTreeBitmapStoreTest, InstallSelfWarmsColdIndexFromDisk)
{
    /// A cold install must self-load the part's on-disk history (same self-heal `readBitmap` does),
    /// not clobber it. Fresh store2 installs csn 9 over on-disk 5/8; the 5/8 versions must remain
    /// readable. This is the half that proves the fix and runs in every build.
    PartStorageFixture fx;
    seedVersions5And8(fx);

    MergeTreeBitmapStore store2{/*cache=*/nullptr};
    store2.installBitmap(*fx.storage, "p", "part", /*csn=*/9, bitmapWithRow(90));
    {
        auto [bm, v] = store2.readBitmap(*fx.storage, /*snapshot_csn=*/7, "p");
        EXPECT_EQ(v, 5u);
        EXPECT_TRUE(bm->contains(50));
    }
    {
        auto [bm, v] = store2.readBitmap(*fx.storage, /*snapshot_csn=*/9, "p");
        EXPECT_EQ(v, 9u);
        EXPECT_TRUE(bm->contains(90));
    }
}

TEST(MergeTreeBitmapStoreTest, InstallMonotonicityFiresOnColdIndex)
{
    /// The monotonicity guard must fire even cold: a fresh store over on-disk 5/8 must reject an
    /// install at csn 5 (≤ on-disk max 8). Without the self-warm fix the cold index is empty and
    /// this silently succeeds. Gated by EXPECT_INSTALL_REJECTS — the guard is a LOGICAL_ERROR,
    /// which aborts in debug/sanitizer (skipped there) and throws otherwise.
    PartStorageFixture fx;
    seedVersions5And8(fx);

    MergeTreeBitmapStore store3{/*cache=*/nullptr};
    EXPECT_INSTALL_REJECTS(store3.installBitmap(*fx.storage, "p", "part", /*csn=*/5, bitmapWithRow(51)));
}

TEST(MergeTreeBitmapStoreTest, CorruptFileSurfacesError)
{
    PartStorageFixture fx;
    auto file_path = fx.partFile(DeleteBitmap::fileNameForCSN(1));
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
