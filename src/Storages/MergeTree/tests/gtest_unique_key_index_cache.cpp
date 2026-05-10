#include <gtest/gtest.h>

#include "config.h"

#include <Storages/MergeTree/UniqueKey/UniqueKeyIndexCache.h>
#include <Common/ProfileEvents.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#if USE_ROCKSDB
#include <rocksdb/advanced_cache.h>
#include <rocksdb/slice.h>
#endif

using namespace DB;

#if USE_ROCKSDB

namespace ProfileEvents
{
    extern const Event UniqueKeyIndexCacheHits;
    extern const Event UniqueKeyIndexCacheMisses;
}

namespace
{

struct FakeObject
{
    std::atomic<int> * destructs;
    int value;
    ~FakeObject() { if (destructs) destructs->fetch_add(1); }
};

void fakeDeleter(void * obj, rocksdb::MemoryAllocator * /*a*/)
{
    delete static_cast<FakeObject *>(obj);
}

const rocksdb::Cache::CacheItemHelper kTestHelper{
    rocksdb::CacheEntryRole::kDataBlock, fakeDeleter};

UniqueKeyIndexCache makeCache(size_t bytes)
{
    return UniqueKeyIndexCache("SLRU",
                               CurrentMetrics::end(), CurrentMetrics::end(),
                               bytes, /*size_ratio=*/0.5);
}

}

TEST(UniqueKeyIndexCache, InsertLookupAndHitMissCounters)
{
    UniqueKeyIndexCache cache = makeCache(1 << 20);

    const auto hits_before = ProfileEvents::global_counters[ProfileEvents::UniqueKeyIndexCacheHits].load();
    const auto misses_before = ProfileEvents::global_counters[ProfileEvents::UniqueKeyIndexCacheMisses].load();

    auto * obj = new FakeObject{nullptr, 42};
    rocksdb::Slice key("key-1");
    rocksdb::Cache::Handle * ih = nullptr;
    auto st = cache.Insert(key, obj, &kTestHelper, sizeof(*obj),
                           &ih, rocksdb::Cache::Priority::LOW,
                           rocksdb::Slice(), rocksdb::CompressionType::kNoCompression);
    ASSERT_TRUE(st.ok());
    ASSERT_NE(ih, nullptr);
    EXPECT_EQ(static_cast<FakeObject *>(cache.Value(ih))->value, 42);

    auto * lh = cache.Lookup(key, nullptr, nullptr, rocksdb::Cache::Priority::LOW, nullptr);
    ASSERT_NE(lh, nullptr);
    EXPECT_EQ(static_cast<FakeObject *>(cache.Value(lh))->value, 42);
    auto * miss = cache.Lookup(rocksdb::Slice("absent"), nullptr, nullptr,
                               rocksdb::Cache::Priority::LOW, nullptr);
    EXPECT_EQ(miss, nullptr);

    /// Coverage build's ProfileEvents propagate into a thread-local subtree
    /// that doesn't reach `global_counters`; deltas read 0 there only.
#if !WITH_COVERAGE
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqueKeyIndexCacheHits].load() - hits_before, 1u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqueKeyIndexCacheMisses].load() - misses_before, 1u);
#else
    (void)hits_before;
    (void)misses_before;
#endif

    cache.Release(lh, /*erase_if_last_ref=*/false);
    cache.Release(ih, /*erase_if_last_ref=*/false);
    cache.EraseUnRefEntries();
}

TEST(UniqueKeyIndexCache, EraseBySliceKey)
{
    std::atomic<int> destructs{0};
    UniqueKeyIndexCache cache = makeCache(1 << 20);
    auto * obj = new FakeObject{&destructs, 7};
    rocksdb::Slice key("erase-me");
    cache.Insert(key, obj, &kTestHelper, sizeof(*obj), nullptr,
                 rocksdb::Cache::Priority::LOW,
                 rocksdb::Slice(), rocksdb::CompressionType::kNoCompression);

    auto * h = cache.Lookup(key, nullptr, nullptr, rocksdb::Cache::Priority::LOW, nullptr);
    EXPECT_NE(h, nullptr);
    cache.Release(h, false);

    cache.Erase(key);
    EXPECT_EQ(destructs.load(), 1);
    EXPECT_EQ(cache.Lookup(key, nullptr, nullptr, rocksdb::Cache::Priority::LOW, nullptr), nullptr);
}

TEST(UniqueKeyIndexCache, ReleaseEraseIfLastRefRemovesEntry)
{
    UniqueKeyIndexCache cache = makeCache(1 << 20);
    auto * obj = new FakeObject{nullptr, 7};
    rocksdb::Cache::Handle * h = nullptr;
    cache.Insert(rocksdb::Slice("eraseme"), obj, &kTestHelper, 64, &h,
                 rocksdb::Cache::Priority::LOW,
                 rocksdb::Slice(), rocksdb::CompressionType::kNoCompression);
    ASSERT_NE(h, nullptr);

    auto * h2 = cache.Lookup(rocksdb::Slice("eraseme"), &kTestHelper, nullptr,
                             rocksdb::Cache::Priority::LOW, /*stats=*/nullptr);
    ASSERT_NE(h2, nullptr);
    EXPECT_FALSE(cache.Release(h2, /*erase_if_last_ref=*/false));
    EXPECT_TRUE(cache.Release(h, /*erase_if_last_ref=*/true));

    auto * h3 = cache.Lookup(rocksdb::Slice("eraseme"), &kTestHelper, nullptr,
                             rocksdb::Cache::Priority::LOW, /*stats=*/nullptr);
    EXPECT_EQ(h3, nullptr);
}

TEST(UniqueKeyIndexCache, ReleaseEraseLastRefRespectsOtherLiveHandle)
{
    /// rocksdb::Cache::Release(h, erase_if_last_ref=true) must NOT erase the
    /// entry from the cache while another live handle still pins the same
    /// entry. Without the per-entry pin_count check, releasing one handle
    /// would evict the entry under the other handle's feet — a contract
    /// violation that turns into an avoidable cache miss for live readers.
    UniqueKeyIndexCache cache = makeCache(1 << 20);
    auto * obj = new FakeObject{nullptr, 7};
    rocksdb::Cache::Handle * h1 = nullptr;
    cache.Insert(rocksdb::Slice("key"), obj, &kTestHelper, 64, &h1,
                 rocksdb::Cache::Priority::LOW,
                 rocksdb::Slice(), rocksdb::CompressionType::kNoCompression);
    ASSERT_NE(h1, nullptr);

    auto * h2 = cache.Lookup(rocksdb::Slice("key"), &kTestHelper, nullptr,
                             rocksdb::Cache::Priority::LOW, nullptr);
    ASSERT_NE(h2, nullptr);

    /// h2 still pinned: Release(h1, true) must NOT erase the entry.
    EXPECT_FALSE(cache.Release(h1, /*erase_if_last_ref=*/true));

    auto * h3 = cache.Lookup(rocksdb::Slice("key"), &kTestHelper, nullptr,
                             rocksdb::Cache::Priority::LOW, nullptr);
    ASSERT_NE(h3, nullptr) << "entry was erased while h2 was still pinning it";
    cache.Release(h3, /*erase_if_last_ref=*/false);
    cache.Release(h2, /*erase_if_last_ref=*/false);
}

TEST(UniqueKeyIndexCache, ReleaseEraseIdentityAwareDoesNotEvictReplacement)
{
    /// A concurrent re-`Insert` of the same key replaces the table resident
    /// with a new shared_ptr while leaving the old handle pinning the old
    /// entry. `Release(old, true)` must NOT erase the new resident.
    UniqueKeyIndexCache cache = makeCache(1 << 20);

    auto * obj_old = new FakeObject{nullptr, 1};
    rocksdb::Cache::Handle * h_old = nullptr;
    cache.Insert(rocksdb::Slice("samekey"), obj_old, &kTestHelper, 64, &h_old,
                 rocksdb::Cache::Priority::LOW,
                 rocksdb::Slice(), rocksdb::CompressionType::kNoCompression);
    ASSERT_NE(h_old, nullptr);

    auto * obj_new = new FakeObject{nullptr, 2};
    rocksdb::Cache::Handle * h_new = nullptr;
    cache.Insert(rocksdb::Slice("samekey"), obj_new, &kTestHelper, 64, &h_new,
                 rocksdb::Cache::Priority::LOW,
                 rocksdb::Slice(), rocksdb::CompressionType::kNoCompression);
    ASSERT_NE(h_new, nullptr);

    EXPECT_FALSE(cache.Release(h_old, /*erase_if_last_ref=*/true));

    auto * h_check = cache.Lookup(rocksdb::Slice("samekey"), &kTestHelper, nullptr,
                                  rocksdb::Cache::Priority::LOW, /*stats=*/nullptr);
    ASSERT_NE(h_check, nullptr);
    EXPECT_EQ(static_cast<FakeObject *>(cache.Value(h_check))->value, 2);

    cache.Release(h_check, /*erase_if_last_ref=*/false);
    cache.Release(h_new, /*erase_if_last_ref=*/false);
}

TEST(UniqueKeyIndexCache, CreateStandaloneAlwaysReturnsHandle)
{
    /// `SetStrictCapacityLimit` is best-effort here: the flag is reported by
    /// `HasStrictCapacityLimit` but admission is unconditional. CreateStandalone
    /// returns a handle for any charge.
    UniqueKeyIndexCache cache = makeCache(/*bytes=*/64);
    cache.SetStrictCapacityLimit(true);
    EXPECT_TRUE(cache.HasStrictCapacityLimit());

    auto * obj = new FakeObject{nullptr, 1};
    auto * h = cache.CreateStandalone(rocksdb::Slice("k"), obj, &kTestHelper,
                                      /*charge=*/8192, /*allow_uncharged=*/false);
    ASSERT_NE(h, nullptr);
    EXPECT_EQ(static_cast<FakeObject *>(cache.Value(h))->value, 1);
    EXPECT_EQ(cache.GetCharge(h), 8192u);
    cache.Release(h, /*erase_if_last_ref=*/false);
}

TEST(UniqueKeyIndexCache, ThreadSafetySmoke)
{
    UniqueKeyIndexCache cache = makeCache(128 * 1024);

    std::atomic<bool> stop{false};
    std::atomic<size_t> ops{0};

    auto worker = [&](int id)
    {
        std::mt19937_64 rng(id);
        while (!stop.load(std::memory_order_relaxed))
        {
            std::string k = "t" + std::to_string(rng() % 100);
            rocksdb::Slice key(k);
            if (rng() % 2 == 0)
            {
                auto * obj = new FakeObject{nullptr, id};
                cache.Insert(key, obj, &kTestHelper, 256, nullptr,
                             rocksdb::Cache::Priority::LOW,
                             rocksdb::Slice(), rocksdb::CompressionType::kNoCompression);
            }
            else
            {
                auto * h = cache.Lookup(key, nullptr, nullptr, rocksdb::Cache::Priority::LOW, nullptr);
                if (h)
                    cache.Release(h, false);
            }
            ops.fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i)
        threads.emplace_back(worker, i);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    stop.store(true, std::memory_order_relaxed);
    for (auto & t : threads)
        t.join();

    EXPECT_GT(ops.load(), 0u);
    cache.EraseUnRefEntries();
}

#else

TEST(UniqueKeyIndexCache, NotCompiledInWithoutRocksDB)
{
    GTEST_SKIP() << "USE_ROCKSDB is off; index cache adapter is not compiled.";
}

#endif
