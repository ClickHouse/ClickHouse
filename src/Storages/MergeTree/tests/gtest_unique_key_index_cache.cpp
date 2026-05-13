#include <gtest/gtest.h>

#include "config.h"

#include <Storages/MergeTree/UniqueKey/UniqueKeyIndexCache.h>
#include <Common/ProfileEvents.h>

#include <atomic>
#include <string>

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
    cache.Release(h2, /*erase_if_last_ref=*/false);
    EXPECT_TRUE(cache.Release(h, /*erase_if_last_ref=*/true));

    auto * h3 = cache.Lookup(rocksdb::Slice("eraseme"), &kTestHelper, nullptr,
                             rocksdb::Cache::Priority::LOW, /*stats=*/nullptr);
    EXPECT_EQ(h3, nullptr);
}

TEST(UniqueKeyIndexCache, ReleaseEraseLastRefRespectsOtherLiveHandle)
{
    /// Release(h, erase_if_last_ref=true) must NOT erase the entry while
    /// another HandlePin still pins it. The predicate requires
    /// `pinned.use_count() == 2` (backing + this HandlePin); a second live
    /// HandlePin pushes use_count to 3 and the predicate refuses.
    UniqueKeyIndexCache cache = makeCache(1 << 20);
    auto * obj = new FakeObject{nullptr, 7};
    rocksdb::Cache::Handle * h1 = nullptr;
    cache.Insert(rocksdb::Slice("k"), obj, &kTestHelper, 64, &h1,
                 rocksdb::Cache::Priority::LOW,
                 rocksdb::Slice(), rocksdb::CompressionType::kNoCompression);
    ASSERT_NE(h1, nullptr);

    auto * h2 = cache.Lookup(rocksdb::Slice("k"), &kTestHelper, nullptr,
                             rocksdb::Cache::Priority::LOW, nullptr);
    ASSERT_NE(h2, nullptr);

    EXPECT_FALSE(cache.Release(h1, /*erase_if_last_ref=*/true));

    auto * h3 = cache.Lookup(rocksdb::Slice("k"), &kTestHelper, nullptr,
                             rocksdb::Cache::Priority::LOW, nullptr);
    ASSERT_NE(h3, nullptr) << "entry was erased while h2 was still pinning it";
    cache.Release(h3, /*erase_if_last_ref=*/false);
    cache.Release(h2, /*erase_if_last_ref=*/false);
}

TEST(UniqueKeyIndexCache, ReleaseEraseIdentityAwareDoesNotEvictReplacement)
{
    /// A concurrent re-Insert of the same key replaces the table resident
    /// with a new shared_ptr while the old handle still pins the old entry.
    /// Release(old, erase_if_last_ref=true) must NOT erase the new resident
    /// — the predicate's `v == pinned` identity check refuses.
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

TEST(UniqueKeyIndexCache, StrictCapacityLimitIsUnsupported)
{
    /// `HasStrictCapacityLimit()` is hardcoded false; `SetStrictCapacityLimit`
    /// is a no-op.
    UniqueKeyIndexCache cache = makeCache(/*bytes=*/64);
    EXPECT_FALSE(cache.HasStrictCapacityLimit());
    cache.SetStrictCapacityLimit(true);
    EXPECT_FALSE(cache.HasStrictCapacityLimit());
}

TEST(UniqueKeyIndexCache, RefReturnsFalse)
{
    /// This adapter declines `Ref`; callers must respect the false return
    /// and not assume the handle's ref count was bumped.
    UniqueKeyIndexCache cache = makeCache(1 << 20);
    auto * obj = new FakeObject{nullptr, 1};
    rocksdb::Cache::Handle * h = nullptr;
    cache.Insert(rocksdb::Slice("k"), obj, &kTestHelper, sizeof(*obj), &h,
                 rocksdb::Cache::Priority::LOW,
                 rocksdb::Slice(), rocksdb::CompressionType::kNoCompression);
    ASSERT_NE(h, nullptr);

    EXPECT_FALSE(cache.Ref(h));
    EXPECT_FALSE(cache.Ref(nullptr));

    cache.Release(h, /*erase_if_last_ref=*/false);
}

TEST(UniqueKeyIndexCache, CreateStandaloneAlwaysSucceedsWhenNonStrict)
{
    /// Per `rocksdb::Cache::CreateStandalone`: "if `allow_uncharged==true` or
    /// `strict_capacity_limit=false`, the operation always succeeds and
    /// returns a valid Handle." Strict mode is permanently off, so both
    /// `allow_uncharged` branches must produce a charged handle.
    UniqueKeyIndexCache cache = makeCache(/*bytes=*/64);
    ASSERT_FALSE(cache.HasStrictCapacityLimit());

    for (bool allow_uncharged : {false, true})
    {
        auto * obj = new FakeObject{nullptr, 1};
        auto * h = cache.CreateStandalone(rocksdb::Slice("k"), obj, &kTestHelper,
                                          /*charge=*/8192, allow_uncharged);
        ASSERT_NE(h, nullptr) << "allow_uncharged=" << allow_uncharged;
        EXPECT_EQ(static_cast<FakeObject *>(cache.Value(h))->value, 1);
        EXPECT_EQ(cache.GetCharge(h), 8192u);
        cache.Release(h, /*erase_if_last_ref=*/false);
    }
}

#else

TEST(UniqueKeyIndexCache, NotCompiledInWithoutRocksDB)
{
    GTEST_SKIP() << "USE_ROCKSDB is off; index cache adapter is not compiled.";
}

#endif
