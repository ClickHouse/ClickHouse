#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <condition_variable>
#include <mutex>
#include <thread>

namespace
{
using namespace DB;

PuffinFilesCacheKey makeKey(String etag, Int64 offset, Int64 size, String path = "puffin", String storage = "local://test//")
{
    ObjectMetadata metadata;
    metadata.etag = std::move(etag);
    metadata.etag_is_strong = true;
    auto key = PuffinFilesCache::tryCreateKey(storage, path, metadata, offset, size);
    EXPECT_TRUE(key.has_value());
    return *key;
}

std::unique_ptr<roaring::Roaring64Map> bitmapWith(UInt64 position)
{
    auto bitmap = std::make_unique<roaring::Roaring64Map>();
    bitmap->add(static_cast<uint64_t>(position));
    return bitmap;
}
}

TEST(PuffinFilesCacheKey, SkipsWeakOrEmptyEtag)
{
    ObjectMetadata empty;
    EXPECT_FALSE(PuffinFilesCache::tryCreateKey("storage", "path", empty, 0, 16).has_value());

    ObjectMetadata weak;
    weak.etag = "mtime-size";
    weak.etag_is_strong = false;
    EXPECT_FALSE(PuffinFilesCache::tryCreateKey("storage", "path", weak, 0, 16).has_value());

    ObjectMetadata strong;
    strong.etag = "abc";
    strong.etag_is_strong = true;
    auto key = PuffinFilesCache::tryCreateKey("storage", "path", strong, 10, 20);
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(key->etag, "abc");
    EXPECT_EQ(key->content_offset, 10);
    EXPECT_EQ(key->content_size_in_bytes, 20);
    EXPECT_NE(*key, makeKey("abc", 11, 20));
    EXPECT_NE(*key, makeKey("other", 10, 20));
    EXPECT_NE(*key, makeKey("abc", 10, 20, "other-path"));
    EXPECT_NE(*key, makeKey("abc", 10, 20, "path", "other-storage"));
}

TEST(PuffinFilesCache, ReturnsCloneAndCountsWeight)
{
    PuffinFilesCache cache("SLRU", /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 10, /*size_ratio=*/ 0.5);
    const auto key = makeKey("etag", 0, 32);
    int loads = 0;

    auto first = cache.getOrSetDeletionVector(key, [&]()
    {
        ++loads;
        return bitmapWith(7);
    });
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(loads, 1);
    EXPECT_TRUE(first->contains(static_cast<uint64_t>(7)));
    first->add(static_cast<uint64_t>(9));

    auto second = cache.getOrSetDeletionVector(key, [&]()
    {
        ++loads;
        return bitmapWith(1);
    });
    EXPECT_EQ(loads, 1);
    EXPECT_TRUE(second->contains(static_cast<uint64_t>(7)));
    EXPECT_FALSE(second->contains(static_cast<uint64_t>(9)));
    EXPECT_GT(cache.sizeInBytes(), key.approximateMemoryBytes());
    EXPECT_EQ(cache.count(), 1u);

    cache.clear();
    EXPECT_EQ(cache.count(), 0u);
    auto third = cache.getOrSetDeletionVector(key, [&]()
    {
        ++loads;
        return bitmapWith(3);
    });
    EXPECT_EQ(loads, 2);
    EXPECT_TRUE(third->contains(static_cast<uint64_t>(3)));
    EXPECT_FALSE(third->contains(static_cast<uint64_t>(7)));
}

TEST(PuffinFilesCache, ClearDuringLoadDoesNotInsert)
{
    PuffinFilesCache cache("LRU", /*max_size_in_bytes=*/ 1 << 20, /*max_count=*/ 10, /*size_ratio=*/ 0.5);
    const auto key = makeKey("etag", 4, 16);

    std::mutex mutex;
    std::condition_variable cv;
    bool load_started = false;
    bool allow_finish = false;

    std::thread loader([&]()
    {
        cache.getOrSetDeletionVector(key, [&]()
        {
            {
                std::lock_guard lock(mutex);
                load_started = true;
            }
            cv.notify_one();

            std::unique_lock lock(mutex);
            cv.wait(lock, [&]() { return allow_finish; });
            return bitmapWith(5);
        });
    });

    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&]() { return load_started; });
    }

    cache.clear();

    {
        std::lock_guard lock(mutex);
        allow_finish = true;
    }
    cv.notify_one();
    loader.join();

    EXPECT_EQ(cache.count(), 0u);

    int loads = 0;
    auto reloaded = cache.getOrSetDeletionVector(key, [&]()
    {
        ++loads;
        return bitmapWith(5);
    });
    EXPECT_EQ(loads, 1);
    EXPECT_TRUE(reloaded->contains(static_cast<uint64_t>(5)));
    EXPECT_EQ(cache.count(), 1u);
}
