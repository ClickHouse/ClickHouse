#include <gtest/gtest.h>

#include "config.h"

#if USE_ROCKSDB

#include <Storages/MergeTree/UniqueKey/UniqueKeySSTReaderCache.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeySSTProbe.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>

#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>

#include <Interpreters/Context.h>
#include <IO/ReadSettings.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

#include <base/unaligned.h>

#include <fmt/format.h>

#include <rocksdb/slice.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace DB;

namespace CurrentMetrics
{
    extern const Metric UniqueKeySSTReaderCacheBytes;
    extern const Metric UniqueKeySSTReaderCacheEntries;
}

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{

class UniqueKeySSTReaderCacheTest : public ::testing::Test
{
protected:
    std::filesystem::path base;
    std::shared_ptr<DiskLocal> disk;
    std::shared_ptr<SingleDiskVolume> volume;
    int counter = 0;

    void SetUp() override
    {
        base = std::filesystem::temp_directory_path()
            / ("gtest_uk_sst_reader_cache_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(base);
        std::filesystem::create_directories(base);
        disk = std::make_shared<DiskLocal>("test_disk", base.string());
        volume = std::make_shared<SingleDiskVolume>("test_volume", disk);

        /// The SST writer streams through the context's temporary storage. Set
        /// it only if no other fixture in this binary already did —
        /// `setTemporaryStoragePath` throws (LOGICAL_ERROR, aborts in Debug) if
        /// called twice, so two SST-using suites must guard on the shared state.
        if (!getContext().context->getSharedTempDataOnDisk())
        {
            auto shared_tmp = std::filesystem::temp_directory_path() / "ck_uk_probe_gtest_tmp";
            std::filesystem::create_directories(shared_tmp);
            getMutableContext().context->setTemporaryStoragePath(shared_tmp.string() + "/", 0);
        }
    }

    void TearDown() override
    {
        volume.reset();
        disk.reset();
        std::filesystem::remove_all(base);
    }

    UniqueKeySSTReaderCache makeCache(size_t max_size_in_bytes = 8 * 1024 * 1024)
    {
        return UniqueKeySSTReaderCache(
            "LRU",
            CurrentMetrics::UniqueKeySSTReaderCacheBytes,
            CurrentMetrics::UniqueKeySSTReaderCacheEntries,
            max_size_in_bytes,
            /*size_ratio=*/0.5);
    }

    /// Write `num_keys` entries via the real `SSTIndexWriter` and return the
    /// part storage holding the finished SST. Keys are fixed-width strings so
    /// lexicographic order matches the strictly-increasing requirement.
    DataPartStoragePtr writeSST(const String & part_dir, size_t num_keys)
    {
        std::filesystem::create_directories(base / part_dir);
        auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", part_dir);

        SSTIndexWriter writer(*storage, getContext().context);
        for (size_t i = 0; i < num_keys; ++i)
        {
            String key = fmt::format("k{:06}", i);
            writer.addEncoded(key, static_cast<UInt32>(i));
        }
        MergeTreeDataPartChecksums sst_checksums;
        writer.finish(sst_checksums, /*fsync=*/false);
        return storage;
    }

    static MergeTreeDataPartChecksum::uint128 fakeHash(UInt64 first)
    {
        return {first, 0xdeadbeef};
    }

    static String keyOf(size_t i) { return fmt::format("k{:06}", i); }
};

/// A miss opens once; a hit returns the very same reader without reopening.
TEST_F(UniqueKeySSTReaderCacheTest, HitSharesOneOpen)
{
    auto cache = makeCache();
    auto storage = writeSST("part_a", 100);
    const auto key = UniqueKeySSTReaderCache::makeKey("part_a", fakeHash(1));

    std::atomic<int> opens = 0;
    const auto loader = [&]() -> SSTFileReaderPtr
    {
        ++opens;
        return openSSTReaderFromStorage(storage, SSTIndexWriter::FILE_NAME, ReadSettings{});
    };

    auto [r1, loaded1] = cache.getOrSet(key, loader);
    auto [r2, loaded2] = cache.getOrSet(key, loader);

    EXPECT_TRUE(loaded1);
    EXPECT_FALSE(loaded2);
    EXPECT_EQ(opens.load(), 1);
    EXPECT_EQ(r1.get(), r2.get());
}

/// The SST's content checksum is part of the key: a rebuilt SST (new hash) must
/// not be served the old reader, and the old entry stays independently addressable.
TEST_F(UniqueKeySSTReaderCacheTest, FileHashIsPartOfKey)
{
    auto cache = makeCache();
    auto storage = writeSST("part_a", 100);
    const auto key_v1 = UniqueKeySSTReaderCache::makeKey("part_a", fakeHash(1));
    const auto key_v2 = UniqueKeySSTReaderCache::makeKey("part_a", fakeHash(2));

    std::atomic<int> opens = 0;
    const auto loader = [&]() -> SSTFileReaderPtr
    {
        ++opens;
        return openSSTReaderFromStorage(storage, SSTIndexWriter::FILE_NAME, ReadSettings{});
    };

    auto [r_v1, loaded_v1] = cache.getOrSet(key_v1, loader);
    auto [r_v2, loaded_v2] = cache.getOrSet(key_v2, loader);

    /// Same part, different content: two distinct entries, two opens.
    EXPECT_TRUE(loaded_v1);
    EXPECT_TRUE(loaded_v2);
    EXPECT_EQ(opens.load(), 2);
    EXPECT_NE(r_v1.get(), r_v2.get());
}

/// A loader exception propagates and poisons nothing: the next attempt reopens.
TEST_F(UniqueKeySSTReaderCacheTest, FailedOpenIsNotCached)
{
    auto cache = makeCache();
    auto storage = writeSST("part_a", 100);
    const auto key = UniqueKeySSTReaderCache::makeKey("part_a", fakeHash(1));

    const auto failing = []() -> SSTFileReaderPtr
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA, "simulated corrupt SST");
    };
    EXPECT_THROW(cache.getOrSet(key, failing), Exception);
    EXPECT_EQ(cache.get(key), nullptr);

    auto [reader, loaded] = cache.getOrSet(
        key, [&]() -> SSTFileReaderPtr { return openSSTReaderFromStorage(storage, SSTIndexWriter::FILE_NAME, ReadSettings{}); });
    EXPECT_TRUE(loaded);
    ASSERT_NE(reader, nullptr);
}

/// With `no_block_cache` the reader preloads index + filter blocks, so resident
/// size tracks the SST, and the cache weight is exactly that plus overhead.
TEST_F(UniqueKeySSTReaderCacheTest, WeightReflectsResidentIndexAndFilter)
{
    auto small = openSSTReaderFromStorage(writeSST("part_small", 16), SSTIndexWriter::FILE_NAME, ReadSettings{});
    auto big = openSSTReaderFromStorage(writeSST("part_big", 100000), SSTIndexWriter::FILE_NAME, ReadSettings{});

    EXPECT_GT(small->memoryUsage(), 0u);
    EXPECT_GT(big->memoryUsage(), small->memoryUsage());

    UniqueKeySSTReaderWeightFunction weight;
    EXPECT_EQ(weight(*small), small->memoryUsage() + UniqueKeySSTReaderWeightFunction::UNIQUE_KEY_SST_READER_CACHE_OVERHEAD);
}

/// Many threads racing on one key open it exactly once (stampede protection),
/// and the shared reader serves concurrent `multiGet`s correctly.
TEST_F(UniqueKeySSTReaderCacheTest, ConcurrentAccessSharesOneReader)
{
    constexpr size_t NUM_KEYS = 64;
    constexpr size_t NUM_THREADS = 8;

    auto cache = makeCache();
    auto storage = writeSST("part_a", NUM_KEYS);
    const auto key = UniqueKeySSTReaderCache::makeKey("part_a", fakeHash(1));

    std::atomic<int> opens = 0;
    const auto loader = [&]() -> SSTFileReaderPtr
    {
        ++opens;
        return openSSTReaderFromStorage(storage, SSTIndexWriter::FILE_NAME, ReadSettings{});
    };

    std::atomic<bool> start = false;
    std::atomic<int> failures = 0;
    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);

    for (size_t t = 0; t < NUM_THREADS; ++t)
    {
        threads.emplace_back([&, t]()
        {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();

            auto [reader, loaded] = cache.getOrSet(key, loader);
            if (!reader)
            {
                failures.fetch_add(1, std::memory_order_relaxed);
                return;
            }

            for (size_t iter = 0; iter < 50; ++iter)
            {
                std::vector<String> owned_keys;
                owned_keys.reserve(PROBE_BATCH_SIZE);
                for (size_t i = 0; i < PROBE_BATCH_SIZE; ++i)
                    owned_keys.push_back(keyOf((t * PROBE_BATCH_SIZE + i) % NUM_KEYS));

                std::vector<rocksdb::Slice> keys;
                keys.reserve(owned_keys.size());
                for (const auto & k : owned_keys)
                    keys.emplace_back(k);

                std::vector<String> values;
                auto statuses = reader->multiGet(keys, values);
                if (statuses.size() != keys.size())
                {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                for (size_t i = 0; i < keys.size(); ++i)
                {
                    const size_t expected = (t * PROBE_BATCH_SIZE + i) % NUM_KEYS;
                    if (!statuses[i].ok() || values[i].size() != sizeof(UInt32)
                        || unalignedLoadBigEndian<UInt32>(values[i].data()) != expected)
                        failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(opens.load(), 1);
}

/// The byte cap (resident size + per-reader overhead) bounds the cache.
TEST_F(UniqueKeySSTReaderCacheTest, ByteCapEvicts)
{
    auto storage = writeSST("part_a", 100);
    const size_t one_reader_weight
        = UniqueKeySSTReaderWeightFunction{}(*openSSTReaderFromStorage(storage, SSTIndexWriter::FILE_NAME, ReadSettings{}));
    auto cache = makeCache(one_reader_weight);

    const auto loader = [&]() -> SSTFileReaderPtr
    {
        return openSSTReaderFromStorage(storage, SSTIndexWriter::FILE_NAME, ReadSettings{});
    };
    const auto key_1 = UniqueKeySSTReaderCache::makeKey("part_a", fakeHash(1));
    const auto key_2 = UniqueKeySSTReaderCache::makeKey("part_a", fakeHash(2));

    cache.getOrSet(key_1, loader);
    cache.getOrSet(key_2, loader);

    EXPECT_EQ(cache.get(key_1), nullptr);
    EXPECT_NE(cache.get(key_2), nullptr);
}

/// Absent keys short-circuit through the shared reader's bloom filter.
TEST_F(UniqueKeySSTReaderCacheTest, SharedReaderReportsNotFound)
{
    auto cache = makeCache();
    auto storage = writeSST("part_a", 100);
    const auto key = UniqueKeySSTReaderCache::makeKey("part_a", fakeHash(1));

    auto [reader, loaded] = cache.getOrSet(
        key, [&]() -> SSTFileReaderPtr { return openSSTReaderFromStorage(storage, SSTIndexWriter::FILE_NAME, ReadSettings{}); });
    ASSERT_TRUE(loaded);

    std::vector<rocksdb::Slice> keys;
    String absent = keyOf(100000);
    keys.emplace_back(absent);
    std::vector<String> values;
    auto statuses = reader->multiGet(keys, values);
    ASSERT_EQ(statuses.size(), 1u);
    EXPECT_TRUE(statuses[0].IsNotFound());
}

}

#endif
