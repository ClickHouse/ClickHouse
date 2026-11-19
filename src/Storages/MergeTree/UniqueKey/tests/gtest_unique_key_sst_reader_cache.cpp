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
#include <Common/tests/gtest_global_context.h>

#include <fmt/format.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

/// Cache semantics (hit/miss, eviction, failed loads) belong to `CacheBase`;
/// only the reader's own sharing-related behavior is tested here.
class UniqueKeySSTReaderCacheTest : public ::testing::Test
{
protected:
    std::filesystem::path base;
    std::shared_ptr<DiskLocal> disk;
    std::shared_ptr<SingleDiskVolume> volume;

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

    /// Write `num_keys` entries via the real `SSTIndexWriter` and return the
    /// part storage holding the finished SST. Keys are fixed-width strings so
    /// lexicographic order matches the strictly-increasing requirement.
    DataPartStoragePtr writeSST(const String & part_dir, size_t num_keys)
    {
        std::filesystem::create_directories(base / part_dir);
        auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", part_dir);

        SSTIndexWriter writer(*storage, getContext().context);
        for (size_t i = 0; i < num_keys; ++i)
            writer.addEncoded(keyOf(i), static_cast<UInt32>(i));
        MergeTreeDataPartChecksums sst_checksums;
        writer.finish(sst_checksums, /*fsync=*/false);
        return storage;
    }

    static String keyOf(size_t i) { return fmt::format("k{:06}", i); }
};

/// With `no_block_cache` the reader preloads index + filter blocks, so resident
/// size tracks the SST, and the cache weight is exactly that plus overhead.
TEST_F(UniqueKeySSTReaderCacheTest, WeightReflectsResidentIndexAndFilter)
{
    auto small = openSSTReaderFromStorage(writeSST("part_small", 16), SSTIndexWriter::FILE_NAME);
    auto big = openSSTReaderFromStorage(writeSST("part_big", 100000), SSTIndexWriter::FILE_NAME);

    EXPECT_GT(small->memoryUsage(), 0u);
    EXPECT_GT(big->memoryUsage(), small->memoryUsage());

    UniqueKeySSTReaderWeightFunction weight;
    EXPECT_EQ(weight(*small), small->memoryUsage() + UniqueKeySSTReaderWeightFunction::UNIQUE_KEY_SST_READER_CACHE_OVERHEAD);
}

/// A reader keeps no buffer between operations. Concurrent probes each reuse
/// an operation-local buffer and must still return every row; the reader also
/// stays usable for whole-file checksum verification.
TEST_F(UniqueKeySSTReaderCacheTest, SharedReaderServesConcurrentProbes)
{
    constexpr size_t NUM_KEYS = 64;
    constexpr size_t NUM_THREADS = 8;

    auto storage = writeSST("part_a", NUM_KEYS);
    SSTFileReaderPtr reader = openSSTReaderFromStorage(storage, SSTIndexWriter::FILE_NAME);

    std::vector<String> owned_keys;
    for (size_t i = 0; i < NUM_KEYS; ++i)
        owned_keys.push_back(keyOf(i));
    std::vector<std::string_view> views(owned_keys.begin(), owned_keys.end());

    std::atomic<int> failures = 0;
    std::vector<std::thread> threads;
    for (size_t t = 0; t < NUM_THREADS; ++t)
    {
        threads.emplace_back([&]()
        {
            SSTProbeTargetPart target(/*part=*/nullptr, /*pinned_bitmap=*/nullptr, reader);
            for (size_t iter = 0; iter < 20; ++iter)
            {
                std::vector<std::optional<UInt64>> out;
                target.findRowIndexBatch(views, out);
                for (size_t i = 0; i < NUM_KEYS; ++i)
                    if (out[i] != i)
                        failures.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto & thread : threads)
        thread.join();
    EXPECT_EQ(failures.load(), 0);

    EXPECT_TRUE(reader->verifyChecksum().ok());
}

}

#endif
