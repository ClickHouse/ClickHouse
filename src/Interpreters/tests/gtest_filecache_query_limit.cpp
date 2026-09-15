#include <Columns/IColumn.h>
#include <IO/copyData.h>
#include <Interpreters/FileCache/IFileCachePriority.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>


#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <thread>

#include <Core/ServerUUID.h>
#include <Common/ThreadStatus.h>
#include <Common/iota.h>
#include <Common/randomSeed.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheSettings.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/FileCache/EvictionCandidates.h>
#include <Interpreters/FileCache/SLRUFileCachePriority.h>
#include <Interpreters/FileCache/QueryLimit.h>
#if CLICKHOUSE_CLOUD
#include <Interpreters/FileCache/OvercommitFileCachePriority.h>
#endif

#include <Common/DimensionalMetrics.h>
#include <Common/HistogramMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <base/hex.h>
#include <base/sleep.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/QueryScope.h>
#include <Common/SipHash.h>
#include <Common/filesystemHelpers.h>
#include <Common/scope_guard_safe.h>
#include <Common/tests/gtest_global_context.h>

#include <Poco/ConsoleChannel.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/BoundedReadBuffer.h>
#include <Interpreters/FileCache/WriteBufferToFileSegment.h>

#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Interpreters/DatabaseCatalog.h>
#include <base/scope_guard.h>
#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

namespace CurrentMetrics
{
    extern const Metric FilesystemCachePriorityQueueElements;
    extern const Metric FilesystemCacheInvalidatedElements;
}

namespace ProfileEvents
{
    extern const Event FilesystemCacheDowngradedFileSegments;
    extern const Event FilesystemCacheEvictedFileSegments;
}

using namespace std::chrono_literals;
namespace DB::FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsUInt64 boundary_alignment;
    extern const FileCacheSettingsUInt64 reserve_granularity;
    extern const FileCacheSettingsFileCachePolicy cache_policy;
    extern const FileCacheSettingsBool load_metadata_asynchronously;
    extern const FileCacheSettingsBool enable_filesystem_query_cache_limit;
}

namespace fs = std::filesystem;
using namespace DB;

namespace
{
/// Own cache directories, so these tests never share cache state with `gtest_filecache.cpp`.
fs::path caches_dir = fs::current_path() / "query_limit_cache_test";
std::string cache_base_path = caches_dir / "cache1" / "";
std::string cache_base_path2 = caches_dir / "cache2" / "";
std::string cache_base_path3 = caches_dir / "cache3" / "";
}

class FileCacheQueryLimitTest : public ::testing::Test
{
public:
    FileCacheQueryLimitTest()
    {
        /// Reset current_thread to avoid conflicts of ThreadStatus with MainThreadStatus.
        current_thread = nullptr;
        /// The context has to exist before a cache is initialized.
        getContext();
    }

    ~FileCacheQueryLimitTest() override
    {
        current_thread = MainThreadStatus::get();
    }

    void SetUp() override
    {
        fs::remove_all(caches_dir);
        fs::create_directories(cache_base_path);
        fs::create_directories(cache_base_path2);
        fs::create_directories(cache_base_path3);
    }

    void TearDown() override
    {
        fs::remove_all(caches_dir);
    }
};

namespace
{
/// A cache which permits the per-query limit, plus a budget of `size_limit` bytes for one query.
struct CacheWithQueryBudget
{
    CacheWithQueryBudget(const std::string & cache_name, const std::string & cache_path, size_t size_limit,
                         size_t reserve_granularity = 0, size_t cache_max_size = 1000)
    {
        DB::FileCacheSettings settings;
        settings[FileCacheSetting::path] = cache_path;
        settings[FileCacheSetting::max_size] = cache_max_size;
        settings[FileCacheSetting::max_elements] = 100;
        settings[FileCacheSetting::boundary_alignment] = 1;
        settings[FileCacheSetting::load_metadata_asynchronously] = false;
        settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;
        settings[FileCacheSetting::enable_filesystem_query_cache_limit] = true;
        settings[FileCacheSetting::reserve_granularity] = reserve_granularity;

        cache = std::make_unique<DB::FileCache>(cache_name, settings);
        cache->initialize();
        budget = std::make_shared<FileCacheQueryBudget>(size_limit);
    }

    /// Reserves `size` for a new file segment and writes `written_size` bytes of it, leaving the
    /// rest reserved but not written. False when the budget refused the reservation.
    bool tryCacheSegment(const std::string & cache_path, const std::string & key_name,
                         size_t offset, size_t size, size_t written_size,
                         const FileCacheQueryBudgetPtr & charged_budget) const
    {
        auto segments = cache->getOrSet(
            DB::FileCacheKey::fromPath(key_name), offset, size, INT_MAX, {}, 0, FileCache::getCommonOrigin());
        auto segment = *segments->begin();
        EXPECT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());

        std::string failure_reason;
        if (!segment->reserve(size, 1000, failure_reason, charged_budget))
            return false;

        auto key_str = segment->key().toString();
        fs::create_directories(fs::path(cache_path) / key_str.substr(0, 3) / key_str);

        std::string data(written_size, '0');
        segment->write(data.data(), data.size(), segment->getCurrentWriteOffset());
        return true;
    }

    bool tryCacheSegment(const std::string & cache_path, size_t offset, size_t size, size_t written_size) const
    {
        return tryCacheSegment(cache_path, "query_budget_key", offset, size, written_size, budget);
    }

    std::unique_ptr<DB::FileCache> cache;
    FileCacheQueryBudgetPtr budget;
};
}

TEST_F(FileCacheQueryLimitTest, BudgetCountsReservedBytes)
{
    /// Charged when the space is reserved, not when it is written, so that threads of one query
    /// cannot together write more than the limit. What is reserved and not written stays charged.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    CacheWithQueryBudget cache_with_budget("budget_reserved_bytes", cache_base_path, /* size_limit */100);
    ASSERT_TRUE(cache_with_budget.tryCacheSegment(cache_base_path, /* offset */0, /* size */8, /* written */2));
    ASSERT_EQ(cache_with_budget.budget->getChargedBytes(), 8u);
}

TEST_F(FileCacheQueryLimitTest, BudgetIsCumulativeAcrossSegments)
{
    /// The budget is for the whole query, not for a single reservation: two 10 byte segments must
    /// not both fit into 15 bytes.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    CacheWithQueryBudget cache_with_budget("budget_cumulative", cache_base_path2, /* size_limit */15);
    ASSERT_TRUE(cache_with_budget.tryCacheSegment(cache_base_path2, /* offset */0, /* size */10, /* written */10));
    ASSERT_FALSE(cache_with_budget.tryCacheSegment(cache_base_path2, /* offset */100, /* size */10, /* written */10));
    ASSERT_EQ(cache_with_budget.budget->getChargedBytes(), 10u);
}

TEST_F(FileCacheQueryLimitTest, BudgetIsNotGivenBackWhenSegmentsLeaveTheCache)
{
    /// The budget counts what the query wrote, not what it currently has cached: dropping the cache
    /// does not give it back. This is what makes the accounting need no bookkeeping per segment.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    CacheWithQueryBudget cache_with_budget("budget_after_drop", cache_base_path3, /* size_limit */15);
    ASSERT_TRUE(cache_with_budget.tryCacheSegment(cache_base_path3, /* offset */0, /* size */10, /* written */10));

    cache_with_budget.cache->removeAllReleasable(FileCache::getCommonOrigin().user_id);

    ASSERT_EQ(cache_with_budget.budget->getChargedBytes(), 10u);
    ASSERT_FALSE(cache_with_budget.tryCacheSegment(cache_base_path3, /* offset */100, /* size */10, /* written */10));
}

TEST_F(FileCacheQueryLimitTest, EachQueryKeepsWhatItsOwnReservationsTook)
{
    /// One file segment reserved by two queries in turn: each keeps what its own reservation took,
    /// and neither is given back what the other reserved.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    CacheWithQueryBudget cache_with_budget("budget_handover", cache_base_path, /* size_limit */100);
    auto second_budget = std::make_shared<FileCacheQueryBudget>(/* size_limit */100);

    auto segments = cache_with_budget.cache->getOrSet(
        DB::FileCacheKey::fromPath("handover_key"), 0, 20, INT_MAX, {}, 0, FileCache::getCommonOrigin());
    auto segment = *segments->begin();
    ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());

    std::string failure_reason;
    ASSERT_TRUE(segment->reserve(8, 1000, failure_reason, cache_with_budget.budget));

    auto key_str = segment->key().toString();
    fs::create_directories(fs::path(cache_base_path) / key_str.substr(0, 3) / key_str);
    std::string data(8, '0');
    segment->write(data.data(), data.size(), segment->getCurrentWriteOffset());

    /// The download is handed over: the second query reserves the rest of the same segment.
    ASSERT_TRUE(segment->reserve(12, 1000, failure_reason, second_budget));

    ASSERT_EQ(cache_with_budget.budget->getChargedBytes(), 8u);
    ASSERT_EQ(second_budget->getChargedBytes(), 12u);
}

TEST_F(FileCacheQueryLimitTest, ARefusedReservationIsNotCharged)
{
    /// The budget is taken before the space is reserved, so a reservation which the cache refuses
    /// must give it back. Otherwise a query in a full cache burns its budget caching nothing.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    /// The cache holds 10 bytes, the budget allows much more.
    CacheWithQueryBudget cache_with_budget(
        "budget_refused", cache_base_path2, /* size_limit */1000, /* reserve_granularity */0, /* cache_max_size */10);

    /// Held by its holder, so it cannot be evicted to make room for the next reservation.
    auto segments = cache_with_budget.cache->getOrSet(
        DB::FileCacheKey::fromPath("held_key"), 0, 10, INT_MAX, {}, 0, FileCache::getCommonOrigin());
    auto held = *segments->begin();
    ASSERT_EQ(held->getOrSetDownloader(), FileSegment::getCallerId());
    std::string failure_reason;
    ASSERT_TRUE(held->reserve(10, 1000, failure_reason, cache_with_budget.budget));

    auto key_str = held->key().toString();
    fs::create_directories(fs::path(cache_base_path2) / key_str.substr(0, 3) / key_str);
    std::string data(10, '0');
    held->write(data.data(), data.size(), held->getCurrentWriteOffset());
    ASSERT_EQ(cache_with_budget.budget->getChargedBytes(), 10u);

    /// The cache is full of what this query holds, so this reservation cannot succeed.
    ASSERT_FALSE(cache_with_budget.tryCacheSegment(cache_base_path2, "refused_key", 0, 10, 10, cache_with_budget.budget));

    /// The refused reservation left the budget as it was.
    ASSERT_EQ(cache_with_budget.budget->getChargedBytes(), 10u);
}

TEST_F(FileCacheQueryLimitTest, ReadingWithoutABudgetIsNotCharged)
{
    /// A query which sets no limit has no budget, and its writes are charged to no one.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    CacheWithQueryBudget cache_with_budget("budget_absent", cache_base_path2, /* size_limit */10);
    ASSERT_TRUE(cache_with_budget.tryCacheSegment(cache_base_path2, "unlimited_key", 0, 50, 50, nullptr));
    ASSERT_EQ(cache_with_budget.budget->getChargedBytes(), 0u);
}
