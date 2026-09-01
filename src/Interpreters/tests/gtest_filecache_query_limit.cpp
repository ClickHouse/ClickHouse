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
/// Reserves `size` for a new file segment under `key_name` and writes `downloaded_size` bytes of it,
/// leaving the segment incomplete: `size - downloaded_size` stays reserved but not written. Returns
/// the holder, or null if the reservation was refused.
FileSegmentsHolderPtr reserveAndWriteIncompleteSegment(
    DB::FileCache & cache,
    const std::string & cache_path,
    const std::string & key_name,
    size_t offset,
    size_t size,
    size_t downloaded_size)
{
    auto segments = cache.getOrSet(
        DB::FileCacheKey::fromPath(key_name), offset, size, INT_MAX, {}, 0, FileCache::getCommonOrigin());
    auto segment = *segments->begin();
    EXPECT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());

    std::string failure_reason;
    if (!segment->reserve(size, 1000, failure_reason))
        return nullptr;

    auto key_str = segment->key().toString();
    fs::create_directories(fs::path(cache_path) / key_str.substr(0, 3) / key_str);

    std::string data(downloaded_size, '0');
    segment->write(data.data(), data.size(), segment->getCurrentWriteOffset());
    return segments;
}

/// The same, but completes the segment, which gives the unwritten part of the reservation back.
bool reserveAndWriteCompletedSegment(
    DB::FileCache & cache,
    const std::string & cache_path,
    const std::string & key_name,
    size_t offset,
    size_t size,
    size_t downloaded_size)
{
    auto segments = reserveAndWriteIncompleteSegment(cache, cache_path, key_name, offset, size, downloaded_size);
    if (!segments)
        return false;

    FileSegment::complete(
        FileSegmentPtr(*segments->begin()), /* allow_background_download */false,
        /* force_shrink_to_downloaded_size */false);
    return true;
}

/// A cache with the per-query limit enabled, plus a query context holder for `query_id`,
/// as `CachedOnDiskReadBufferFromFile` creates it.
struct CacheWithQueryLimit
{
    CacheWithQueryLimit(
        const std::string & cache_name,
        const std::string & cache_path,
        const std::string & query_id_,
        size_t query_limit_bytes,
        size_t reserve_granularity,
        size_t cache_max_size = 1000)
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

        read_settings.query_limit_bytes = query_limit_bytes;
        read_settings.skip_download_if_exceeds_per_query_cache_write_limit = true;
        query_id = query_id_;
        holder = cache->getQueryContextHolder(query_id, read_settings);
    }

    /// All read buffers of the query are destroyed and a new one is created.
    void recreateQueryContextHolder()
    {
        holder.reset();
        holder = cache->getQueryContextHolder(query_id, read_settings);
    }

    bool tryCacheSegment(size_t offset, size_t size, size_t downloaded_size, const std::string & cache_path) const
    {
        return reserveAndWriteCompletedSegment(*cache, cache_path, "query_limit_key", offset, size, downloaded_size);
    }

    std::unique_ptr<DB::FileCache> cache;
    FilesystemCacheSettings read_settings;
    std::string query_id;
    FileCache::QueryContextHolderPtr holder;
};
}

TEST_F(FileCacheQueryLimitTest, QueryLimitUnchargesEvictedSegments)
{
    /// A segment evicted from the cache must stop counting against the query budget: otherwise a
    /// long query in a small cache is charged for data which is no longer cached and stops caching.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const std::string query_id = "query_id_evicted_segments";
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    CacheWithQueryLimit cache_with_limit(
        "query_limit_evicted", cache_base_path, query_id,
        /* query_limit_bytes */25, /* reserve_granularity */0, /* cache_max_size */20);
    ASSERT_TRUE(cache_with_limit.holder != nullptr);

    ASSERT_TRUE(cache_with_limit.tryCacheSegment(0, 10, 10, cache_base_path));
    ASSERT_TRUE(cache_with_limit.tryCacheSegment(100, 10, 10, cache_base_path));
    /// The cache is full, so this evicts the first segment.
    ASSERT_TRUE(cache_with_limit.tryCacheSegment(200, 5, 5, cache_base_path));
    /// 10 (evicted) + 10 + 5 exceeds the budget, 10 + 5 does not.
    ASSERT_TRUE(cache_with_limit.tryCacheSegment(300, 10, 10, cache_base_path));
}

TEST_F(FileCacheQueryLimitTest, QueryLimitUnchargesSegmentsDroppedFromCache)
{
    /// Eviction is not the only way a segment leaves the cache: dropping the cache or removing a key
    /// takes segments out too, and the query which cached them must stop being charged for them all
    /// the same, otherwise it never writes into the cache again.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const std::string query_id = "query_id_dropped_segments";
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    const std::string cache_path = caches_dir / "cache_query_limit_dropped" / "";
    CacheWithQueryLimit cache_with_limit(
        "query_limit_dropped", cache_path, query_id,
        /* query_limit_bytes */20, /* reserve_granularity */0);
    ASSERT_TRUE(cache_with_limit.holder != nullptr);

    ASSERT_TRUE(cache_with_limit.tryCacheSegment(0, 10, 10, cache_path));
    ASSERT_TRUE(cache_with_limit.tryCacheSegment(100, 10, 10, cache_path));
    /// The whole budget is used up by the two segments above.
    ASSERT_FALSE(cache_with_limit.tryCacheSegment(200, 10, 10, cache_path));

    cache_with_limit.cache->removeAllReleasable(FileCache::getCommonOrigin().user_id);

    /// Nothing of this query is cached anymore, so its whole budget is available again.
    ASSERT_TRUE(cache_with_limit.tryCacheSegment(300, 10, 10, cache_path));
    ASSERT_TRUE(cache_with_limit.tryCacheSegment(400, 10, 10, cache_path));
}

TEST_F(FileCacheQueryLimitTest, QueryLimitGivesBackNotWrittenBytesToTheQueryWhichReservedThem)
{
    /// A file segment is completed by whoever holds it last, which after a hand-over of the download
    /// is not the query charged for it. The bytes reserved but not written must go back to the
    /// query which reserved them, and to nobody when a background download reserved them.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const std::string owner_query_id = "reserving_query";
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(owner_query_id);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    CacheWithQueryLimit cache_with_limit(
        "query_limit_not_written_owner", cache_base_path3, owner_query_id,
        /* query_limit_bytes */12, /* reserve_granularity */0);
    ASSERT_TRUE(cache_with_limit.holder != nullptr);

    /// 8 of the 12 byte budget, of which only 2 bytes are written: 6 bytes of reserve-ahead.
    auto reserved = reserveAndWriteIncompleteSegment(*cache_with_limit.cache, cache_base_path3, "query_limit_key", 0, 8, 2);
    ASSERT_TRUE(reserved != nullptr);
    /// Hand the download over, so that another query may complete the segment.
    (*reserved->begin())->resetDownloader();

    /// Another query completes the segment, which is what gives the reserve-ahead back.
    std::thread completing_query([&]
    {
        DB::ThreadStatus completing_thread_status;
        auto completing_context = DB::Context::createCopy(getContext().context);
        completing_context->makeQueryContext();
        completing_context->setCurrentQueryId("completing_query");
        auto completing_scope_holder = DB::QueryScope::create(completing_context);
        auto completing_holder = cache_with_limit.cache->getQueryContextHolder("completing_query", cache_with_limit.read_settings);

        /// Destroying the holder completes the segment.
        reserved = nullptr;
    });
    completing_query.join();

    /// The 6 bytes went back to the query which reserved them, not to the one which completed the
    /// segment, so only the 2 written bytes stay charged and 8 more bytes fit into the budget.
    ASSERT_TRUE(reserveAndWriteCompletedSegment(*cache_with_limit.cache, cache_base_path3, "query_limit_key_2", 0, 8, 8));
}

TEST_F(FileCacheQueryLimitTest, QueryLimitUnchargesSegmentsEvictedByAnotherQuery)
{
    /// A segment is evicted by a query other than the one which cached it: it must stop counting
    /// against the budget of the query which cached it, the same as for self-eviction.
    ServerUUID::setRandomForUnitTests();

    const std::string cache_path = cache_base_path2;
    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_path;
    settings[FileCacheSetting::max_size] = 25;
    settings[FileCacheSetting::max_elements] = 100;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;
    settings[FileCacheSetting::enable_filesystem_query_cache_limit] = true;
    settings[FileCacheSetting::reserve_granularity] = 0;

    DB::FileCache cache("query_limit_cross_query", settings);
    cache.initialize();

    FilesystemCacheSettings limited_settings;
    limited_settings.query_limit_bytes = 12;
    limited_settings.skip_download_if_exceeds_per_query_cache_write_limit = true;

    /// The evicting query must not be limited itself, it has to reserve more than the cache holds.
    FilesystemCacheSettings unlimited_settings;
    unlimited_settings.query_limit_bytes = 1000;
    unlimited_settings.skip_download_if_exceeds_per_query_cache_write_limit = true;

    std::mutex mutex;
    std::condition_variable cv;
    /// 1: the limited query cached its segments, 2: the other query evicted them.
    int stage = 0;
    bool cached_after_eviction = false;

    /// The limited query stays alive (thread group and holder) while the other query evicts.
    std::thread limited_query([&]
    {
        DB::ThreadStatus thread_status;
        auto query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId("cross_query_limited");
        auto query_scope_holder = DB::QueryScope::create(query_context);
        auto holder = cache.getQueryContextHolder("cross_query_limited", limited_settings);

        /// 5 + 5 of the 12 byte budget.
        EXPECT_TRUE(reserveAndWriteCompletedSegment(cache, cache_path, "cross_query_a", 0, 5, 5));
        EXPECT_TRUE(reserveAndWriteCompletedSegment(cache, cache_path, "cross_query_b", 0, 5, 5));

        {
            std::lock_guard lock(mutex);
            stage = 1;
        }
        cv.notify_all();
        {
            std::unique_lock lock(mutex);
            cv.wait(lock, [&] { return stage == 2; });
        }

        /// Both segments of this query are gone from the cache, so 5 more bytes fit into the
        /// budget again. While they were still charged, 10 + 5 exceeded the 12 byte limit.
        cached_after_eviction = reserveAndWriteCompletedSegment(cache, cache_path, "cross_query_c", 0, 5, 5);
    });

    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return stage == 1; });
    }

    {
        DB::ThreadStatus thread_status;
        auto query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId("cross_query_evicting");
        auto query_scope_holder = DB::QueryScope::create(query_context);
        auto holder = cache.getQueryContextHolder("cross_query_evicting", unlimited_settings);

        /// 10 + 21 does not fit into the 25 byte cache, so this evicts both segments above.
        ASSERT_TRUE(reserveAndWriteCompletedSegment(cache, cache_path, "cross_query_d", 0, 21, 21));
    }

    {
        std::lock_guard lock(mutex);
        stage = 2;
    }
    cv.notify_all();
    limited_query.join();

    ASSERT_TRUE(cached_after_eviction);
}

TEST_F(FileCacheQueryLimitTest, QueryLimitUnchargeDoesNotUnderflow)
{
    /// The bytes given back on file segment completion belong to the whole segment, so they can be
    /// more than this query reserved of it (another query may have reserved the rest). Uncharging
    /// must not underflow the per-query accounting.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const std::string query_id = "query_id_uncharge_underflow";
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    CacheWithQueryLimit cache_with_limit(
        "query_limit_underflow", cache_base_path3, query_id,
        /* query_limit_bytes */20, /* reserve_granularity */0);
    ASSERT_TRUE(cache_with_limit.holder != nullptr);

    ASSERT_TRUE(cache_with_limit.tryCacheSegment(0, 10, 10, cache_base_path3));

    /// More bytes than the 10 this query is charged for the segment.
    cache_with_limit.holder->context->tryDecrementSize(DB::FileCacheKey::fromPath("query_limit_key"), 0, 1000);

    /// The budget is intact (not wrapped around), so 10 more bytes still fit into the limit.
    ASSERT_TRUE(cache_with_limit.tryCacheSegment(100, 10, 10, cache_base_path3));
}

TEST_F(FileCacheQueryLimitTest, QueryLimitIsCumulative)
{
    /// `query_limit_bytes` is a budget for the whole query, not for a single
    /// reservation: two 10 byte segments must not both fit into a 15 byte budget.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const std::string query_id = "query_id_cumulative_limit";
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    CacheWithQueryLimit cache_with_limit(
        "query_limit_cumulative", cache_base_path, query_id,
        /* query_limit_bytes */15, /* reserve_granularity */0);
    ASSERT_TRUE(cache_with_limit.holder != nullptr);

    ASSERT_TRUE(cache_with_limit.tryCacheSegment(0, 10, 10, cache_base_path));
    ASSERT_FALSE(cache_with_limit.tryCacheSegment(100, 10, 10, cache_base_path));
}

TEST_F(FileCacheQueryLimitTest, QueryLimitGivesBackReservedButNotWrittenBytes)
{
    /// Reserve-ahead charges a whole granule against the query budget, but the part which was
    /// never written is given back on file segment completion, so it must not be charged either:
    /// after downloading 2 of the 8 reserved bytes, 8 more bytes still fit into a 15 byte budget.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const std::string query_id = "query_id_reserved_but_not_written";
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    CacheWithQueryLimit cache_with_limit(
        "query_limit_not_written", cache_base_path3, query_id,
        /* query_limit_bytes */15, /* reserve_granularity */8);
    ASSERT_TRUE(cache_with_limit.holder != nullptr);

    ASSERT_TRUE(cache_with_limit.tryCacheSegment(0, 8, 2, cache_base_path3));
    ASSERT_TRUE(cache_with_limit.tryCacheSegment(100, 8, 8, cache_base_path3));
}

TEST_F(FileCacheQueryLimitTest, QueryLimitSurvivesReadBufferLifetime)
{
    /// Read buffers of one query do not necessarily overlap in time, and each of them creates its
    /// own query context holder. The budget must belong to the query, not to a buffer: it must not
    /// restart when all current buffers are destroyed while the query is still running.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const std::string query_id = "query_id_buffer_lifetime";
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    CacheWithQueryLimit cache_with_limit(
        "query_limit_buffer_lifetime", cache_base_path2, query_id,
        /* query_limit_bytes */15, /* reserve_granularity */0);
    ASSERT_TRUE(cache_with_limit.holder != nullptr);

    ASSERT_TRUE(cache_with_limit.tryCacheSegment(0, 10, 10, cache_base_path2));
    cache_with_limit.recreateQueryContextHolder();
    ASSERT_FALSE(cache_with_limit.tryCacheSegment(100, 10, 10, cache_base_path2));
}

TEST_F(FileCacheQueryLimitTest, QueryLimitUnchargeKeepsInFlightReservationsUsable)
{
    /// A segment can leave the cache while another thread is reserving for it: that thread holds
    /// the record's queue entry between the two cache locks. Uncharging must release the charge and
    /// drop the record without invalidating the entry that thread holds, which made the reservation
    /// fail on an invalid iterator before.
    ServerUUID::setRandomForUnitTests();

    const std::string cache_path = caches_dir / "test_query_limit_uncharge";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path,
                                 /* background_download_queue_size_limit */0,
                                 /* background_download_threads */0,
                                 /* write_cache_per_user_directory */false);

    const auto key = DB::FileCacheKey::fromPath("uncharge_key");
    const auto & origin = FileCache::getCommonOrigin();
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(origin), &cache_metadata);

    CacheStateGuard state_guard;
    FileCacheQueryLimit::QueryContext context(/* query_cache_size */100, /* recache_on_query_limit_exceeded */false);

    /// A reservation in flight: the record exists, its size is not accounted yet.
    auto it = context.add(key_metadata, /* offset */0, /* size */0);
    ASSERT_TRUE(it != nullptr);

    /// The segment leaves the cache meanwhile (evicted or dropped by anyone).
    context.unchargeRemoved(key, /* offset */0);

    /// The record is gone, so a new reservation starts a new one instead of reusing a dead entry.
    ASSERT_TRUE(context.tryGet(key, /* offset */0) == nullptr);

    /// The reservation which was already in flight still completes on the entry it holds.
    it->incrementSize(10, state_guard.lock());
    ASSERT_EQ(context.getPriority().getSize(state_guard.lock()), 10u);

    /// The next reservation of this query takes the invalidated entry out of the queue, which gives
    /// those bytes back and keeps the queue from growing with every segment the query has cached.
    context.removeInvalidatedEntries(/* max_batch */10);
    ASSERT_EQ(context.getPriority().getSize(state_guard.lock()), 0u);
    ASSERT_EQ(context.getPriority().getElementsCount(state_guard.lock()), 0u);
}

TEST_F(FileCacheQueryLimitTest, QueryLimitChargesReservationsWithoutAQuery)
{
    /// A background download continues a segment from a thread which belongs to no query. Those
    /// bytes must be charged to the query which reserved the segment so far, otherwise they escape
    /// its budget entirely.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const std::string owner_query_id = "no_query_thread_owner";
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(owner_query_id);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    CacheWithQueryLimit cache_with_limit(
        "query_limit_no_query_thread", cache_base_path2, owner_query_id,
        /* query_limit_bytes */12, /* reserve_granularity */0);
    ASSERT_TRUE(cache_with_limit.holder != nullptr);

    /// This query reserves 8 of its 12 byte budget for the segment and writes 4 of them.
    auto segments = cache_with_limit.cache->getOrSet(
        DB::FileCacheKey::fromPath("query_limit_key"), 0, 16, INT_MAX, {}, 0, FileCache::getCommonOrigin());
    auto segment = *segments->begin();
    ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());

    std::string failure_reason;
    ASSERT_TRUE(segment->reserve(8, 1000, failure_reason));

    auto key_str = segment->key().toString();
    fs::create_directories(fs::path(cache_base_path2) / key_str.substr(0, 3) / key_str);
    std::string data(4, '0');
    segment->write(data.data(), data.size(), segment->getCurrentWriteOffset());

    /// Hand the download over, as completing a segment for background download does.
    segment->resetDownloader();

    /// Continuing the download from a thread without a query of its own needs 8 more bytes, which
    /// do not fit into what the owner has left: 8 + 8 exceeds 12.
    bool reserved_without_a_query = true;
    std::thread download_thread([&]
    {
        EXPECT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());
        std::string background_failure_reason;
        reserved_without_a_query = segment->reserve(12, 1000, background_failure_reason);
    });
    download_thread.join();

    ASSERT_FALSE(reserved_without_a_query);
}

TEST_F(FileCacheQueryLimitTest, QueryLimitContextIsDroppedWhenTheNextQueryArrives)
{
    /// A query which released its last holder before finishing leaves its context behind. It must
    /// not stay for the lifetime of the cache: the next query arriving cleans it up.
    ServerUUID::setRandomForUnitTests();

    FilesystemCacheSettings read_settings;
    read_settings.query_limit_bytes = 1024;

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path3;
    settings[FileCacheSetting::max_size] = 1000;
    settings[FileCacheSetting::max_elements] = 100;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::enable_filesystem_query_cache_limit] = true;

    DB::FileCache cache("query_limit_context_sweep", settings);
    cache.initialize();

    /// The first query takes a holder and drops it while still running, then finishes.
    {
        DB::ThreadStatus thread_status;
        auto query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId("sweep_finished_query");
        auto query_scope_holder = DB::QueryScope::create(query_context);

        auto holder = cache.getQueryContextHolder("sweep_finished_query", read_settings);
        ASSERT_TRUE(holder != nullptr);
        /// Dropping the holder must keep the context, the query is still running.
        holder = nullptr;
        ASSERT_TRUE(cache.isQueryLimitInUse());
    }

    /// The next query arrives: the finished query's context is gone, only this one is left.
    {
        DB::ThreadStatus thread_status;
        auto query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId("sweep_next_query");
        auto query_scope_holder = DB::QueryScope::create(query_context);

        auto holder = cache.getQueryContextHolder("sweep_next_query", read_settings);
        ASSERT_TRUE(holder != nullptr);
        ASSERT_EQ(cache.getQueryLimitContextsCount(), 1u);
    }
}

TEST_F(FileCacheQueryLimitTest, QueryLimitContextRevivedDuringRelease)
{
    /// Regression for STID 4192-71db: a holder for some query_id decides it is the last one and
    /// releases its query context, but a concurrent holder for the same query_id revives the
    /// context first. The release must then be a no-op: the revived context must survive (so the
    /// per-query download limit keeps being enforced for the rest of the query) and a later release
    /// of the revived context must not fail with "Attempt to release query context that does not exist".

    FileCacheQueryLimit query_limit;

    const std::string query_id = "query_id_revive";
    FilesystemCacheSettings cache_settings;
    cache_settings.query_limit_bytes = 1024;

    /// holder1 takes the context; query_map and holder1 both reference it (use_count == 2).
    auto context1 = query_limit.getOrSetQueryContext(query_id, cache_settings);
    ASSERT_TRUE(context1 != nullptr);
    ASSERT_EQ(context1.use_count(), 2);

    /// holder2 revives the same context before holder1 releases (getOrSetQueryContext returns the
    /// existing entry). Now query_map, holder1 and holder2 all reference it (use_count == 3).
    auto context2 = query_limit.getOrSetQueryContext(query_id, cache_settings);
    ASSERT_EQ(context1.get(), context2.get());
    ASSERT_EQ(context1.use_count(), 3);

    /// holder1 releases. The map still maps query_id to the live context and another holder is
    /// alive, so the entry must be kept (no erase, no throw) and nothing is handed back for
    /// destruction.
    std::vector<FileCacheQueryLimit::QueryContextPtr> doomed1;
    ASSERT_NO_THROW(doomed1 = query_limit.removeQueryContext(context1));
    ASSERT_TRUE(doomed1.empty());
    context1.reset();

    /// Enforcement is preserved: the revived context is still discoverable.
    {
        DB::ThreadStatus thread_status;
        auto query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId(query_id);
        auto query_scope_holder = DB::QueryScope::create(query_context);

        auto found = query_limit.tryGetQueryContext();
        ASSERT_EQ(found.get(), context2.get());
    }

    /// holder2 is now the last holder; releasing it actually removes the entry, once, and hands the
    /// orphaned context back so it is destroyed by the caller outside the cache lock.
    const auto * context2_raw = context2.get();
    std::vector<FileCacheQueryLimit::QueryContextPtr> doomed2;
    ASSERT_NO_THROW(doomed2 = query_limit.removeQueryContext(context2));
    ASSERT_EQ(doomed2.size(), 1u);
    ASSERT_EQ(doomed2[0].get(), context2_raw);
    ASSERT_EQ(doomed2[0].use_count(), 1);
    context2.reset();

    /// After full release the context is gone.
    {
        DB::ThreadStatus thread_status;
        auto query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId(query_id);
        auto query_scope_holder = DB::QueryScope::create(query_context);

        auto found = query_limit.tryGetQueryContext();
        ASSERT_EQ(found.get(), nullptr);
    }
}

TEST_F(FileCacheQueryLimitTest, QueryLimitConcurrentReleaseNoLeak)
{
    /// Regression for #109508: two holders for the same query_id release "at the same time".
    /// A query with parallel read streams has several holders (each CachedOnDiskReadBufferFromFile
    /// creates its own), so use_count is > 2. If the last-holder decision reads use_count before this
    /// holder drops its own reference (or drops it outside the lock), both releasers observe the shared
    /// count, both skip the erase, and after both drop their reference only the map entry remains and is
    /// never removed. That orphans query_map[query_id] for the lifetime of the cache and lets a later
    /// query reusing the same query_id pick up stale per-query limit state. The fix drops each holder's
    /// reference under the lock and erases once the map entry is the sole owner.

    FileCacheQueryLimit query_limit;

    const std::string query_id = "query_id_concurrent_release";
    FilesystemCacheSettings cache_settings;
    cache_settings.query_limit_bytes = 1024;

    /// Two holders take the same context; query_map + both holders reference it (use_count == 3).
    auto context1 = query_limit.getOrSetQueryContext(query_id, cache_settings);
    auto context2 = query_limit.getOrSetQueryContext(query_id, cache_settings);
    ASSERT_EQ(context1.get(), context2.get());
    ASSERT_EQ(context1.use_count(), 3);

    /// Keep a raw pointer to assert which release actually surrenders the context for destruction.
    const auto * context_raw = context1.get();

    /// Both holders decide to release while both are still alive (the interleaving that leaks): each
    /// removeQueryContext drops that holder's reference under the lock. The first keeps the entry (one
    /// holder still alive) and returns nullptr; the second erases it and returns the now-orphaned
    /// context so the caller destroys it after the cache lock is released. Neither throws.
    std::vector<FileCacheQueryLimit::QueryContextPtr> doomed1;
    std::vector<FileCacheQueryLimit::QueryContextPtr> doomed2;
    ASSERT_NO_THROW(doomed1 = query_limit.removeQueryContext(context1));
    ASSERT_NO_THROW(doomed2 = query_limit.removeQueryContext(context2));

    /// removeQueryContext resets each passed reference, so both are already null here.
    ASSERT_EQ(context1, nullptr);
    ASSERT_EQ(context2, nullptr);

    /// Only the last release hands the context back for out-of-lock destruction; the earlier one
    /// returns nullptr because another holder was still alive.
    ASSERT_TRUE(doomed1.empty());
    ASSERT_EQ(doomed2.size(), 1u);
    ASSERT_EQ(doomed2[0].get(), context_raw);
    ASSERT_EQ(doomed2[0].use_count(), 1);

    /// The entry must be gone: with the pre-fix logic both releases skipped the erase and the entry
    /// leaked, so tryGetQueryContext would still find it.
    {
        DB::ThreadStatus thread_status;
        auto query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId(query_id);
        auto query_scope_holder = DB::QueryScope::create(query_context);

        auto found = query_limit.tryGetQueryContext();
        ASSERT_EQ(found.get(), nullptr);
    }
}
