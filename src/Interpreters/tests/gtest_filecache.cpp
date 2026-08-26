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
namespace fs = std::filesystem;
using namespace DB;

static constexpr auto TEST_LOG_LEVEL = "debug";

/// Diagnostics must go through this logger: the root channel is a `ConsoleChannel(std::cerr)` that
/// serialises its writers under a static mutex, which a bare `std::cerr <<` would not take.
/// Must stay lazy: a logger keeps whatever channel the root had when it was created, so one created
/// before `SetUp` installs the channel silently drops every message.
static LoggerPtr testLog()
{
    return getLogger("FileCacheTest");
}

/// For waits which must observe the concurrent download finishing, so they cannot use the
/// (much smaller) timeout a query would pass.
static constexpr size_t TEST_WAIT_FOR_DOWNLOAD_TIMEOUT_MS = 60000;

namespace DB::ErrorCodes
{
    extern const int FILECACHE_ACCESS_DENIED;
    extern const int CANNOT_READ_ALL_DATA;
}
namespace DB::FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsUInt64 max_file_segment_size;
    extern const FileCacheSettingsUInt64 boundary_alignment;
    extern const FileCacheSettingsFileCachePolicy cache_policy;
    extern const FileCacheSettingsDouble slru_size_ratio;
    extern const FileCacheSettingsDouble keep_free_space_elements_ratio;
    extern const FileCacheSettingsNonZeroUInt64 load_metadata_threads;
    extern const FileCacheSettingsBool load_metadata_asynchronously;
    extern const FileCacheSettingsBool write_cache_per_user_id_directory;
    extern const FileCacheSettingsBool allow_dynamic_cache_resize;
    extern const FileCacheSettingsUInt64 idle_client_ttl_sec;
    extern const FileCacheSettingsUInt64 idle_client_check_interval_sec;
    extern const FileCacheSettingsBool expose_prometheus_eviction_metrics;
    extern const FileCacheSettingsBool expose_prometheus_eviction_metrics_per_user;
    extern const FileCacheSettingsBool enable_bypass_cache_with_threshold;
    extern const FileCacheSettingsUInt64 bypass_cache_threshold;
}

void printRanges(const auto & segments)
{
    String out;
    for (const auto & segment : segments)
        out += fmt::format("\n{} (state: {})", segment->range().toString(), DB::FileSegment::stateToString(segment->state()));
    LOG_DEBUG(testLog(), "Having file segments: {}", out);
}

[[maybe_unused]] static String getFileSegmentPath(const String & base_path, const DB::FileCache::Key & key, size_t offset)
{
    auto key_str = key.toString();
    return fs::path(base_path) / key_str.substr(0, 3) / key_str / DB::toString(offset);
}

static void download(const std::string & cache_base_path, DB::FileSegment & file_segment)
{
    const auto & key = file_segment.key();
    size_t size = file_segment.range().size();

    auto key_str = key.toString();
    auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) / key_str;
    if (!fs::exists(subdir))
        fs::create_directories(subdir);

    std::string data(size, '0');
    file_segment.write(data.data(), size, file_segment.getCurrentWriteOffset());
}

using Range = FileSegment::Range;
using Ranges = std::vector<Range>;
using State = FileSegment::State;
using States = std::vector<State>;
using Holder = FileSegmentsHolder;
using HolderPtr = FileSegmentsHolderPtr;

fs::path caches_dir = fs::current_path() / "lru_cache_test";
std::string cache_base_path = caches_dir / "cache1" / "";
std::string cache_base_path2 = caches_dir / "cache2" / "";
std::string cache_base_path3 = caches_dir / "cache3" / "";


static void assertEqual(const FileSegmentsHolderPtr & file_segments, const Ranges & expected_ranges, const States & expected_states = {})
{
    {
        String got;
        for (const auto & file_segment : *file_segments)
            got += file_segment->range().toString() + ", ";
        String expected;
        for (const auto & r : expected_ranges)
            expected += r.toString() + ", ";
        LOG_DEBUG(testLog(), "File segments: {}\nExpected: {}", got, expected);
    }

    ASSERT_EQ(file_segments->size(), expected_ranges.size());

    if (!expected_states.empty())
        ASSERT_EQ(file_segments->size(), expected_states.size());

    auto get_expected_state = [&](size_t i)
    {
        if (expected_states.empty())
            return State::DOWNLOADED;
        else
            return expected_states[i];
    };

    size_t i = 0;
    for (const auto & file_segment : *file_segments)
    {
        ASSERT_EQ(file_segment->range(), expected_ranges[i]);
        ASSERT_EQ(file_segment->state(), get_expected_state(i));
        ++i;
    }
}

static void assertEqual(const std::vector<FileSegment::Info> & file_segments, const Ranges & expected_ranges, const States & expected_states = {})
{
    {
        String got;
        for (const auto & file_segment : file_segments)
            got += FileSegment::Range(file_segment.range_left, file_segment.range_right).toString() + ", ";
        String expected;
        for (const auto & r : expected_ranges)
            expected += r.toString() + ", ";
        LOG_DEBUG(testLog(), "File segments: {}\nExpected: {}", got, expected);
    }

    ASSERT_EQ(file_segments.size(), expected_ranges.size());

    if (!expected_states.empty())
        ASSERT_EQ(file_segments.size(), expected_states.size());

    auto get_expected_state = [&](size_t i)
    {
        if (expected_states.empty())
            return State::DOWNLOADED;
        else
            return expected_states[i];
    };

    size_t i = 0;
    for (const auto & file_segment : file_segments)
    {
        ASSERT_EQ(FileSegment::Range(file_segment.range_left, file_segment.range_right), expected_ranges[i]);
        ASSERT_EQ(file_segment.state, get_expected_state(i));
        ++i;
    }
}

static void assertEqual(const IFileCachePriority::PriorityDumpPtr & dump, const Ranges & expected_ranges, const States & expected_states = {})
{
    if (const auto * lru = dynamic_cast<const LRUFileCachePriority::IPriorityDump *>(dump.get()))
    {
        assertEqual(lru->infos, expected_ranges, expected_states);
    }
    else
    {
        ASSERT_TRUE(false);
    }
}

static void assertProtectedOrProbationary(const std::vector<FileSegmentInfo> & file_segments, const Ranges & expected, bool assert_protected)
{
    /// Logged before the first assertion, so a failure still reports every segment.
    {
        String got;
        for (const auto & f : file_segments)
        {
            auto range = FileSegment::Range(f.range_left, f.range_right);
            bool is_protected = (f.queue_entry_type == IFileCachePriority::QueueEntryType::SLRU_Protected);
            got += fmt::format("{} (protected: {}), ", range.toString(), is_protected);
        }
        String expected_str;
        for (const auto & range : expected)
            expected_str += range.toString() + ", ";
        LOG_DEBUG(testLog(), "File segments: {}\nExpected: {}", got, expected_str);
    }

    std::vector<Range> res;
    for (const auto & f : file_segments)
    {
        auto range = FileSegment::Range(f.range_left, f.range_right);
        bool is_protected = (f.queue_entry_type == IFileCachePriority::QueueEntryType::SLRU_Protected);
        bool is_probationary = (f.queue_entry_type == IFileCachePriority::QueueEntryType::SLRU_Probationary);
        ASSERT_TRUE(is_probationary || is_protected);

        if ((is_protected && assert_protected) || (!is_protected && !assert_protected))
        {
            res.push_back(range);
        }
    }

    ASSERT_EQ(res.size(), expected.size());
    for (size_t i = 0; i < res.size(); ++i)
    {
        ASSERT_EQ(res[i], expected[i]);
    }
}

static void assertProtected(const std::vector<FileSegmentInfo> & file_segments, const Ranges & expected)
{
    LOG_DEBUG(testLog(), "Assert protected");
    assertProtectedOrProbationary(file_segments, expected, true);
}

static void assertProbationary(const std::vector<FileSegmentInfo> & file_segments, const Ranges & expected)
{
    LOG_DEBUG(testLog(), "Assert probationary");
    assertProtectedOrProbationary(file_segments, expected, false);
}

static void assertProtected(const IFileCachePriority::PriorityDumpPtr & dump, const Ranges & expected)
{
    if (const auto * lru = dynamic_cast<const LRUFileCachePriority::IPriorityDump *>(dump.get()))
    {
        assertProtected(lru->infos, expected);
    }
    else
    {
        ASSERT_TRUE(false);
    }
}

static void assertProbationary(const IFileCachePriority::PriorityDumpPtr & dump, const Ranges & expected)
{
    if (const auto * lru = dynamic_cast<const LRUFileCachePriority::IPriorityDump *>(dump.get()))
    {
        assertProbationary(lru->infos, expected);
    }
    else
    {
        ASSERT_TRUE(false);
    }
}

static FileSegmentPtr get(const HolderPtr & holder, int i)
{
    auto it = std::next(holder->begin(), i);
    if (it == holder->end())
        std::terminate();
    return *it;
}

static void download(FileSegmentPtr file_segment, bool complete = true)
{
    LOG_DEBUG(testLog(), "Downloading range {}", file_segment->range().toString());

    ASSERT_EQ(file_segment->getOrSetDownloader(), FileSegment::getCallerId());
    ASSERT_EQ(file_segment->state(), State::DOWNLOADING);
    ASSERT_EQ(file_segment->getDownloadedSize(), 0);

    std::string failure_reason;
    ASSERT_TRUE(file_segment->reserve(file_segment->range().size(), 1000, failure_reason));
    download(cache_base_path, *file_segment);
    ASSERT_EQ(file_segment->state(), State::DOWNLOADING);

    if (complete)
    {
        FileSegment::complete(FileSegmentPtr(file_segment), /*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
        ASSERT_EQ(file_segment->state(), State::DOWNLOADED);
    }
}

static void assertDownloadFails(FileSegmentPtr file_segment)
{
    ASSERT_EQ(file_segment->getOrSetDownloader(), FileSegment::getCallerId());
    ASSERT_EQ(file_segment->getDownloadedSize(), 0);
    std::string failure_reason;
    ASSERT_FALSE(file_segment->reserve(file_segment->range().size(), 1000, failure_reason));
    FileSegment::complete(FileSegmentPtr(file_segment), /*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
}

static void download(const HolderPtr & holder)
{
    for (auto & it : *holder)
    {
        download(it);
    }
}

static void increasePriority(const HolderPtr & holder)
{
    for (auto & it : *holder)
        it->increasePriority();
}

[[maybe_unused]] static void increasePriority(const HolderPtr & holder, size_t pos)
{
    FileSegments::iterator it = holder->begin();
    std::advance(it, pos);
    (*it)->increasePriority();
}

class FileCacheTest : public ::testing::Test
{
public:
    FileCacheTest()
    {
        /// Reset current_thread to avoid conflicts of ThreadStatus with MainThreadStatus
        current_thread = nullptr;

        /// Context has to be created before calling cache.initialize();
        /// Otherwise the tests which run before FileCacheTest.get are failed
        /// It is logical to call destroyContext() at destructor.
        /// But that wouldn't work because for proper initialization and destruction global/static objects
        /// testing::Environment has to be used.
        getContext();
    }

    ~FileCacheTest() override
    {
        /// Reset current_thread back
        current_thread = MainThreadStatus::get();
    }

    static void setupLogs(const std::string & level)
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);
        Poco::Logger::root().setLevel(level);
    }

    void SetUp() override
    {
        if(const char * test_log_level = std::getenv("TEST_LOG_LEVEL")) // NOLINT(concurrency-mt-unsafe)
            setupLogs(test_log_level);
        else
            setupLogs(TEST_LOG_LEVEL);

        UInt64 seed = randomSeed();
        if (const char * random_seed = std::getenv("TEST_RANDOM_SEED")) // NOLINT(concurrency-mt-unsafe)
            seed = std::stoull(random_seed);
        std::cout << "TEST_RANDOM_SEED=" << seed << std::endl;
        rng = pcg64(seed);

        if (fs::exists(cache_base_path))
            fs::remove_all(cache_base_path);
        if (fs::exists(cache_base_path2))
            fs::remove_all(cache_base_path2);
        if (fs::exists(cache_base_path3))
            fs::remove_all(cache_base_path3);
        fs::create_directories(cache_base_path);
        fs::create_directories(cache_base_path2);
        fs::create_directories(cache_base_path3);
    }

    void TearDown() override
    {
        if (fs::exists(cache_base_path))
            fs::remove_all(cache_base_path);
        if (fs::exists(cache_base_path3))
            fs::remove_all(cache_base_path3);
    }

    pcg64 rng;
};

TEST_F(FileCacheTest, LRUPolicy)
{
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    /// To work with cache need query_id and query context.
    std::string query_id = "query_id";

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse>
</clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 30;
    settings[FileCacheSetting::max_elements] = 5;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    const size_t file_size = INT_MAX; // the value doesn't really matter because boundary_alignment == 1.


    const auto & user = FileCache::getCommonOrigin();
    {
        auto cache = DB::FileCache("1", settings);
        cache.initialize();
        auto key = DB::FileCacheKey::fromPath("key1");

        auto get_or_set = [&](size_t offset, size_t size)
        {
            return cache.getOrSet(key, offset, size, file_size, {}, 0, user);
        };

        {
            auto holder = get_or_set(0, 10); /// Add range [0, 9]
            assertEqual(holder, { Range(0, 9) }, { State::EMPTY });
            download(*holder->begin());
            assertEqual(holder, { Range(0, 9) }, { State::DOWNLOADED });
            increasePriority(holder);
        }

        /// Current cache:    [__________]
        ///                   ^          ^
        ///                   0          9
        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(0, 9) });
        assertEqual(cache.dumpQueue(), { Range(0, 9) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 1);
        ASSERT_EQ(cache.getUsedCacheSize(), 10);

        {
            /// Want range [5, 14], but [0, 9] already in cache, so only [10, 14] will be put in cache.
            auto holder = get_or_set(5, 10);
            assertEqual(holder, { Range(0, 9), Range(10, 14) }, { State::DOWNLOADED, State::EMPTY });
            download(get(holder, 1));
            assertEqual(holder, { Range(0, 9), Range(10, 14) }, { State::DOWNLOADED, State::DOWNLOADED });
            increasePriority(holder);
        }

        /// Current cache:    [__________][_____]
        ///                   ^          ^^     ^
        ///                   0          910    14
        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(0, 9), Range(10, 14) });
        assertEqual(cache.dumpQueue(), { Range(0, 9), Range(10, 14) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 2);
        ASSERT_EQ(cache.getUsedCacheSize(), 15);

        /// Get [9, 9]
        {
            auto holder = get_or_set(9, 1);
            assertEqual(holder, { Range(0, 9) }, { State::DOWNLOADED });
            increasePriority(holder);
        }

        assertEqual(cache.dumpQueue(), { Range(10, 14), Range(0, 9) });
        /// Get [9, 10]
        assertEqual(get_or_set(9, 2), {Range(0, 9), Range(10, 14)}, {State::DOWNLOADED, State::DOWNLOADED});

        /// Get [10, 10]
        {
            auto holder = get_or_set(10, 1);
            assertEqual(holder, { Range(10, 14) }, { State::DOWNLOADED });
            increasePriority(holder);
        }

        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(0, 9), Range(10, 14) });
        assertEqual(cache.dumpQueue(), { Range(0, 9), Range(10, 14) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 2);
        ASSERT_EQ(cache.getUsedCacheSize(), 15);

        {
            auto holder = get_or_set(17, 4);
            download(holder); /// Get [17, 20]
            increasePriority(holder);
        }

        {
            auto holder = get_or_set(24, 3);
            download(holder); /// Get [24, 26]
            increasePriority(holder);
        }

        {
            auto holder = get_or_set(27, 1);
            download(holder); /// Get [27, 27]
            increasePriority(holder);
        }

        /// Current cache:    [__________][_____]   [____]    [___][]
        ///                   ^          ^^     ^   ^    ^    ^   ^^^
        ///                   0          910    14  17   20   24  2627
        ///
        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(0, 9), Range(10, 14), Range(17, 20), Range(24, 26), Range(27, 27) });
        assertEqual(cache.dumpQueue(), { Range(0, 9), Range(10, 14), Range(17, 20), Range(24, 26), Range(27, 27) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 5);
        ASSERT_EQ(cache.getUsedCacheSize(), 23);

        {
            auto holder = get_or_set(0, 26);
            assertEqual(holder,
                        { Range(0, 9),       Range(10, 14),     Range(15, 16),  Range(17, 20),     Range(21, 23), Range(24, 26) },
                        { State::DOWNLOADED, State::DOWNLOADED, State::EMPTY,   State::DOWNLOADED, State::EMPTY,  State::DOWNLOADED });
            download(get(holder, 2)); /// [27, 27] was evicted.
            assertEqual(holder,
                        { Range(0, 9),       Range(10, 14),     Range(15, 16),     Range(17, 20),     Range(21, 23), Range(24, 26) },
                        { State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::EMPTY,  State::DOWNLOADED });
            assertDownloadFails(get(holder, 4));
            assertEqual(holder,
                        { Range(0, 9),       Range(10, 14),     Range(15, 16),     Range(17, 20),     Range(21, 23),     Range(24, 26) },
                        { State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, State::DOWNLOADED });

            /// Range [27, 27] must be evicted in previous getOrSet [0, 25].
            /// Let's not invalidate pointers to returned segments from range [0, 25] and
            /// as max elements size is reached, next attempt to put something in cache should fail.
            /// This will also check that [27, 27] was indeed evicted.
            auto holder2 = get_or_set(27, 1);
            assertEqual(holder2, { Range(27, 27) }, { State::EMPTY });
            assertDownloadFails(*holder2->begin());
            assertEqual(holder2, { Range(27, 27) }, { State::PARTIALLY_DOWNLOADED_NO_CONTINUATION });

            auto holder3 = get_or_set(28, 3);
            assertEqual(holder3, { Range(28, 30) }, { State::EMPTY });
            assertDownloadFails(*holder3->begin());
            assertEqual(holder3, { Range(28, 30) }, { State::PARTIALLY_DOWNLOADED_NO_CONTINUATION });

            increasePriority(holder);
            increasePriority(holder2);
            increasePriority(holder3);
        }

        /// Current cache:    [__________][_____][   ][____]    [___]
        ///                   ^                            ^    ^
        ///                   0                            20   24
        ///
        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(0, 9), Range(10, 14), Range(15, 16), Range(17, 20), Range(24, 26) });
        assertEqual(cache.dumpQueue(), { Range(0, 9), Range(10, 14), Range(15, 16), Range(17, 20), Range(24, 26) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 5);
        ASSERT_EQ(cache.getUsedCacheSize(), 24);

        {
            auto holder = get_or_set(12, 10);
            assertEqual(holder,
                        { Range(10, 14),     Range(15, 16),     Range(17, 20),     Range(21, 21) },
                        { State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::EMPTY });
            download(get(holder, 3));
            assertEqual(holder,
                        { Range(10, 14),     Range(15, 16),     Range(17, 20),     Range(21, 21) },
                        { State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED });
            increasePriority(holder);
        }

        /// Current cache:    [_____][__][____][_]   [___]
        ///                   ^          ^       ^   ^   ^
        ///                   10         17      21  24  26
        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(10, 14), Range(15, 16), Range(17, 20), Range(21, 21), Range(24, 26) });
        assertEqual(cache.dumpQueue(), { Range(24, 26), Range(10, 14), Range(15, 16), Range(17, 20), Range(21, 21) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 5);
        ASSERT_EQ(cache.getUsedCacheSize(), 15);

        {
            auto holder = get_or_set(23, 5);
            assertEqual(holder,
                        { Range(23, 23), Range(24, 26),     Range(27, 27) },
                        { State::EMPTY,  State::DOWNLOADED, State::EMPTY });
            download(get(holder, 0));
            download(get(holder, 2));
            increasePriority(holder);
        }

        /// Current cache:    [____][_]  [][___][__]
        ///                   ^       ^  ^^^   ^^  ^
        ///                   17      21 2324  26  27
        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(17, 20), Range(21, 21), Range(23, 23), Range(24, 26), Range(27, 27) });
        assertEqual(cache.dumpQueue(), { Range(17, 20), Range(21, 21), Range(23, 23), Range(24, 26), Range(27, 27) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 5);
        ASSERT_EQ(cache.getUsedCacheSize(), 10);

        {
            auto holder = get_or_set(2, 3); /// Get [2, 4]
            assertEqual(holder, { Range(2, 4) }, { State::EMPTY });

            auto holder2 = get_or_set(30, 2); /// Get [30, 31]
            assertEqual(holder2, { Range(30, 31) }, { State::EMPTY });

            download(get(holder, 0));
            download(get(holder2, 0));

            auto holder3 = get_or_set(23, 1); /// Get [23, 23]
            assertEqual(holder3, { Range(23, 23) }, { State::DOWNLOADED });

            auto holder4 = get_or_set(24, 3); /// Get [24, 26]
            assertEqual(holder4, { Range(24, 26) }, { State::DOWNLOADED });

            auto holder5 = get_or_set(27, 1); /// Get [27, 27]
            assertEqual(holder5, { Range(27, 27) }, { State::DOWNLOADED });

            auto holder6 = get_or_set(0, 40);
            assertEqual(holder6,
                        { Range(0, 1), Range(2, 4),        Range(5, 22), Range(23, 23),     Range(24, 26),     Range(27, 27),    Range(28, 29), Range(30, 31),     Range(32, 39) },
                        { State::EMPTY, State::DOWNLOADED, State::EMPTY, State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::EMPTY, State::DOWNLOADED, State::EMPTY });

            assertDownloadFails(get(holder6, 0));
            assertDownloadFails(get(holder6, 2));
            assertDownloadFails(get(holder6, 6));
            assertDownloadFails(get(holder6, 8));

            increasePriority(holder);
            increasePriority(holder2);
            increasePriority(holder3);
            increasePriority(holder4);
            increasePriority(holder5);
            increasePriority(holder6);
        }

        /// Current cache:    [___]       [_][___][_]   [__]
        ///                   ^   ^       ^  ^   ^  ^   ^  ^
        ///                   2   4       23 24  26 27  30 31
        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(2, 4), Range(23, 23), Range(24, 26), Range(27, 27), Range(30, 31) });
        assertEqual(cache.dumpQueue(), { Range(2, 4), Range(23, 23), Range(24, 26), Range(27, 27), Range(30, 31) });

        /// Get [2, 4]
        {
            auto holder = get_or_set(2, 3);
            assertEqual(holder, { Range(2, 4) }, { State::DOWNLOADED });
            increasePriority(holder);
        }


        {
            auto holder = get_or_set(25, 5); /// Get [25, 29]
            assertEqual(holder,
                        { Range(24, 26),     Range(27, 27),     Range(28, 29) },
                        { State::DOWNLOADED, State::DOWNLOADED, State::EMPTY });

            auto file_segment = get(holder, 2);
            ASSERT_TRUE(file_segment->getOrSetDownloader() == FileSegment::getCallerId());
            ASSERT_TRUE(file_segment->state() == State::DOWNLOADING);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&]
            {
                DB::ThreadStatus thread_status_1;
                auto query_context_1 = DB::Context::createCopy(getContext().context);
                query_context_1->makeQueryContext();
                query_context_1->setCurrentQueryId("query_id_1");
                chassert(&DB::CurrentThread::get() == &thread_status_1);
                auto query_scope_holder_1 = DB::QueryScope::create(query_context_1);

                auto holder2 = get_or_set(25, 5); /// Get [25, 29] once again.
                assertEqual(holder2,
                            { Range(24, 26),     Range(27, 27),     Range(28, 29) },
                            { State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADING });

                auto file_segment2 = get(holder2, 2);
                ASSERT_TRUE(file_segment2->getOrSetDownloader() != FileSegment::getCallerId());
                ASSERT_EQ(file_segment2->state(), State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                file_segment2->wait(file_segment2->range().right, TEST_WAIT_FOR_DOWNLOAD_TIMEOUT_MS);
                ASSERT_EQ(file_segment2->getDownloadedSize(), file_segment2->range().size());
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&]{ return lets_start_download; });
            }

            download(file_segment);
            ASSERT_EQ(file_segment->state(), State::DOWNLOADED);

            other_1.join();

            increasePriority(holder);
        }

        /// Current cache:    [___]       [___][_][__][__]
        ///                   ^   ^       ^   ^  ^^  ^^  ^
        ///                   2   4       24  26 27  2930 31
        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(2, 4), Range(24, 26), Range(27, 27), Range(28, 29), Range(30, 31) });
        assertEqual(cache.dumpQueue(), { Range(30, 31), Range(2, 4), Range(24, 26), Range(27, 27), Range(28, 29) });

        {
            /// Now let's check the similar case but getting ERROR state after segment->wait(), when
            /// state is changed not manually via segment->completeWithState(state) but from destructor of holder
            /// and notify_all() is also called from destructor of holder.

            auto holder = get_or_set(3, 23); /// Get [3, 25]
            assertEqual(holder,
                        { Range(2, 4),       Range(5, 23), Range(24, 26) },
                        { State::DOWNLOADED, State::EMPTY, State::DOWNLOADED });

            auto file_segment = get(holder, 1);
            ASSERT_TRUE(file_segment->getOrSetDownloader() == FileSegment::getCallerId());
            ASSERT_TRUE(file_segment->state() == State::DOWNLOADING);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&]
            {
                DB::ThreadStatus thread_status_1;
                auto query_context_1 = DB::Context::createCopy(getContext().context);
                query_context_1->makeQueryContext();
                query_context_1->setCurrentQueryId("query_id_1");
                chassert(&DB::CurrentThread::get() == &thread_status_1);
                auto query_scope_holder_1 = DB::QueryScope::create(query_context_1);

                auto holder2 = get_or_set(3, 23); /// get [3, 25] once again.
                assertEqual(holder,
                            { Range(2, 4),       Range(5, 23),       Range(24, 26) },
                            { State::DOWNLOADED, State::DOWNLOADING, State::DOWNLOADED });

                auto file_segment2 = get(holder, 1);
                ASSERT_TRUE(file_segment2->getDownloader() != FileSegment::getCallerId());

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                file_segment2->wait(file_segment2->range().left, TEST_WAIT_FOR_DOWNLOAD_TIMEOUT_MS);
                ASSERT_EQ(file_segment2->state(), DB::FileSegment::State::EMPTY);
                ASSERT_EQ(file_segment2->getOrSetDownloader(), DB::FileSegment::getCallerId());
                download(file_segment2);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&]{ return lets_start_download; });
            }

            holder = nullptr;
            other_1.join();
            ASSERT_TRUE(file_segment->state() == DB::FileSegment::State::DOWNLOADED);
        }
    }

    /// Current cache:    [___][        ][___][_][__]
    ///                   ^   ^^         ^   ^^  ^  ^
    ///                   2   45       24  2627 28 29

    {
        /// Test LRUCache::restore().

        auto cache2 = DB::FileCache("2", settings);
        cache2.initialize();
        auto key = DB::FileCacheKey::fromPath("key1");

        /// Get [2, 29]
        assertEqual(
            cache2.getOrSet(key, 2, 28, file_size, {}, 0, user),
            {Range(2, 4), Range(5, 23), Range(24, 26), Range(27, 27), Range(28, 29)},
            {State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED});
    }

    {
        /// Test max file segment size

        auto settings2 = settings;
        settings2[FileCacheSetting::max_file_segment_size] = 10;
        settings2[FileCacheSetting::path] = caches_dir / "cache2";
        settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;
        fs::create_directories(settings2[FileCacheSetting::path].value);
        auto cache2 = DB::FileCache("3", settings2);
        cache2.initialize();
        auto key = DB::FileCacheKey::fromPath("key1");

        /// Get [0, 24]
        assertEqual(
            cache2.getOrSet(key, 0, 25, file_size, {}, 0, user),
            {Range(0, 9), Range(10, 19), Range(20, 24)},
            {State::EMPTY, State::EMPTY, State::EMPTY});
    }

    {
        /// Test delayed cleanup

        auto cache = FileCache("4", settings);
        cache.initialize();
        const auto key = FileCacheKey::fromPath("key10");
        const auto key_path = cache.getKeyPath(key, user);

        cache.removeAllReleasable(user.user_id);
        ASSERT_EQ(cache.getUsedCacheSize(), 0);
        ASSERT_TRUE(!fs::exists(key_path));
        ASSERT_TRUE(!fs::exists(fs::path(key_path).parent_path()));

        download(cache.getOrSet(key, 0, 10, file_size, {}, 0, user));
        ASSERT_EQ(cache.getUsedCacheSize(), 10);
        /// A fully downloaded regular segment encodes its size in the file name (`<offset>_<size>`).
        ASSERT_TRUE(fs::exists(cache.getFileSegmentPath(key, 0, FileSegmentKind::Regular, user, /* size */10)));

        cache.removeAllReleasable(user.user_id);
        ASSERT_EQ(cache.getUsedCacheSize(), 0);
        ASSERT_TRUE(!fs::exists(key_path));
        ASSERT_TRUE(!fs::exists(cache.getFileSegmentPath(key, 0, FileSegmentKind::Regular, user, /* size */10)));
    }

    {
        /// Test background thread delated cleanup

        auto cache = DB::FileCache("5", settings);
        cache.initialize();
        const auto key = FileCacheKey::fromPath("key10");
        const auto key_path = cache.getKeyPath(key, user);

        cache.removeAllReleasable(user.user_id);
        ASSERT_EQ(cache.getUsedCacheSize(), 0);
        ASSERT_TRUE(!fs::exists(key_path));
        ASSERT_TRUE(!fs::exists(fs::path(key_path).parent_path()));

        download(cache.getOrSet(key, 0, 10, file_size, {}, 0, user));
        ASSERT_EQ(cache.getUsedCacheSize(), 10);
        ASSERT_TRUE(fs::exists(key_path));

        cache.removeAllReleasable(user.user_id);
        ASSERT_EQ(cache.getUsedCacheSize(), 0);
        sleepForSeconds(2);
        ASSERT_TRUE(!fs::exists(key_path));
    }
}

TEST_F(FileCacheTest, writeBuffer)
{
    ServerUUID::setRandomForUnitTests();
    FileCacheSettings settings;
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 5;
    settings[FileCacheSetting::max_file_segment_size] = 5;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    FileCache cache("6", settings);
    cache.initialize();
    const auto & user = FileCache::getCommonOrigin();

    auto write_to_cache = [&, this](const String & key, const Strings & data, bool flush, ReadBufferPtr * out_read_buffer = nullptr)
    {
        CreateFileSegmentSettings segment_settings;
        segment_settings.kind = FileSegmentKind::Ephemeral;
        segment_settings.unbounded = true;

        auto cache_key = FileCacheKey::fromPath(key);
        auto holder = cache.set(cache_key, 0, 3, segment_settings, user);
        /// The same is done in TemporaryDataOnDisk::createStreamToCacheFile.
        std::filesystem::create_directories(cache.getKeyPath(cache_key, user));
        EXPECT_EQ(holder->size(), 1);
        auto & segment = holder->front();
        WriteBufferToFileSegment out(&segment);
        std::list<std::thread> threads;
        std::mutex mu;

        /// get random permutation of indexes
        std::vector<size_t> indexes(data.size());
        iota(indexes.data(), indexes.size(), size_t(0));
        std::shuffle(indexes.begin(), indexes.end(), rng);

        for (auto i : indexes)
        {
            /// Write from diffetent threads to check
            /// that no assertions inside cache related to downloaderId are triggered
            const auto & s = data[i];
            threads.emplace_back([&]
            {
                std::unique_lock lock(mu);
                out.write(s.data(), s.size());
                /// test different buffering scenarios
                if (flush)
                    out.next();
            });
        }
        for (auto & t : threads)
            t.join();

        out.finalize();
        if (out_read_buffer)
            *out_read_buffer = out.tryGetReadBuffer();
        return holder;
    };

    std::vector<fs::path> file_segment_paths;
    {
        auto holder = write_to_cache("key1", {"abc", "defg"}, false);
        file_segment_paths.emplace_back(holder->front().getPath());

        ASSERT_EQ(fs::file_size(file_segment_paths.back()), 7);
        EXPECT_EQ(holder->front().range().size(), 7);
        EXPECT_EQ(holder->front().range().left, 0);
        ASSERT_EQ(cache.getUsedCacheSize(), 7);

        {
            ReadBufferPtr reader = nullptr;

            auto holder2 = write_to_cache("key2", {"22", "333", "4444", "55555", "1"}, true, &reader);
            file_segment_paths.emplace_back(holder2->front().getPath());

            LOG_DEBUG(testLog(), "File segments: {}", holder2->toString());

            ASSERT_EQ(fs::file_size(file_segment_paths.back()), 15);
            EXPECT_TRUE(reader);
            if (reader)
            {
                String result;
                readStringUntilEOF(result, *reader);
                /// sort result to make it independent of the order of writes
                std::sort(result.begin(), result.end());
                EXPECT_EQ(result, "122333444455555");
            }

            EXPECT_EQ(holder2->front().range().size(), 15);
            EXPECT_EQ(holder2->front().range().left, 0);
            ASSERT_EQ(cache.getUsedCacheSize(), 22);
        }
        ASSERT_FALSE(fs::exists(file_segment_paths.back()));
        ASSERT_EQ(cache.getUsedCacheSize(), 7);
    }

    for (const auto & file_segment_path : file_segment_paths)
    {
        ASSERT_FALSE(fs::exists(file_segment_path));
    }
    ASSERT_EQ(cache.getUsedCacheSize(), 0);
}


static Block generateBlock(size_t size = 0)
{
    Block block;
    ColumnWithTypeAndName column;
    column.name = "x";
    column.type = std::make_shared<DataTypeUInt64>();

    {
        MutableColumnPtr mut_col = column.type->createColumn();
        for (size_t i = 0; i < size; ++i)
            mut_col->insert(i);
        column.column = std::move(mut_col);
    }

    block.insert(column);
    return block;
}

static size_t readAllTemporaryData(NativeReader & stream)
{
    Block block;
    size_t read_rows = 0;
    do
    {
        block = stream.read();
        read_rows += block.rows();
    } while (!block.empty());
    return read_rows;
}

TEST_F(FileCacheTest, temporaryData)
try
{
    ServerUUID::setRandomForUnitTests();
    DB::FileCacheSettings settings;
    settings[FileCacheSetting::max_size] = 10_KiB;
    settings[FileCacheSetting::max_file_segment_size] = 1_KiB;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    DB::FileCache file_cache("7", settings);
    file_cache.initialize();

    const auto & user = FileCache::getCommonOrigin();
    auto tmp_data_scope = std::make_shared<TemporaryDataOnDiskScope>(TemporaryDataOnDiskSettings{}, &file_cache);

    auto some_data_holder = file_cache.getOrSet(FileCacheKey::fromPath("some_data"), 0, 5_KiB, 5_KiB, CreateFileSegmentSettings{}, 0, user);

    {
        ASSERT_EQ(some_data_holder->size(), 5);
        std::string failure_reason;
        for (auto & segment : *some_data_holder)
        {
            ASSERT_TRUE(segment->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(segment->reserve(segment->range().size(), 1000, failure_reason));
            download(segment);
        }
    }

    size_t size_used_before_temporary_data = file_cache.getUsedCacheSize();
    size_t segments_used_before_temporary_data = file_cache.getFileSegmentsNum();
    ASSERT_GT(size_used_before_temporary_data, 0);
    ASSERT_GT(segments_used_before_temporary_data, 0);

    size_t size_used_with_temporary_data = {};
    size_t segments_used_with_temporary_data = {};


    {
        TemporaryBlockStreamHolder stream(std::make_shared<const Block>(generateBlock()), tmp_data_scope);
        ASSERT_TRUE(stream);
        /// Do nothing with stream, just create it and destroy.
    }

    {
        TemporaryBlockStreamHolder stream(std::make_shared<const Block>(generateBlock()), tmp_data_scope);
        ASSERT_GT(stream->write(generateBlock(100)), 0);

        ASSERT_GT(file_cache.getUsedCacheSize(), 0);
        ASSERT_GT(file_cache.getFileSegmentsNum(), 0);

        size_t used_size_before_attempt = file_cache.getUsedCacheSize();
        /// data can't be evicted because it is still held by `some_data_holder`
        ASSERT_THROW({
            stream->write(generateBlock(2000));
            stream.finishWriting();
        }, DB::Exception);

        ASSERT_THROW(stream.finishWriting(), DB::Exception);

        ASSERT_EQ(file_cache.getUsedCacheSize(), used_size_before_attempt);
    }

    {
        size_t before_used_size = file_cache.getUsedCacheSize();
        auto write_buf_stream = std::make_unique<TemporaryDataBuffer>(tmp_data_scope);

        write_buf_stream->write("1234567890", 10);
        write_buf_stream->write("abcde", 5);
        auto read_buf = write_buf_stream->read();

        ASSERT_GT(file_cache.getUsedCacheSize(), before_used_size + 10);

        char buf[15];
        size_t read_size = read_buf->read(buf, 15);
        ASSERT_EQ(read_size, 15);
        ASSERT_EQ(std::string(buf, 15), "1234567890abcde");
        read_size = read_buf->read(buf, 15);
        ASSERT_EQ(read_size, 0);
    }

    {
        TemporaryBlockStreamHolder stream(std::make_shared<const Block>(generateBlock()), tmp_data_scope);

        ASSERT_GT(stream->write(generateBlock(100)), 0);

        some_data_holder = nullptr;

        stream->write(generateBlock(2000));

        stream.finishWriting();

        String file_path = stream.getHolder()->describeFilePath().substr(strlen("fscache://"));

        ASSERT_TRUE(fs::exists(file_path)) << "File " << file_path << " should exist";
        ASSERT_GT(fs::file_size(file_path), 100) << "File " << file_path << " should be larger than 100 bytes";

        ASSERT_EQ(readAllTemporaryData(*stream.getReadStream()), 2100);

        size_used_with_temporary_data = file_cache.getUsedCacheSize();
        segments_used_with_temporary_data = file_cache.getFileSegmentsNum();
        ASSERT_GT(size_used_with_temporary_data, 0);
        ASSERT_GT(segments_used_with_temporary_data, 0);
    }

    /// All temp data should be evicted after removing temporary files
    ASSERT_LE(file_cache.getUsedCacheSize(), size_used_with_temporary_data);
    ASSERT_LE(file_cache.getFileSegmentsNum(), segments_used_with_temporary_data);

    /// Some segments reserved by `some_data_holder` was eviced by temporary data
    ASSERT_LE(file_cache.getUsedCacheSize(), size_used_before_temporary_data);
    ASSERT_LE(file_cache.getFileSegmentsNum(), segments_used_before_temporary_data);
}
catch (...)
{
    LOG_ERROR(testLog(), "{}", getCurrentExceptionMessage(true));
    throw;
}

/// `getDownloadedContiguousOrEmpty` must inspect the actually downloaded segments even when
/// `enable_bypass_cache_with_threshold` is on and the requested range exceeds the threshold.
/// Otherwise getImpl() would return a synthetic DETACHED placeholder and the helper would
/// wrongly report present-but-large data (e.g. distributed-cache temporary data) as missing.
TEST_F(FileCacheTest, GetDownloadedContiguousIgnoresBypassThreshold)
try
{
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const size_t bypass_threshold = 100;
    const size_t chunk = bypass_threshold; /// each cached segment stays at/below the threshold

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 100_KiB;
    settings[FileCacheSetting::max_file_segment_size] = chunk;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;
    /// Any read larger than `bypass_threshold` bytes would normally bypass the cache.
    settings[FileCacheSetting::enable_bypass_cache_with_threshold] = true;
    settings[FileCacheSetting::bypass_cache_threshold] = bypass_threshold;

    DB::FileCache file_cache("bypass-temp", settings);
    file_cache.initialize();

    const auto & user = FileCache::getCommonOrigin();
    const auto key = FileCacheKey::fromPath("bypass_temp_key");

    /// Populate several contiguous segments, each no larger than the bypass threshold (a single
    /// write larger than the threshold would itself bypass the cache and never be stored). The
    /// downloaded data covers a range that, when read at once, exceeds the threshold.
    const size_t num_chunks = 3;
    const size_t downloaded_size = num_chunks * chunk;
    for (size_t i = 0; i < num_chunks; ++i)
    {
        auto holder = file_cache.getOrSet(key, i * chunk, chunk, downloaded_size, CreateFileSegmentSettings{}, 0, user);
        download(holder);
    }

    const auto & user_id = user.user_id;

    /// The whole downloaded range is larger than the threshold but must still be reported present.
    EXPECT_FALSE(file_cache.getDownloadedContiguousOrEmpty(key, 0, downloaded_size, user_id)->empty());
    /// A sub-range that also exceeds the threshold is present too.
    EXPECT_FALSE(file_cache.getDownloadedContiguousOrEmpty(key, 10, downloaded_size - 10, user_id)->empty());
    /// A range past the downloaded data is correctly reported as missing.
    EXPECT_TRUE(file_cache.getDownloadedContiguousOrEmpty(key, 0, downloaded_size + 1, user_id)->empty());
}
catch (...)
{
    LOG_ERROR(testLog(), "{}", getCurrentExceptionMessage(true));
    throw;
}

TEST_F(FileCacheTest, CachedReadBuffer)
{
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    /// To work with cache need query_id and query context.
    std::string query_id = "query_id";

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse>
</clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_file_segment_size] = 5;
    settings[FileCacheSetting::max_size] = 30;
    settings[FileCacheSetting::max_elements] = 10;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

    std::string file_path = fs::current_path() / "test";
    auto read_buffer_creator = [&]()
    {
        return createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt);
    };

    auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
    std::string s(30, '*');
    wb->write(s.data(), s.size());
    wb->next();
    wb->finalize();

    auto cache = std::make_shared<DB::FileCache>("8", settings);
    cache->initialize();

    auto key = DB::FileCacheKey::fromPath(file_path);
    const auto & user = FileCache::getCommonOrigin();

    {
        auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
            file_path, key, cache, user, read_buffer_creator,
            read_settings.filesystem_cache_settings, read_settings.remote_fs_settings.buffer_size, read_settings.local_fs_settings.buffer_size,
            "test", s.size(), false, false, std::nullopt, nullptr);

        WriteBufferFromOwnString result;
        copyData(*cached_buffer, result);
        ASSERT_EQ(result.str(), s);

        assertEqual(cache->dumpQueue(), { Range(0, 4), Range(5, 9), Range(10, 14), Range(15, 19), Range(20, 24), Range(25, 29) });
    }

    {
        auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
            file_path, key, cache, user, read_buffer_creator,
            read_settings.filesystem_cache_settings, /* remote_fs_buffer_size */ 10, /* local_fs_buffer_size */ 10,
            "test", s.size(), false, false, std::nullopt, nullptr);

        cached_buffer->next();
        assertEqual(cache->dumpQueue(), {Range(10, 14), Range(15, 19), Range(20, 24), Range(25, 29), Range(0, 4), Range(5, 9)});

        cached_buffer->position() = cached_buffer->buffer().end();
        cached_buffer->next();
        assertEqual(cache->dumpQueue(), {Range(15, 19), Range(20, 24), Range(25, 29), Range(0, 4), Range(5, 9), Range(10, 14)});
    }
}

namespace
{

/// Behaves like a remote reader (e.g. `ReadBufferFromS3`): supports right-bounded reads (so
/// `getRemoteReadBuffer` uses it as is, without wrapping) and reports the remote object's metadata.
/// The metadata reflects the current size of the underlying file, so a file smaller than the object
/// size the cached buffer was created with imitates a remote object that was overwritten with
/// shorter content between listing and reading.
class FakeRemoteReadBuffer : public BoundedReadBuffer
{
public:
    explicit FakeRemoteReadBuffer(std::unique_ptr<SeekableReadBuffer> impl_) : BoundedReadBuffer(std::move(impl_)) {}

    std::optional<RemoteFileMetadata> getRemoteFileMetadata() const override
    {
        return RemoteFileMetadata{.size = static_cast<size_t>(fs::file_size(getFileName())), .last_modification_time = 0};
    }
};

/// A source that fails on the first read, imitating e.g. a network error in a remote reader.
class FailingReadBuffer : public BoundedReadBuffer
{
public:
    explicit FailingReadBuffer(std::unique_ptr<SeekableReadBuffer> impl_) : BoundedReadBuffer(std::move(impl_)) {}

    bool nextImpl() override
    {
        throw std::runtime_error("Simulated source read failure");
    }
};

/// The query scope required for reading through the cache (a query id and a query context bound to
/// the current thread).
struct TestQueryScope
{
    explicit TestQueryScope(const std::string & query_id = "query_id")
    {
        ServerUUID::setRandomForUnitTests();

        Poco::XML::DOMParser dom_parser;
        std::string xml(R"CONFIG(<clickhouse>
</clickhouse>)CONFIG");
        Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
        getMutableContext().context->setConfig(config);

        query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId(query_id);
        chassert(&DB::CurrentThread::get() == &thread_status);
        query_scope = DB::QueryScope::create(query_context);
    }

    DB::ThreadStatus thread_status;
    ContextMutablePtr query_context;
    DB::QueryScope query_scope;
};

void setupCacheSettings(FileCacheSettings & settings, size_t max_file_segment_size)
{
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_file_segment_size] = max_file_segment_size;
    settings[FileCacheSetting::max_size] = 30;
    settings[FileCacheSetting::max_elements] = 10;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;
}

void writeSourceFile(const std::string & path, const std::string & data)
{
    WriteBufferFromFile wb(path, DBMS_DEFAULT_BUFFER_SIZE);
    wb.write(data.data(), data.size());
    wb.next();
    wb.finalize();
}

std::string makeSourceData(size_t size)
{
    std::string data(size, 0);
    for (size_t i = 0; i < size; ++i)
        data[i] = 'a' + i % 26;
    return data;
}

}

/// The following CachedReadBuffer* tests cover the failure paths of CachedOnDiskReadBufferFromFile
/// on which a downloader releases the file segment while unwinding. A segment's remote reader is
/// shared between the readers of the segment and synchronized only by the downloader election, so
/// before the failing downloader publishes `PARTIALLY_DOWNLOADED_NO_CONTINUATION` (which wakes up
/// the waiters and makes the reader extractable) it must withdraw the reader from the segment with
/// `resetRemoteFileReader` -- otherwise another reader could take it over and mutate it while the
/// unwinding frame still references it. `FileSegment::setDownloadFinishedWithoutContinuation`
/// chasserts that invariant at the publication point, so these tests fail deterministically in
/// debug and sanitizer builds if one of those withdrawals is removed; where the withdrawal is
/// observable in the segment's final state, the tests additionally assert it directly.

/// The remote object was truncated (overwritten with shorter content) between listing and reading,
/// detected in the middle of a plain read (`nextImpl`, the EOF branch of `readFromFileSegment`).
TEST_F(FileCacheTest, CachedReadBufferTruncatedObject)
{
    TestQueryScope query_scope;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

    /// The object was listed with size 30, but only 24 bytes exist by the time it is read.
    const std::string data = makeSourceData(24);
    const size_t expected_object_size = 30;
    std::string file_path = fs::current_path() / "test_truncated_object";
    writeSourceFile(file_path, data);

    auto read_buffer_creator = [&]() -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<FakeRemoteReadBuffer>(createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt));
    };

    DB::FileCacheSettings settings;
    setupCacheSettings(settings, /* max_file_segment_size */ 5);
    auto cache = std::make_shared<DB::FileCache>("truncated_object", settings);
    cache->initialize();

    auto key = DB::FileCacheKey::fromPath(file_path);
    const auto & user = FileCache::getCommonOrigin();

    /// Pin the segment in which the truncation will be detected (EOF falls into [20, 24]),
    /// to observe its state after the failure.
    auto probe = cache->getOrSet(key, 20, 5, expected_object_size, {}, 0, user);
    ASSERT_EQ(probe->size(), 1);

    auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
        file_path, key, cache, user, read_buffer_creator,
        read_settings.filesystem_cache_settings, DBMS_DEFAULT_BUFFER_SIZE, DBMS_DEFAULT_BUFFER_SIZE,
        "test", expected_object_size, false, false, std::nullopt, nullptr);

    WriteBufferFromOwnString result;
    try
    {
        copyData(*cached_buffer, result);
        FAIL() << "Expected CANNOT_READ_ALL_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    /// Everything before the truncation point was read and downloaded.
    EXPECT_EQ(result.str(), data);
    EXPECT_EQ(probe->front().getDownloadedSize(), 4);

    /// The failing downloader released the segment so that the readers waiting on it can take over,
    /// and withdrew the shared remote reader beforehand: a woken-up waiter must not be able to grab
    /// a reader that the unwinding frame still references.
    EXPECT_EQ(probe->front().state(), State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
    EXPECT_FALSE(probe->front().extractRemoteFileReader());
}

/// The truncation is detected while predownloading: the second reader seeks into a partially
/// downloaded segment, becomes its downloader, has to predownload the gap [current write offset,
/// seek offset) first, and hits EOF inside it (the seek offset lies beyond the truncated object).
TEST_F(FileCacheTest, CachedReadBufferTruncatedObjectPredownload)
{
    TestQueryScope query_scope;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

    /// The object was listed with size 10, but only 4 bytes exist by the time it is read.
    const std::string data = makeSourceData(4);
    const size_t expected_object_size = 10;
    std::string file_path = fs::current_path() / "test_truncated_object_predownload";
    writeSourceFile(file_path, data);

    auto read_buffer_creator = [&]() -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<FakeRemoteReadBuffer>(createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt));
    };

    DB::FileCacheSettings settings;
    setupCacheSettings(settings, /* max_file_segment_size */ 10);
    auto cache = std::make_shared<DB::FileCache>("truncated_object_predownload", settings);
    cache->initialize();

    auto key = DB::FileCacheKey::fromPath(file_path);
    const auto & user = FileCache::getCommonOrigin();

    /// Pin the single segment [0, 9] to observe its state after the failure.
    auto probe = cache->getOrSet(key, 0, expected_object_size, expected_object_size, {}, 0, user);
    ASSERT_EQ(probe->size(), 1);

    /// The first reader downloads bytes [0, 2) and stops mid-segment, leaving the segment
    /// PARTIALLY_DOWNLOADED with the shared remote reader registered in it for reuse.
    auto first_reader = std::make_shared<CachedOnDiskReadBufferFromFile>(
        file_path, key, cache, user, read_buffer_creator,
        read_settings.filesystem_cache_settings, /* remote_fs_buffer_size */ 2, /* local_fs_buffer_size */ 2,
        "test", expected_object_size, false, false, std::nullopt, nullptr);

    ASSERT_TRUE(first_reader->next());
    EXPECT_EQ(std::string(first_reader->buffer().begin(), first_reader->buffer().end()), data.substr(0, 2));
    ASSERT_EQ(probe->front().state(), State::PARTIALLY_DOWNLOADED);
    ASSERT_EQ(probe->front().getCurrentWriteOffset(), 2);

    /// The second reader seeks to offset 5 and becomes the downloader, reusing the reader registered
    /// by the first one. It predownloads bytes [2, 4) and then hits EOF with one more byte to
    /// predownload, because the requested offset lies beyond the truncated object.
    auto second_reader = std::make_shared<CachedOnDiskReadBufferFromFile>(
        file_path, key, cache, user, read_buffer_creator,
        read_settings.filesystem_cache_settings, /* remote_fs_buffer_size */ 8, /* local_fs_buffer_size */ 8,
        "test", expected_object_size, /* allow_seeks_after_first_read */ true, false, std::nullopt, nullptr);

    second_reader->seek(5, SEEK_SET);
    try
    {
        second_reader->next();
        FAIL() << "Expected CANNOT_READ_ALL_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    /// The bytes that did exist were predownloaded before the failure was detected.
    EXPECT_EQ(probe->front().getDownloadedSize(), 4);

    /// As in the plain-read test: released for waiting readers, with the shared reader withdrawn.
    EXPECT_EQ(probe->front().state(), State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
    EXPECT_FALSE(probe->front().extractRemoteFileReader());
}

/// The truncated object is detected under `readBigAt`, which has its own segment-completion cleanup
/// (its state does not outlive the call, unlike the `nextImpl` path).
TEST_F(FileCacheTest, CachedReadBufferTruncatedObjectReadBigAt)
{
    TestQueryScope query_scope;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

    /// The object was listed with size 10, but only 4 bytes exist by the time it is read.
    const std::string data = makeSourceData(4);
    const size_t expected_object_size = 10;
    std::string file_path = fs::current_path() / "test_truncated_object_read_big_at";
    writeSourceFile(file_path, data);

    auto read_buffer_creator = [&]() -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<FakeRemoteReadBuffer>(createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt));
    };

    DB::FileCacheSettings settings;
    setupCacheSettings(settings, /* max_file_segment_size */ 10);
    auto cache = std::make_shared<DB::FileCache>("truncated_object_read_big_at", settings);
    cache->initialize();

    auto key = DB::FileCacheKey::fromPath(file_path);
    const auto & user = FileCache::getCommonOrigin();

    /// Pin the single segment [0, 9] to observe its state after the failure.
    auto probe = cache->getOrSet(key, 0, expected_object_size, expected_object_size, {}, 0, user);
    ASSERT_EQ(probe->size(), 1);

    auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
        file_path, key, cache, user, read_buffer_creator,
        read_settings.filesystem_cache_settings, DBMS_DEFAULT_BUFFER_SIZE, DBMS_DEFAULT_BUFFER_SIZE,
        "test", expected_object_size, false, false, std::nullopt, nullptr);

    std::vector<char> to(expected_object_size, 0);
    try
    {
        cached_buffer->readBigAt(to.data(), to.size(), 0, {});
        FAIL() << "Expected CANNOT_READ_ALL_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    /// Everything before the truncation point was read and downloaded.
    EXPECT_EQ(std::string(to.data(), data.size()), data);
    EXPECT_EQ(probe->front().getDownloadedSize(), 4);

    /// As in the plain-read test: released for waiting readers, with the shared reader withdrawn.
    EXPECT_EQ(probe->front().state(), State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
    EXPECT_FALSE(probe->front().extractRemoteFileReader());
}

/// The source reader of a `readBigAt` call fails mid-read. The unwinding `readBigAt` releases
/// downloader ownership in its cleanup, and must withdraw the shared remote reader from the segment
/// while doing so: the reader still borrows the caller's `to` buffer (the normal un-borrowing is
/// skipped on the exception path precisely because the reader may already be shared), so a new
/// downloader must get a fresh reader instead of one pointing into the unwound caller's memory.
TEST_F(FileCacheTest, CachedReadBufferReadBigAtSourceFailure)
{
    TestQueryScope query_scope;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

    const std::string data = makeSourceData(10);
    std::string file_path = fs::current_path() / "test_read_big_at_source_failure";
    writeSourceFile(file_path, data);

    auto failing_read_buffer_creator = [&]() -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<FailingReadBuffer>(createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt));
    };

    DB::FileCacheSettings settings;
    setupCacheSettings(settings, /* max_file_segment_size */ 10);
    auto cache = std::make_shared<DB::FileCache>("read_big_at_source_failure", settings);
    cache->initialize();

    auto key = DB::FileCacheKey::fromPath(file_path);
    const auto & user = FileCache::getCommonOrigin();

    /// Pin the single segment [0, 9] to observe its state after the failure.
    auto probe = cache->getOrSet(key, 0, data.size(), data.size(), {}, 0, user);
    ASSERT_EQ(probe->size(), 1);

    auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
        file_path, key, cache, user, failing_read_buffer_creator,
        read_settings.filesystem_cache_settings, DBMS_DEFAULT_BUFFER_SIZE, DBMS_DEFAULT_BUFFER_SIZE,
        "test", data.size(), false, false, std::nullopt, nullptr);

    std::vector<char> to(data.size(), 0);
    EXPECT_THROW(cached_buffer->readBigAt(to.data(), to.size(), 0, {}), std::runtime_error);

    /// Become the next downloader of the segment, as a reader waiting on it would.
    auto & file_segment = probe->front();
    ASSERT_EQ(file_segment.getOrSetDownloader(), FileSegment::getCallerId());
    EXPECT_FALSE(file_segment.getRemoteFileReader());
    file_segment.completePartAndResetDownloader();

    /// The segment stays usable: a reader with a healthy source reads it end to end.
    auto read_buffer_creator = [&]() -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<FakeRemoteReadBuffer>(createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt));
    };
    auto recovered_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
        file_path, key, cache, user, read_buffer_creator,
        read_settings.filesystem_cache_settings, DBMS_DEFAULT_BUFFER_SIZE, DBMS_DEFAULT_BUFFER_SIZE,
        "test", data.size(), false, false, std::nullopt, nullptr);
    WriteBufferFromOwnString result;
    copyData(*recovered_buffer, result);
    EXPECT_EQ(result.str(), data);
}

/// The same source failure as above, through the `nextImpl` path.
TEST_F(FileCacheTest, CachedReadBufferSourceFailure)
{
    TestQueryScope query_scope;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

    const std::string data = makeSourceData(10);
    std::string file_path = fs::current_path() / "test_source_failure";
    writeSourceFile(file_path, data);

    auto failing_read_buffer_creator = [&]() -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<FailingReadBuffer>(createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt));
    };

    DB::FileCacheSettings settings;
    setupCacheSettings(settings, /* max_file_segment_size */ 10);
    auto cache = std::make_shared<DB::FileCache>("source_failure", settings);
    cache->initialize();

    auto key = DB::FileCacheKey::fromPath(file_path);
    const auto & user = FileCache::getCommonOrigin();

    /// Pin the single segment [0, 9] to observe its state after the failure.
    auto probe = cache->getOrSet(key, 0, data.size(), data.size(), {}, 0, user);
    ASSERT_EQ(probe->size(), 1);

    auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
        file_path, key, cache, user, failing_read_buffer_creator,
        read_settings.filesystem_cache_settings, DBMS_DEFAULT_BUFFER_SIZE, DBMS_DEFAULT_BUFFER_SIZE,
        "test", data.size(), false, false, std::nullopt, nullptr);

    EXPECT_THROW(cached_buffer->next(), std::runtime_error);

    /// Become the next downloader of the segment, as a reader waiting on it would; the failed
    /// downloader must have withdrawn the shared remote reader on its unwind path.
    auto & file_segment = probe->front();
    ASSERT_EQ(file_segment.getOrSetDownloader(), FileSegment::getCallerId());
    EXPECT_FALSE(file_segment.getRemoteFileReader());
    file_segment.completePartAndResetDownloader();

    /// The segment stays usable: a reader with a healthy source reads it end to end.
    auto read_buffer_creator = [&]() -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<FakeRemoteReadBuffer>(createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt));
    };
    auto recovered_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
        file_path, key, cache, user, read_buffer_creator,
        read_settings.filesystem_cache_settings, DBMS_DEFAULT_BUFFER_SIZE, DBMS_DEFAULT_BUFFER_SIZE,
        "test", data.size(), false, false, std::nullopt, nullptr);
    WriteBufferFromOwnString result;
    copyData(*recovered_buffer, result);
    EXPECT_EQ(result.str(), data);
}

/// A cached read can be issued from a destructor while an unrelated exception is being unwound --
/// e.g. `MergeTreeData::Transaction` rollback reading version metadata from an `s3` disk. The read
/// must succeed: the exception detection in the cleanups of `nextImplStep` and `readBigAt` compares
/// `std::uncaught_exceptions` against the count captured on entry, and a plain `> 0` check would
/// treat every successfully completed step as failed and drop the read state mid-read (an earlier
/// revision of the ownership-handoff fix did exactly that and failed with a null dereference).
TEST_F(FileCacheTest, CachedReadBufferReadDuringExceptionUnwinding)
{
    TestQueryScope query_scope;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

    const std::string data = makeSourceData(30);
    std::string file_path = fs::current_path() / "test_read_during_unwinding";
    writeSourceFile(file_path, data);

    auto read_buffer_creator = [&]() -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<FakeRemoteReadBuffer>(createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt));
    };

    DB::FileCacheSettings settings;
    setupCacheSettings(settings, /* max_file_segment_size */ 5);
    auto cache = std::make_shared<DB::FileCache>("read_during_unwinding", settings);
    cache->initialize();

    auto key = DB::FileCacheKey::fromPath(file_path);
    const auto & user = FileCache::getCommonOrigin();

    auto make_cached_buffer = [&]()
    {
        return std::make_shared<CachedOnDiskReadBufferFromFile>(
            file_path, key, cache, user, read_buffer_creator,
            read_settings.filesystem_cache_settings, DBMS_DEFAULT_BUFFER_SIZE, DBMS_DEFAULT_BUFFER_SIZE,
            "test", data.size(), false, false, std::nullopt, nullptr);
    };

    std::string remote_read_result;
    std::string cached_read_result;
    std::string read_big_at_result;
    std::string error;
    bool outer_exception_caught = false;

    try
    {
        /// The guard's destructor runs the reads while the exception below is being unwound.
        SCOPE_EXIT({
            try
            {
                {
                    /// The data is not cached yet: REMOTE_FS_READ_AND_PUT_IN_CACHE.
                    auto cached_buffer = make_cached_buffer();
                    WriteBufferFromOwnString result;
                    copyData(*cached_buffer, result);
                    remote_read_result = result.str();
                }
                {
                    /// Now the data is cached: CACHED.
                    auto cached_buffer = make_cached_buffer();
                    WriteBufferFromOwnString result;
                    copyData(*cached_buffer, result);
                    cached_read_result = result.str();
                }
                {
                    std::vector<char> to(data.size(), 0);
                    const size_t size = make_cached_buffer()->readBigAt(to.data(), to.size(), 0, {});
                    read_big_at_result = std::string(to.data(), size);
                }
            }
            catch (...)
            {
                error = getCurrentExceptionMessage(true);
            }
        });
        throw std::runtime_error("The exception being unwound while the cached reads run");
    }
    catch (const std::runtime_error &)
    {
        outer_exception_caught = true;
    }

    EXPECT_TRUE(outer_exception_caught);
    EXPECT_EQ(error, "");
    EXPECT_EQ(remote_read_result, data);
    EXPECT_EQ(cached_read_result, data);
    EXPECT_EQ(read_big_at_result, data);
}

TEST_F(FileCacheTest, TemporaryDataReadBufferSize)
{
    ServerUUID::setRandomForUnitTests();
    /// Temporary data stored in cache
    {
        DB::FileCacheSettings settings;
        settings[FileCacheSetting::max_size] = 10_KiB;
        settings[FileCacheSetting::max_file_segment_size] = 1_KiB;
        settings[FileCacheSetting::path] = cache_base_path;
        settings[FileCacheSetting::load_metadata_asynchronously] = false;
        settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

        DB::FileCache file_cache("cache", settings);
        file_cache.initialize();

        auto tmp_data_scope = std::make_shared<TemporaryDataOnDiskScope>(TemporaryDataOnDiskSettings{}, &file_cache);

        auto block = generateBlock(/*size=*/3);
        TemporaryBlockStreamHolder stream(std::make_shared<const Block>(block), tmp_data_scope);

        stream->write(block);
        auto stat = stream.finishWriting();

        /// We allocate buffer of size min(stat.compressed_size, DBMS_DEFAULT_BUFFER_SIZE)
        /// We do care about buffer size because realistic external group by could generate 10^5 temporary files
        ASSERT_EQ(stat.compressed_size, 64);

        auto reader = stream.getReadStream();
        auto * read_buf = reader.getHolder();
        const auto & internal_buffer = static_cast<TemporaryDataReadBuffer *>(read_buf)->compressed_buf.getHolder()->internalBuffer();
        ASSERT_EQ(internal_buffer.size(), 64);
    }

    /// Temporary data stored on disk
    {
        DiskPtr disk;
        SCOPE_EXIT_SAFE(destroyDisk(disk));

        disk = createDisk("temporary_data_read_buffer_size_test_dir");
        VolumePtr volume = std::make_shared<SingleDiskVolume>("volume", disk);

        auto tmp_data_scope = std::make_shared<TemporaryDataOnDiskScope>(TemporaryDataOnDiskSettings{}, volume);

        auto block = generateBlock(/*size=*/3);
        TemporaryBlockStreamHolder stream(std::make_shared<const Block>(block), tmp_data_scope);
        stream->write(block);
        auto stat = stream.finishWriting();

        ASSERT_EQ(stat.compressed_size, 64);
    }
}

TEST_F(FileCacheTest, SLRUPolicy)
try
{
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;
    std::string query_id = "query_id"; /// To work with cache need query_id and query context.

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse>
</clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 40;
    settings[FileCacheSetting::max_elements] = 6;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;

    settings[FileCacheSetting::cache_policy] = FileCachePolicy::SLRU;
    settings[FileCacheSetting::slru_size_ratio] = 0.5;

    const size_t file_size = -1; // the value doesn't really matter because boundary_alignment == 1.
    size_t file_cache_name = 0;
    const auto & user = FileCache::getCommonOrigin();

    {
        auto cache = DB::FileCache(std::to_string(++file_cache_name), settings);
        cache.initialize();
        auto key = FileCacheKey::fromPath("key1");

        auto add_range = [&](size_t offset, size_t size)
        {
            LOG_DEBUG(testLog(), "Add [{}, {}]", offset, offset + size - 1);

            auto holder = cache.getOrSet(key, offset, size, file_size, {}, 0, user);
            assertEqual(holder, { Range(offset, offset + size - 1) }, { State::EMPTY });
            download(*holder->begin());
            assertEqual(holder, { Range(offset, offset + size - 1) }, { State::DOWNLOADED });
        };

        auto check_covering_range = [&](size_t offset, size_t size, Ranges covering_ranges)
        {
            auto holder = cache.getOrSet(key, offset, size, file_size, {}, 0, user);
            std::vector<State> states(covering_ranges.size(), State::DOWNLOADED);
            assertEqual(holder, covering_ranges, states);
            increasePriority(holder);
        };

        add_range(0, 10);
        add_range(10, 5);

        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(0, 9), Range(10, 14) });
        assertEqual(cache.dumpQueue(), { Range(0, 9), Range(10, 14) });

        ASSERT_EQ(cache.getFileSegmentsNum(), 2);
        ASSERT_EQ(cache.getUsedCacheSize(), 15);

        assertProbationary(cache.dumpQueue(), { Range(0, 9), Range(10, 14) });
        assertProtected(cache.dumpQueue(), Ranges{});

        check_covering_range(9, 1, { Range(0, 9) });
        assertEqual(cache.dumpQueue(), { Range(10, 14), Range(0, 9) });

        check_covering_range(10, 1, { Range(10, 14) });
        assertEqual(cache.dumpQueue(), { Range(0, 9), Range(10, 14) });

        assertProbationary(cache.dumpQueue(), Ranges{});
        assertProtected(cache.dumpQueue(), { Range(0, 9), Range(10, 14) });

        add_range(17, 4);
        assertEqual(cache.dumpQueue(), { Range(17, 20), Range(0, 9), Range(10, 14) });

        add_range(24, 3);
        assertEqual(cache.dumpQueue(), { Range(17, 20), Range(24, 26), Range(0, 9), Range(10, 14) });

        add_range(27, 1);
        assertEqual(cache.dumpQueue(), { Range(17, 20), Range(24, 26), Range(27, 27), Range(0, 9), Range(10, 14) });

        assertProbationary(cache.dumpQueue(), { Range(17, 20), Range(24, 26), Range(27, 27) });
        assertProtected(cache.dumpQueue(), { Range(0, 9), Range(10, 14) });

        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(0, 9), Range(10, 14), Range(17, 20), Range(24, 26), Range(27, 27) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 5);
        ASSERT_EQ(cache.getUsedCacheSize(), 23);

        add_range(28, 3);
        assertEqual(cache.dumpQueue(), { Range(24, 26), Range(27, 27), Range(28, 30), Range(0, 9), Range(10, 14) });

        assertProbationary(cache.dumpQueue(), { Range(24, 26), Range(27, 27), Range(28, 30) });
        assertProtected(cache.dumpQueue(), { Range(0, 9), Range(10, 14) });

        check_covering_range(4, 1, { Range(0, 9) });

        assertProbationary(cache.dumpQueue(), { Range(24, 26), Range(27, 27), Range(28, 30) });
        assertProtected(cache.dumpQueue(), { Range(10, 14), Range(0, 9) });

        check_covering_range(27, 3, { Range(27, 27), Range(28, 30) });

        assertProbationary(cache.dumpQueue(), { Range(24, 26), Range(10, 14) });
        assertProtected(cache.dumpQueue(), { Range(0, 9), Range(27, 27), Range(28, 30) });

        assertEqual(cache.getFileSegmentInfos(key, user.user_id), { Range(0, 9), Range(10, 14), Range(24, 26), Range(27, 27), Range(28, 30) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 5);
        ASSERT_EQ(cache.getUsedCacheSize(), 22);
    }

    {
        ReadSettings read_settings;
        read_settings.enable_filesystem_cache = true;
        read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

        auto write_file = [](const std::string & filename, const std::string & s)
        {
            std::string file_path = fs::current_path() / filename;
            auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
            wb->write(s.data(), s.size());
            wb->next();
            wb->finalize();
            return file_path;
        };

        DB::FileCacheSettings settings2;
        settings2[FileCacheSetting::path] = cache_base_path2;
        settings2[FileCacheSetting::max_file_segment_size] = 5;
        settings2[FileCacheSetting::max_size] = 30;
        settings2[FileCacheSetting::max_elements] = 6;
        settings2[FileCacheSetting::boundary_alignment] = 1;
        settings2[FileCacheSetting::slru_size_ratio] = 0.5;
        settings2[FileCacheSetting::load_metadata_asynchronously] = false;
        settings2[FileCacheSetting::cache_policy] = FileCachePolicy::SLRU;

        auto cache = std::make_shared<DB::FileCache>("slru_2", settings2);
        cache->initialize();

        auto read_and_check = [&](const std::string & file, const FileCacheKey & key, const std::string & expect_result)
        {
            auto read_buffer_creator = [&]()
            {
                return createReadBufferFromFileBase(file, read_settings, std::nullopt, std::nullopt);
            };

            auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
                file, key, cache, user, read_buffer_creator,
                read_settings.filesystem_cache_settings, read_settings.remote_fs_settings.buffer_size, read_settings.local_fs_settings.buffer_size,
                "test", expect_result.size(), false, false, std::nullopt, nullptr);

            WriteBufferFromOwnString result;
            copyData(*cached_buffer, result);
            ASSERT_EQ(result.str(), expect_result);
        };

        std::string data1(15, '*');
        auto file1 = write_file("test1", data1);
        auto key1 = DB::FileCacheKey::fromPath(file1);

        read_and_check(file1, key1, data1);

        assertEqual(cache->dumpQueue(), { Range(0, 4), Range(5, 9), Range(10, 14) });
        assertProbationary(cache->dumpQueue(), { Range(0, 4), Range(5, 9), Range(10, 14) });
        assertProtected(cache->dumpQueue(), Ranges{});

        read_and_check(file1, key1, data1);

        assertEqual(cache->dumpQueue(), { Range(0, 4), Range(5, 9), Range(10, 14) });
        assertProbationary(cache->dumpQueue(), Ranges{});
        assertProtected(cache->dumpQueue(), { Range(0, 4), Range(5, 9), Range(10, 14) });

        std::string data2(10, '*');
        auto file2 = write_file("test2", data2);
        auto key2 = DB::FileCacheKey::fromPath(file2);

        read_and_check(file2, key2, data2);

        auto dump = cache->dumpQueue();
        assertEqual(dump, { Range(0, 4), Range(5, 9), Range(0, 4), Range(5, 9), Range(10, 14) });

        const auto & infos = dynamic_cast<const LRUFileCachePriority::IPriorityDump *>(dump.get())->infos;
        ASSERT_EQ(infos[0].key, key2);
        ASSERT_EQ(infos[1].key, key2);
        ASSERT_EQ(infos[2].key, key1);
        ASSERT_EQ(infos[3].key, key1);
        ASSERT_EQ(infos[4].key, key1);

        assertProbationary(cache->dumpQueue(), { Range(0, 4), Range(5, 9) });
        assertProtected(cache->dumpQueue(), { Range(0, 4), Range(5, 9), Range(10, 14) });

        read_and_check(file2, key2, data2);

        dump = cache->dumpQueue();
        assertEqual(dump, { Range(0, 4), Range(5, 9), Range(10, 14), Range(0, 4), Range(5, 9)  });

        const auto & infos2 = dynamic_cast<const LRUFileCachePriority::IPriorityDump *>(dump.get())->infos;
        ASSERT_EQ(infos2[0].key, key1);
        ASSERT_EQ(infos2[1].key, key1);
        ASSERT_EQ(infos2[2].key, key1);
        ASSERT_EQ(infos2[3].key, key2);
        ASSERT_EQ(infos2[4].key, key2);

        assertProbationary(cache->dumpQueue(), { Range(0, 4), Range(5, 9) });
        assertProtected(cache->dumpQueue(), { Range(10, 14), Range(0, 4), Range(5, 9)  });
    }
}
catch (...)
{
    LOG_ERROR(testLog(), "{}", getCurrentExceptionMessage(true));
    throw;
}

TEST_F(FileCacheTest, SLRUDynamicResizeCorrectEviction)
{
    /// Test that SLRU dynamic resize correctly evicts from both sub-queues
    /// after the per-queue stat fix.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

    auto write_file = [](const std::string & filename, const std::string & s)
    {
        std::string file_path = fs::current_path() / filename;
        auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
        wb->write(s.data(), s.size());
        wb->next();
        wb->finalize();
        return file_path;
    };

    /// Create SLRU cache: max_size=30, max_elements=6, ratio=0.5
    /// So protected = 15 bytes / 3 elements, probationary = 15 bytes / 3 elements.
    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path2;
    settings[FileCacheSetting::max_file_segment_size] = 5;
    settings[FileCacheSetting::max_size] = 30;
    settings[FileCacheSetting::max_elements] = 6;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::slru_size_ratio] = 0.5;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::SLRU;
    settings[FileCacheSetting::allow_dynamic_cache_resize] = true;

    auto cache = std::make_shared<DB::FileCache>("slru_resize", settings);
    cache->initialize();

    const auto & user = FileCache::getCommonOrigin();

    auto read_and_check = [&](const std::string & file, const FileCacheKey & key, const std::string & expect_result)
    {
        auto read_buffer_creator = [&]()
        {
            return createReadBufferFromFileBase(file, read_settings, std::nullopt, std::nullopt);
        };
        auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
            file, key, cache, user, read_buffer_creator,
            read_settings.filesystem_cache_settings, read_settings.remote_fs_settings.buffer_size, read_settings.local_fs_settings.buffer_size,
            "test", expect_result.size(), false, false, std::nullopt, nullptr);
        WriteBufferFromOwnString result;
        copyData(*cached_buffer, result);
        ASSERT_EQ(result.str(), expect_result);
    };

    /// Read file1 twice -> 15 bytes in protected (3 segs x 5)
    std::string data1(15, '*');
    auto file1 = write_file("test_resize1", data1);
    auto key1 = DB::FileCacheKey::fromPath(file1);
    read_and_check(file1, key1, data1);
    read_and_check(file1, key1, data1);

    assertProtected(cache->dumpQueue(), { Range(0, 4), Range(5, 9), Range(10, 14) });

    /// Read file2 once -> 10 bytes in probationary (2 segs x 5)
    std::string data2(10, '+');
    auto file2 = write_file("test_resize2", data2);
    auto key2 = DB::FileCacheKey::fromPath(file2);
    read_and_check(file2, key2, data2);

    assertProbationary(cache->dumpQueue(), { Range(0, 4), Range(5, 9) });
    ASSERT_EQ(cache->getUsedCacheSize(), 25);
    ASSERT_EQ(cache->getFileSegmentsNum(), 5);

    /// Resize to max_size=8, max_elements=6.
    /// Protected limit = 4, probationary limit = 4.
    /// Both queues need eviction. Without the fix, the protected pass
    /// would short-circuit and modifySizeLimits would throw LOGICAL_ERROR.
    DB::FileCacheSettings new_settings = settings;
    new_settings[FileCacheSetting::max_size] = 8;
    DB::FileCacheSettings actual_settings = settings;

    /// Must not throw -- this is the core regression test for the bug.
    ASSERT_NO_THROW(cache->applySettingsIfPossible(new_settings, actual_settings));

    /// Verify limits were applied.
    ASSERT_EQ(actual_settings[FileCacheSetting::max_size].value, 8);
    ASSERT_EQ(actual_settings[FileCacheSetting::max_elements].value, 6);

    /// Verify cache usage is within new limits.
    ASSERT_LE(cache->getUsedCacheSize(), 8);
    ASSERT_LE(cache->getFileSegmentsNum(), 6);
}

TEST_F(FileCacheTest, DynamicResizeConcurrentWithReservations)
{
    /// Stress dynamic resize against concurrent reservations: workers keep adding new cache
    /// elements while a resizer keeps shrinking/growing the cache via `applySettingsIfPossible`.
    /// Assert no thread threw, `dumpQueue` does not throw, and a final shrink enforces the new limit.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const std::string cache_path = caches_dir / "cache_dynamic_resize_concurrent" / "";
    if (fs::exists(cache_path))
        fs::remove_all(cache_path);
    fs::create_directories(cache_path);

    constexpr size_t max_size_large = 4096;
    constexpr size_t max_size_small = 512;
    constexpr size_t max_elements = 256;

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_path;
    settings[FileCacheSetting::max_size] = max_size_large;
    settings[FileCacheSetting::max_elements] = max_elements;
    settings[FileCacheSetting::boundary_alignment] = 1;
    /// SLRU exercises both sub-queues during resize eviction.
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::SLRU;
    settings[FileCacheSetting::slru_size_ratio] = 0.5;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::allow_dynamic_cache_resize] = true;

    auto cache = std::make_shared<DB::FileCache>("dynamic_resize_concurrent", settings);
    cache->initialize();

    const auto & user = FileCache::getCommonOrigin();
    const size_t file_size = -1;

    std::atomic<size_t> exceptions_caught{0};
    std::atomic<bool> stop_resizer{false};
    std::mutex first_exception_mutex;
    std::string first_exception_message;
    auto record_exception = [&]
    {
        exceptions_caught.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard lock(first_exception_mutex);
        if (first_exception_message.empty())
            first_exception_message = getCurrentExceptionMessage(true);
    };

    constexpr size_t num_reservers = 8;
    constexpr size_t iterations_per_reserver = 2000;

    /// Each reserver uses its own key, so the contention is on the global cache
    /// budget and eviction (racing the resizer), not on downloading the same segment.
    auto reserver = [&](size_t thread_index, UInt64 seed)
    {
        DB::ThreadStatus reserver_thread_status;
        pcg64 local_rng(seed);
        const auto key = DB::FileCacheKey::fromPath("dyn_resize_key_" + std::to_string(thread_index));
        for (size_t iter = 0; iter < iterations_per_reserver; ++iter)
        {
            try
            {
                const size_t offset = (local_rng() % 256) * 16;
                const size_t size = 1 + (local_rng() % 32);
                auto holder = cache->getOrSet(key, offset, size, file_size, {}, 0, user);
                for (auto & segment : *holder)
                {
                    if (segment->state() == State::EMPTY
                        && segment->getOrSetDownloader() == FileSegment::getCallerId())
                    {
                        std::string failure_reason;
                        if (segment->reserve(segment->range().size(), 1000, failure_reason))
                            download(cache_path, *segment);
                        FileSegment::complete(
                            FileSegmentPtr(segment),
                            /*allow_background_download=*/false,
                            /*force_shrink_to_downloaded_size=*/false);
                    }
                }
            }
            catch (...)
            {
                /// Ok: the exception is saved and asserted on after all threads join.
                record_exception();
            }
        }
    };

    /// `resizer_actual_settings` is the authoritative copy: `applySettingsIfPossible` throws unless
    /// the current limits passed in match the cache's real limits, and only the resizer changes them.
    DB::FileCacheSettings resizer_actual_settings = settings;
    auto resizer = [&]
    {
        DB::ThreadStatus resizer_thread_status;
        size_t toggle = 0;
        while (!stop_resizer.load(std::memory_order_relaxed))
        {
            try
            {
                DB::FileCacheSettings new_settings = resizer_actual_settings;
                new_settings[FileCacheSetting::max_size] = (toggle++ % 2 == 0) ? max_size_small : max_size_large;
                cache->applySettingsIfPossible(new_settings, resizer_actual_settings);
            }
            catch (...)
            {
                /// Ok: the exception is saved and asserted on after all threads join.
                record_exception();
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_reservers);
    for (size_t i = 0; i < num_reservers; ++i)
        threads.emplace_back(reserver, i, rng());
    std::thread resizer_thread(resizer);

    for (auto & t : threads)
        t.join();
    stop_resizer.store(true, std::memory_order_relaxed);
    resizer_thread.join();

    /// A final deterministic shrink (no concurrency) must bring usage within the smaller limit -
    /// resize must still evict correctly after the storm. Reuses `resizer_actual_settings`.
    {
        DB::FileCacheSettings final_settings = resizer_actual_settings;
        final_settings[FileCacheSetting::max_size] = max_size_small;
        ASSERT_NO_THROW(cache->applySettingsIfPossible(final_settings, resizer_actual_settings));
        ASSERT_EQ(resizer_actual_settings[FileCacheSetting::max_size].value, max_size_small);
    }

    ASSERT_LE(cache->getUsedCacheSize(), static_cast<size_t>(max_size_small));
    ASSERT_LE(cache->getFileSegmentsNum(), static_cast<size_t>(max_elements));
    ASSERT_NO_THROW(cache->dumpQueue());

    if (exceptions_caught.load() != 0u)
        std::cerr << "First exception caught (of " << exceptions_caught.load() << "): "
                  << first_exception_message << std::endl;
    ASSERT_EQ(exceptions_caught.load(), 0u);

    /// Destroy the cache before removing the directory: `~FileCache` runs `assertCacheCorrectness`
    /// (debug/sanitizer builds), which stats the cached files.
    cache.reset();
    if (fs::exists(cache_path))
        fs::remove_all(cache_path);
}

TEST_F(FileCacheTest, SLRUFreeSpaceKeepingProtectedOnly)
{
    /// Regression test for https://github.com/ClickHouse/ClickHouse/issues/104307
    ///
    /// `SLRUFileCachePriority::collectEvictionInfo` is invoked from
    /// `FileCache::freeSpaceRatioKeepingThreadFunc` (driven by the
    /// `keep_free_space_size(elements)_ratio` features) with `is_total_space_cleanup=true`.
    /// With a high enough free-space target the function used to `chassert` that we
    /// evict at least one element/byte from the probationary queue. This is wrong when
    /// entries have all been promoted to the protected queue and the probationary queue
    /// is empty: the function must still be able to evict from the protected queue.
    /// Without the fix, the assertion aborts the server in debug/sanitizer builds and
    /// throws a `LOGICAL_ERROR` in release.
    ///
    /// We exercise `SLRUFileCachePriority::collectEvictionInfo` directly rather than
    /// going through `FileCache::freeSpaceRatioKeepingThreadFunc` to avoid the timing
    /// race with the asynchronous background eviction task that `FileCache` schedules
    /// when `keep_free_space_*_ratio` is set: that task evicts entries between the
    /// populate and assert steps, especially on slow builds (e.g. coverage), which
    /// makes the higher-level test inherently flaky. The unit-level test below
    /// reproduces the exact bug condition deterministically and on every build flavor.

    ServerUUID::setRandomForUnitTests();

    /// Match the parameters of the original repro: 30 bytes / 6 elements with
    /// slru_size_ratio = 0.5 yields protected = 15 bytes / 3 elements and probationary
    /// = 15 bytes / 3 elements.
    const size_t max_size = 30;
    const size_t max_elements = 6;
    const double slru_size_ratio = 0.5;
    SLRUFileCachePriority priority(IFileCachePriority::QueueType::Main, max_size, max_elements, slru_size_ratio, "test_104307");

    const std::string cache_path = caches_dir / "test_slru_104307";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path,
                                 /* background_download_queue_size_limit */0,
                                 /* background_download_threads */0,
                                 /* write_cache_per_user_directory */false);

    const auto key = DB::FileCacheKey::fromPath("104307_protected_only_key");
    const auto & origin = FileCache::getCommonOrigin();
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(origin), &cache_metadata);

    CacheStateGuard state_guard;

    /// Add 3 entries of 5 bytes each (15 bytes total) directly to the protected queue,
    /// leaving probationary empty. This is the precondition that used to trigger the
    /// chassert in `collectEvictionInfo`.
    {
        auto state_lock = state_guard.lock();
        auto lock = priority.getPriorityGuardForTests().writeLock();
        priority.addForRestore(key_metadata, /* offset */0, /* size */5,
                               IFileCachePriority::QueueEntryType::SLRU_Protected,
                               lock, &state_lock);
        priority.addForRestore(key_metadata, /* offset */5, /* size */5,
                               IFileCachePriority::QueueEntryType::SLRU_Protected,
                               lock, &state_lock);
        priority.addForRestore(key_metadata, /* offset */10, /* size */5,
                               IFileCachePriority::QueueEntryType::SLRU_Protected,
                               lock, &state_lock);
    }

    /// Verify the precondition: 3 entries / 15 bytes total, all in protected,
    /// probationary empty. The total counters alone would still pass if entries
    /// leaked into probationary, so we also assert per-queue contents explicitly --
    /// the empty-probationary assertion is what proves the regression precondition.
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 3);
    ASSERT_EQ(priority.getSize(state_guard.lock()), 15);
    ASSERT_EQ(priority.getProtectedElementsCount(state_guard.lock()), 3);
    ASSERT_EQ(priority.getProtectedSize(state_guard.lock()), 15);
    ASSERT_EQ(priority.getProbationaryElementsCount(state_guard.lock()), 0);
    ASSERT_EQ(priority.getProbationarySize(state_guard.lock()), 0);

    /// Call `collectEvictionInfo` with `is_total_space_cleanup=true` and a request
    /// covering everything currently in the cache. This is what the background thread
    /// invokes when `desired_size`/`desired_elements_num` is below the current usage
    /// (i.e. `keep_free_space_size(elements)_ratio` is set high enough to drain the cache).
    ///
    /// Without the fix, this aborts via the chassert in debug/sanitizer builds.
    /// With the fix, the function routes the full request to the protected queue
    /// (since probationary is empty) and returns a valid eviction info.
    EvictionInfoPtr eviction_info;
    ASSERT_NO_THROW({
        eviction_info = priority.collectEvictionInfo(
            /* size */15,
            /* elements */3,
            /* reservee */nullptr,
            /* is_total_space_cleanup */true,
            origin,
            state_guard.lock());
    });

    ASSERT_NE(eviction_info, nullptr);
    ASSERT_TRUE(eviction_info->requiresEviction());
}

TEST_F(FileCacheTest, FileCacheGetOrSet)
{
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 30;
    settings[FileCacheSetting::max_elements] = 5;
    settings[FileCacheSetting::max_file_segment_size] = 25;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;

    const auto & user = FileCache::getCommonOrigin();
    const auto key = DB::FileCacheKey::fromPath("key1");

    auto cache = DB::FileCache("1", settings);
    cache.initialize();

    {
        auto holder = cache.getOrSet(key, 0, 20, /* file_size */25, {}, 0, user, /* boundary_alignment */30);
        assertEqual(holder, { Range(0, 24) }, { State::EMPTY });
    }
    {
        auto holder = cache.getOrSet(key, 0, 20, /* file_size */25, {}, 0, user, /* boundary_alignment */22);
        assertEqual(holder, { Range(0, 21) }, { State::EMPTY });
    }
    {
        auto holder = cache.getOrSet(key, 0, 20, /* file_size */25, {}, 0, user, /* boundary_alignment */3);
        assertEqual(holder, { Range(0, 20) }, { State::EMPTY });
    }
    {
        auto holder = cache.getOrSet(key, 0, 20, /* file_size */25, {}, 0, user, /* boundary_alignment */5);
        assertEqual(holder, { Range(0, 19) }, { State::EMPTY });
    }
    {
        auto holder = cache.getOrSet(key, 0, 20, /* file_size */25, {}, 0, user, /* boundary_alignment */1);
        assertEqual(holder, { Range(0, 19) }, { State::EMPTY });
    }
    {
        auto holder = cache.getOrSet(key, 0, 22, /* file_size */25, {}, 0, user, /* boundary_alignment */7);
        assertEqual(holder, { Range(0, 24) }, { State::EMPTY });

        auto holder2 = cache.getOrSet(key, 0, 26, /* file_size */27, {}, 0, user, /* boundary_alignment */30);
        assertEqual(holder2, { Range(0, 24), Range(25, 26) }, { State::EMPTY, State::EMPTY });
    }
    {
        auto holder = cache.getOrSet(key, 0, 22, /* file_size */25, {}, 0, user, /* boundary_alignment */30);
        assertEqual(holder, { Range(0, 24) }, { State::EMPTY });

        auto holder2 = cache.getOrSet(key, 0, 19, /* file_size */27, {}, 0, user, /* boundary_alignment */30);
        assertEqual(holder2, { Range(0, 24) }, { State::EMPTY });
    }
    {
        auto holder = cache.getOrSet(key, 0, 25, /* file_size */26, {}, 0, user, /* boundary_alignment */30);
        assertEqual(holder, { Range(0, 24) }, { State::EMPTY });
    }
    {
        auto holder = cache.getOrSet(key, 0, 25, /* file_size */20, {}, 0, user, /* boundary_alignment */30);
        assertEqual(holder, { Range(0, 19) }, { State::EMPTY });
    }
}

TEST_F(FileCacheTest, ContinueEvictionPos)
{
    ServerUUID::setRandomForUnitTests();

    size_t max_size = 50;
    size_t max_elements = 3;

    LRUFileCachePriority priority(IFileCachePriority::QueueType::Main, max_size, max_elements);

    std::string cache_path = std::filesystem::path(caches_dir) / "test_eviction_pos";
    CacheMetadata cache_metadata(cache_path, 0, 0, false);

    auto key = DB::FileCacheKey::fromPath("evict_key");
    auto origin = FileCache::getCommonOrigin();

    CacheStateGuard state_guard;
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(origin), &cache_metadata);

    auto add_file_segment = [&](size_t offset, size_t size)
    {
        IFileCachePriority::IteratorPtr it;
        {
            auto state_lock = state_guard.lock();
            it = priority.add(key_metadata, offset, size, &state_lock);
        }
        auto path = cache_metadata.getFileSegmentPath(key, offset, FileSegmentKind::Regular, origin);

        if (std::filesystem::exists(path))
            std::filesystem::remove(path);

        std::filesystem::create_directories(std::filesystem::path(path).parent_path());
        std::string data(size, '0');
        WriteBufferFromFile wb(path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
        DB::writeString(data, wb);
        wb.finalize();

        auto file_segment = std::make_shared<FileSegment>(
            key, offset, size, FileSegment::State::DOWNLOADED,
            CreateFileSegmentSettings{}, false, nullptr, key_metadata, it);

        LockedKey(key_metadata).emplace(offset, std::make_shared<FileSegmentMetadata>(std::move(file_segment)));

        return it;
    };

    auto it1 = add_file_segment(0, 10);
    auto it2 = add_file_segment(10, 10);

    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 2);
    ASSERT_EQ(priority.getEvictionPosCount(IFileCachePriority::EvictionCursor::Reserve), 2); /// queue.end()

    FileCacheReserveStat stat;
    auto evicted = std::make_unique<EvictionCandidates>(IFileCachePriority::OnEvictCallback{});

    auto eviction_info = priority.collectEvictionInfo(10, 1, nullptr, false, origin, state_guard.lock());
    priority.collectCandidatesForEviction(*eviction_info, stat, *evicted, nullptr, IFileCachePriority::EvictionCursor::Reserve, 0, false, origin, state_guard);
    eviction_info.reset();

    ASSERT_EQ(evicted->size(), 0); /// Nothing is evicted.
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 2);
    ASSERT_EQ(priority.getEvictionPosCount(IFileCachePriority::EvictionCursor::Reserve), 2); /// queue.end()

    auto it3 = add_file_segment(20, 10);

    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 3);
    ASSERT_EQ(priority.getEvictionPosCount(IFileCachePriority::EvictionCursor::Reserve), 3); /// queue.end()

    evicted = std::make_unique<EvictionCandidates>(IFileCachePriority::OnEvictCallback{});
    stat = {};
    eviction_info = priority.collectEvictionInfo(10, 1, nullptr, false, origin, state_guard.lock());
    priority.collectCandidatesForEviction(*eviction_info, stat, *evicted, nullptr, IFileCachePriority::EvictionCursor::Reserve, 0, false, origin, state_guard);

    ASSERT_EQ(evicted->size(), 1);
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 3);
    ASSERT_EQ(priority.getEvictionPosCount(IFileCachePriority::EvictionCursor::Reserve), 0); /// queue.begin()

    {
        evicted->evict();
        evicted->afterEvictState(state_guard.lock());
        evicted->afterEvictWrite();
        evicted.reset();
    }
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 2);
    ASSERT_EQ(priority.getEvictionPosCount(IFileCachePriority::EvictionCursor::Reserve), 0); /// still queue.begin(), but it2

    auto get_file_segment = [&](size_t offset)
    {
        return LockedKey(key_metadata).getByOffset(offset)->file_segment;
    };

    /// Make fs2 (it2) non-evictable.
    auto fs2 = get_file_segment(10);
    ASSERT_EQ(it2->getEntry()->offset, fs2->offset());
    /// Make fs3 (it3) non-evictable.
    auto fs3 = get_file_segment(20);
    ASSERT_EQ(it3->getEntry()->offset, fs3->offset());

    auto it4 = add_file_segment(30, 10);
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 3);
    ASSERT_EQ(priority.getEvictionPosCount(IFileCachePriority::EvictionCursor::Reserve), 0);

    evicted = std::make_unique<EvictionCandidates>(IFileCachePriority::OnEvictCallback{});
    stat = {};
    eviction_info = priority.collectEvictionInfo(10, 1, nullptr, false, origin, state_guard.lock());
    priority.collectCandidatesForEviction(*eviction_info, stat, *evicted, nullptr, IFileCachePriority::EvictionCursor::Reserve, 0, false, origin, state_guard);

    ASSERT_EQ(evicted->size(), 1);
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 3);
    ASSERT_EQ(priority.getEvictionPosCount(IFileCachePriority::EvictionCursor::Reserve), 3); /// 3 and not 2, because 1 entry is invalidated.

    fs2.reset();
    fs3.reset();

    priority.resetEvictionPos(IFileCachePriority::EvictionCursor::Reserve);
    ASSERT_EQ(priority.getEvictionPosCount(IFileCachePriority::EvictionCursor::Reserve), 0); /// queue.begin()
}

TEST_F(FileCacheTest, MoveEvictionPos)
{
    ServerUUID::setRandomForUnitTests();

    /// Two independent LRU queues, modelling SLRU's protected/probationary sub-queues
    /// between which `LRUFileCachePriority::move` transfers entries.
    LRUFileCachePriority src(IFileCachePriority::QueueType::Main, /* max_size */100, /* max_elements */10, "src");
    LRUFileCachePriority dst(IFileCachePriority::QueueType::Main, /* max_size */100, /* max_elements */10, "dst");

    std::string cache_path = std::filesystem::path(caches_dir) / "test_move_eviction_pos";
    CacheMetadata cache_metadata(cache_path, 0, 0, false);

    auto key = DB::FileCacheKey::fromPath("move_key");
    auto origin = FileCache::getCommonOrigin();
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(origin), &cache_metadata);

    CacheStateGuard state_guard;

    /// Both queues must share one structural guard:
    /// `LRUFileCachePriority::move` splices both lists under a single write lock.
    auto & cache_guard = src.getPriorityGuard();
    dst.setPriorityGuard(cache_guard);

    using Entry = IFileCachePriority::Entry;
    auto add_to_src = [&](size_t offset, size_t size)
    {
        auto write_lock = cache_guard.writeLock();
        auto state_lock = state_guard.lock();
        return src.add(std::make_shared<Entry>(key, offset, size, key_metadata), write_lock, &state_lock);
    };

    /// src queue: [offset 0, offset 10, offset 20].
    add_to_src(0, 10);
    auto it_middle = add_to_src(10, 10);
    add_to_src(20, 10);

    /// Point `src`'s eviction position at the middle entry - the one we are about to move out.
    {
        auto read_lock = cache_guard.readLock();
        src.setEvictionPos(IFileCachePriority::EvictionCursor::Reserve, it_middle.get(), read_lock);
    }
    ASSERT_EQ((*src.getEvictionPos(IFileCachePriority::EvictionCursor::Reserve, cache_guard.readLock()))->offset, 10u);

    /// Move the middle entry out of `src` into `dst` (as an SLRU upgrade/downgrade would).
    /// `move` is called on the destination queue; `src` is the source.
    {
        auto write_lock = cache_guard.writeLock();
        auto state_lock = state_guard.lock();
        dst.move(it_middle, src, write_lock, state_lock);
    }

    /// The moved node was spliced out of src, so src's eviction position must advance to the
    /// next surviving src entry (offset 20). Before the fix it kept pointing at the moved node,
    /// which now lives in `dst` (offset 10) — a dangling cross-queue eviction position.
    ASSERT_EQ((*src.getEvictionPos(IFileCachePriority::EvictionCursor::Reserve, cache_guard.readLock()))->offset, 20u);
}

TEST_F(FileCacheTest, LoadMetadataParallelism)
{
    /// Test that loading cache metadata with different numbers of threads produces
    /// correct results. We build a complex structure — many keys spread across
    /// different 3-char prefix directories, each with multiple segments at
    /// non-overlapping offsets — and then reload it with 1, 3, and 32 threads.

    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    const size_t num_keys = 50;
    const size_t segments_per_key = 3;
    const size_t segment_size = 50;
    const size_t file_size = segments_per_key * segment_size;

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = num_keys * segments_per_key * segment_size * 2;
    settings[FileCacheSetting::max_elements] = num_keys * segments_per_key * 2;
    settings[FileCacheSetting::max_file_segment_size] = segment_size;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::load_metadata_threads] = 1;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    /// Use diverse paths so keys hash to many different 3-char prefix directories,
    /// exercising parallel listing across multiple prefix dirs.
    std::vector<FileCacheKey> keys;
    keys.reserve(num_keys);
    for (size_t i = 0; i < num_keys; ++i)
        keys.push_back(FileCacheKey::fromPath("test/dir/subdir_" + std::to_string(i * 7) + "/file_" + std::to_string(i)));

    const auto & user = FileCache::getCommonOrigin();

    /// Phase 1: populate cache with the full key/segment structure and download everything.
    {
        auto cache = DB::FileCache("LoadMetadataParallelism_init", settings);
        cache.initialize();

        for (size_t k = 0; k < num_keys; ++k)
        {
            for (size_t s = 0; s < segments_per_key; ++s)
            {
                auto holder = cache.getOrSet(keys[k], s * segment_size, segment_size, file_size, {}, 0, user);
                ASSERT_EQ(holder->size(), 1);
                download(*holder->begin());
            }
        }
    }

    /// Phase 2: reload with different thread counts and verify all segments are intact.
    for (UInt64 thread_count : {1u, 3u, 32u})
    {
        const UInt64 expected_listing = std::max(UInt64(1), thread_count / 2);
        const UInt64 expected_loading = thread_count - expected_listing;

        settings[FileCacheSetting::load_metadata_threads] = thread_count;

        testing::internal::CaptureStderr();
        auto cache = DB::FileCache("LoadMetadataParallelism_" + std::to_string(thread_count), settings);
        cache.initialize();
        const auto log_output = testing::internal::GetCapturedStderr();

        const auto expected_log = fmt::format(
            "using {} listing thread(s) and {} loading thread(s)",
            expected_listing, expected_loading);
        ASSERT_NE(log_output.find(expected_log), std::string::npos)
            << "Expected log message not found for load_metadata_threads=" << thread_count
            << "\nExpected substring: " << expected_log;

        size_t total_loaded = 0;
        for (size_t k = 0; k < num_keys; ++k)
        {
            auto infos = cache.getFileSegmentInfos(keys[k], user.user_id);
            ASSERT_EQ(infos.size(), segments_per_key)
                << "key_index=" << k << " load_metadata_threads=" << thread_count;

            std::sort(infos.begin(), infos.end(), [](const auto & a, const auto & b)
            {
                return a.range_left < b.range_left;
            });

            for (size_t s = 0; s < segments_per_key; ++s)
            {
                ASSERT_EQ(infos[s].state, State::DOWNLOADED)
                    << "key_index=" << k << " segment=" << s << " load_metadata_threads=" << thread_count;
                ASSERT_EQ(infos[s].range_left, s * segment_size);
                ASSERT_EQ(infos[s].range_right, (s + 1) * segment_size - 1);
            }
            total_loaded += infos.size();
        }

        ASSERT_EQ(total_loaded, num_keys * segments_per_key)
            << "load_metadata_threads=" << thread_count;
    }
}

TEST_F(FileCacheTest, PartiallyDownloadedDynamicResizeAssertion)
{
    /// Regression: dynamic resize temporarily clears the queue iterator before
    /// evicting a `PARTIALLY_DOWNLOADED` segment. The invariant must allow that
    /// delayed-removal state.

    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("partial_dl_dynamic_resize");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 16;
    settings[FileCacheSetting::max_elements] = 4;
    settings[FileCacheSetting::max_file_segment_size] = 8;
    settings[FileCacheSetting::boundary_alignment] = 8;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;
    settings[FileCacheSetting::allow_dynamic_cache_resize] = true;

    auto cache = std::make_shared<DB::FileCache>("partial_dl_resize", settings);
    cache->initialize();

    const auto & user = FileCache::getCommonOrigin();
    auto key = DB::FileCacheKey::fromPath("partial_dl_resize_key");

    /// Segment 1: `PARTIALLY_DOWNLOADED` with reserved size 8 and downloaded size 3.
    {
        auto holder = cache->getOrSet(key, 0, 8, /*file_size=*/8, {}, 0, user);
        ASSERT_EQ(holder->size(), 1u);
        auto seg = *holder->begin();
        ASSERT_EQ(seg->state(), State::EMPTY);

        ASSERT_EQ(seg->getOrSetDownloader(), FileSegment::getCallerId());
        ASSERT_EQ(seg->state(), State::DOWNLOADING);

        std::string failure_reason;
        ASSERT_TRUE(seg->reserve(/*size_to_reserve=*/8, /*lock_wait_timeout_milliseconds=*/1000, failure_reason));

        /// `seg->write` expects the key directory to exist, as in `download`.
        auto key_str = key.toString();
        auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) / key_str;
        if (!fs::exists(subdir))
            fs::create_directories(subdir);
        std::string data(3, 'a');
        seg->write(data.data(), data.size(), seg->getCurrentWriteOffset());

        FileSegment::complete(
            FileSegmentPtr(seg),
            /*allow_background_download=*/false,
            /*force_shrink_to_downloaded_size=*/false);

        ASSERT_EQ(seg->state(), State::PARTIALLY_DOWNLOADED)
            << "Test setup did not produce a PARTIALLY_DOWNLOADED segment; "
               "got: " << FileSegment::stateToString(seg->state());
        ASSERT_EQ(seg->getReservedSize(), 8u);
        ASSERT_EQ(seg->getDownloadedSize(), 3u);
    }

    /// Segment 2: a `DOWNLOADED` segment to make resize evict real entries.
    {
        auto holder = cache->getOrSet(key, 8, 8, /*file_size=*/16, {}, 0, user);
        ASSERT_EQ(holder->size(), 1u);
        auto seg = *holder->begin();
        ASSERT_EQ(seg->state(), State::EMPTY);
        download(seg, /*complete=*/true);
        ASSERT_EQ(seg->state(), State::DOWNLOADED);
    }

    /// Sanity: the partial segment is still in `PARTIALLY_DOWNLOADED`.
    {
        auto infos = cache->getFileSegmentInfos(key, user.user_id);
        ASSERT_EQ(infos.size(), 2u);
        bool found_partial = false;
        for (const auto & info : infos)
        {
            if (info.range_left == 0 && info.range_right == 7)
            {
                ASSERT_EQ(info.state, State::PARTIALLY_DOWNLOADED);
                ASSERT_EQ(info.downloaded_size, 3u);
                found_partial = true;
            }
        }
        ASSERT_TRUE(found_partial);
    }

    /// Trigger resize while the partial segment is in delayed-removal state.
    DB::FileCacheSettings new_settings = settings;
    new_settings[FileCacheSetting::max_size] = 4;
    DB::FileCacheSettings actual_settings = settings;

    ASSERT_NO_THROW(cache->applySettingsIfPossible(new_settings, actual_settings));

    ASSERT_LE(cache->getUsedCacheSize(), 4u);
}

TEST_F(FileCacheTest, FailedEvictionRestorePreservesInvariants)
{
    /// Regression: failed eviction must restore queue entries with reserved size
    /// and clear delayed-removal state on the segment.

    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("failed_eviction_restore");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 16;
    settings[FileCacheSetting::max_elements] = 4;
    settings[FileCacheSetting::max_file_segment_size] = 8;
    settings[FileCacheSetting::boundary_alignment] = 8;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;
    settings[FileCacheSetting::allow_dynamic_cache_resize] = true;

    auto cache = std::make_shared<DB::FileCache>("failed_eviction_restore", settings);
    cache->initialize();

    const auto & user = FileCache::getCommonOrigin();
    auto key = DB::FileCacheKey::fromPath("failed_eviction_restore_key");

    /// `PARTIALLY_DOWNLOADED` segment, reserved size 8 and downloaded size 3.
    {
        auto holder = cache->getOrSet(key, 0, 8, /*file_size=*/8, {}, 0, user);
        auto seg = *holder->begin();
        ASSERT_EQ(seg->getOrSetDownloader(), FileSegment::getCallerId());
        std::string failure_reason;
        ASSERT_TRUE(seg->reserve(/*size_to_reserve=*/8, /*lock_wait_timeout_milliseconds=*/1000, failure_reason));

        auto key_str = key.toString();
        auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) / key_str;
        if (!fs::exists(subdir))
            fs::create_directories(subdir);
        std::string data(3, 'a');
        seg->write(data.data(), data.size(), seg->getCurrentWriteOffset());

        FileSegment::complete(FileSegmentPtr(seg), false, false);
        ASSERT_EQ(seg->state(), State::PARTIALLY_DOWNLOADED);
        /// The holder is still alive, so the segment is not shrunk yet and keeps its full
        /// reservation; the reserve-ahead surplus (8 reserved vs 3 downloaded) is reclaimed
        /// once the holder below is destroyed and the last-holder completion shrinks it.
        ASSERT_EQ(seg->getReservedSize(), 8u);
        ASSERT_EQ(seg->getDownloadedSize(), 3u);
    }

    /// Second segment to keep the cache full and force eviction during resize.
    {
        auto holder = cache->getOrSet(key, 8, 8, /*file_size=*/16, {}, 0, user);
        auto seg = *holder->begin();
        download(seg, /*complete=*/true);
        ASSERT_EQ(seg->state(), State::DOWNLOADED);
    }

    /// seg1's surplus was reclaimed when its holder was released (3 reserved) and seg2 is
    /// fully downloaded (8 reserved).
    ASSERT_EQ(cache->getUsedCacheSize(), 11u);
    ASSERT_EQ(cache->getFileSegmentsNum(), 2u);

    /// Force the failed-eviction restore loop to run.
    {
        DB::FailPointInjection::enableFailPoint("file_cache_dynamic_resize_fail_to_evict");
        SCOPE_EXIT({
            DB::FailPointInjection::disableFailPoint("file_cache_dynamic_resize_fail_to_evict");
        });

        /// Trigger resize. The restore path must keep the total queue size at 11.
        DB::FileCacheSettings new_settings = settings;
        new_settings[FileCacheSetting::max_size] = 4;
        DB::FileCacheSettings actual_settings = settings;

        ASSERT_NO_THROW(cache->applySettingsIfPossible(new_settings, actual_settings));

        /// Failed eviction reverts limits to the previous value.
        ASSERT_EQ(actual_settings[FileCacheSetting::max_size].value, 16u);

        /// Release-visible check for restored reserved-size accounting.
        ASSERT_EQ(cache->getUsedCacheSize(), 11u);
        ASSERT_EQ(cache->getFileSegmentsNum(), 2u);

        /// All segments must still be reachable from the priority queue.
        {
            auto infos = cache->getFileSegmentInfos(key, user.user_id);
            ASSERT_EQ(infos.size(), 2u);
            for (const auto & info : infos)
                ASSERT_NE(info.queue_entry_type, IFileCachePriority::QueueEntryType::None);
        }
    }

    /// A second resize verifies delayed-removal state was cleared.
    {
        DB::FileCacheSettings second_new_settings = settings;
        second_new_settings[FileCacheSetting::max_size] = 4;
        DB::FileCacheSettings second_actual = settings;

        ASSERT_NO_THROW(cache->applySettingsIfPossible(second_new_settings, second_actual));
        ASSERT_LE(cache->getUsedCacheSize(), 4u);
    }
}

TEST_F(FileCacheTest, EvictionMetricsTryIncreasePriority)
{
    /// Regression: when tryIncreasePriority promotes a probationary entry and the
    /// protected queue is full, some protected entries are downgraded to probationary.
    /// If probationary is also full at that point (the promoted entry still holds its
    /// slot with a Moving flag), real probationary entries are evicted to make room.
    /// Those evictions must fire onSegmentEvicted and advance the metric counter.

    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(R"(<clickhouse></clickhouse>)");
    getMutableContext().context->setConfig(new Poco::Util::XMLConfiguration(document));
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("eviction_metrics_promotion_test");
    auto query_scope_holder = DB::QueryScope::create(query_context);

    auto sum_dim = [](const std::string & name)
    {
        double total = 0;
        ::DimensionalMetrics::Factory::instance().forEachFamily([&](::DimensionalMetrics::MetricFamily & f)
        {
            if (f.getName() != name) return;
            f.forEachMetric([&](const ::DimensionalMetrics::LabelValues &, const ::DimensionalMetrics::Metric & m) { total += m.get(); });
        });
        return total;
    };

    /// SLRU: probationary = 10 B, protected = 10 B; five 5-byte segments fit exactly.
    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 20;
    settings[FileCacheSetting::max_elements] = 10;
    settings[FileCacheSetting::max_file_segment_size] = 5;
    settings[FileCacheSetting::boundary_alignment] = 5;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::SLRU;
    settings[FileCacheSetting::slru_size_ratio] = 0.5;
    settings[FileCacheSetting::expose_prometheus_eviction_metrics] = true;

    auto cache = DB::FileCache("eviction_metrics_promotion", settings);
    cache.initialize();
    const auto & user = FileCache::getCommonOrigin();
    auto key = FileCacheKey::fromPath("eviction_metrics_promotion_key");

    /// A and B go to probationary, get promoted to protected, then their holders
    /// are released so they become releasable eviction/downgrade candidates.
    {
        auto holderA = cache.getOrSet(key, 0, 5, /*file_size=*/20, {}, 0, user);
        ASSERT_EQ(holderA->size(), 1u);
        download(*holderA->begin());

        auto holderB = cache.getOrSet(key, 5, 5, /*file_size=*/20, {}, 0, user);
        ASSERT_EQ(holderB->size(), 1u);
        download(*holderB->begin());

        increasePriority(holderA);
        increasePriority(holderB);
    }
    /// Protected: {A=5, B=5} = 10/10 full. Probationary: empty.
    /// A and B are releasable: no live holders, so they can be downgraded.

    /// C and D fill probationary. D's holder is released so it is evictable.
    auto holderC = cache.getOrSet(key, 10, 5, /*file_size=*/20, {}, 0, user);
    ASSERT_EQ(holderC->size(), 1u);
    download(*holderC->begin());

    {
        auto holderD = cache.getOrSet(key, 15, 5, /*file_size=*/20, {}, 0, user);
        ASSERT_EQ(holderD->size(), 1u);
        download(*holderD->begin());
    }
    /// Protected: {A, B} = 10/10. Probationary: {C, D} = 10/10.
    /// D is releasable: no live holder.

    const auto evictions_before = sum_dim("filesystem_cache_evictions_total");

    /// Promote C: protected full → downgrade A (releasable) → probationary has no
    /// room (C holds its slot with a Moving flag) → D (releasable) is evicted.
    increasePriority(holderC);

    EXPECT_GT(sum_dim("filesystem_cache_evictions_total") - evictions_before, 0.0)
        << "Promotion-induced probationary eviction must be counted in the metric";
}

TEST_F(FileCacheTest, ExposeEvictionMetrics)
{
    /// Verify that `filesystem_cache_*` metric families update iff the
    /// `expose_prometheus_eviction_metrics` / `_per_user`
    /// flags are set on the cache.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(R"(<clickhouse></clickhouse>)");
    getMutableContext().context->setConfig(new Poco::Util::XMLConfiguration(document));
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("eviction_metrics_test");
    auto query_scope_holder = DB::QueryScope::create(query_context);

    auto sum_dim = [](const std::string & name)
    {
        double total = 0;
        ::DimensionalMetrics::Factory::instance().forEachFamily([&](::DimensionalMetrics::MetricFamily & f)
        {
            if (f.getName() != name) return;
            f.forEachMetric([&](const ::DimensionalMetrics::LabelValues &, const ::DimensionalMetrics::Metric & m) { total += m.get(); });
        });
        return total;
    };
    auto sum_hist = [](const std::string & name)
    {
        /// Each observation increments exactly one bucket (or the +Inf overflow),
        /// so summing every bucket counter gives the total observation count.
        uint64_t total = 0;
        ::HistogramMetrics::Factory::instance().forEachFamily([&](::HistogramMetrics::MetricFamily & f)
        {
            if (f.getName() != name) return;
            const auto & buckets = f.getBuckets();
            f.forEachMetric([&](const ::HistogramMetrics::LabelValues &, const ::HistogramMetrics::Metric & m)
            {
                for (size_t i = 0; i <= buckets.size(); ++i)
                    total += m.getCounter(i);
            });
        });
        return total;
    };
    auto dim_value_for_labels = [](const std::string & name, ::DimensionalMetrics::Labels expected_labels, ::DimensionalMetrics::LabelValues expected_label_values)
    {
        bool found_family = false;
        bool found_metric = false;
        double value = 0;
        ::DimensionalMetrics::Factory::instance().forEachFamily([&](::DimensionalMetrics::MetricFamily & f)
        {
            if (f.getName() != name)
                return;

            found_family = true;
            EXPECT_EQ(f.getLabels(), expected_labels);
            f.forEachMetric([&](const ::DimensionalMetrics::LabelValues & label_values, const ::DimensionalMetrics::Metric & m)
            {
                if (label_values == expected_label_values)
                {
                    found_metric = true;
                    value += m.get();
                }
            });
        });

        EXPECT_TRUE(found_family) << "Missing dimensional metric family " << name;
        EXPECT_TRUE(found_metric) << "Missing dimensional metric row " << name;
        return value;
    };
    auto hist_observations_for_labels = [](const std::string & name, ::HistogramMetrics::Labels expected_labels, ::HistogramMetrics::LabelValues expected_label_values)
    {
        bool found_family = false;
        bool found_metric = false;
        uint64_t total = 0;
        ::HistogramMetrics::Factory::instance().forEachFamily([&](::HistogramMetrics::MetricFamily & f)
        {
            if (f.getName() != name)
                return;

            found_family = true;
            EXPECT_EQ(f.getLabels(), expected_labels);
            const auto & buckets = f.getBuckets();
            f.forEachMetric([&](const ::HistogramMetrics::LabelValues & label_values, const ::HistogramMetrics::Metric & m)
            {
                if (label_values == expected_label_values)
                {
                    found_metric = true;
                    for (size_t i = 0; i <= buckets.size(); ++i)
                        total += m.getCounter(i);
                }
            });
        });

        EXPECT_TRUE(found_family) << "Missing histogram metric family " << name;
        EXPECT_TRUE(found_metric) << "Missing histogram metric row " << name;
        return total;
    };

    const FileCacheOriginInfo origin("eviction_metrics_test_user", 0);
    const auto & user_id = origin.user_id;

    auto run_workload = [&](const std::string & cache_name, bool expose, bool per_user)
    {
        DB::FileCacheSettings settings;
        settings[FileCacheSetting::path] = cache_base_path;
        settings[FileCacheSetting::max_size] = 40;
        settings[FileCacheSetting::max_elements] = 6;
        settings[FileCacheSetting::boundary_alignment] = 1;
        settings[FileCacheSetting::load_metadata_asynchronously] = false;
        settings[FileCacheSetting::cache_policy] = FileCachePolicy::SLRU;
        settings[FileCacheSetting::slru_size_ratio] = 0.5;
        settings[FileCacheSetting::expose_prometheus_eviction_metrics] = expose;
        settings[FileCacheSetting::expose_prometheus_eviction_metrics_per_user] = per_user;

        auto cache = DB::FileCache(cache_name, settings);
        cache.initialize();
        auto key = FileCacheKey::fromPath(cache_name);
        /// max_size=40, max_elements=6; 8 segments of size 5 forces evictions.
        for (size_t i = 0; i < 8; ++i)
        {
            auto holder = cache.getOrSet(key, i * 10, 5, static_cast<size_t>(-1), {}, 0, origin);
            ASSERT_EQ(holder->size(), 1u);
            download(*holder->begin());
        }
    };

    const std::string aggregate_cache_name = "cache_with_eviction_metrics";
    const std::string per_user_cache_name = "cache_with_eviction_metrics_per_user";

    const auto evictions_off_before = sum_dim("filesystem_cache_evictions_total");
    const auto by_user_off_before = sum_dim("filesystem_cache_evictions_by_user_total");
    run_workload("eviction_metrics_off", /*expose=*/false, /*per_user=*/false);
    EXPECT_EQ(sum_dim("filesystem_cache_evictions_total"), evictions_off_before);
    EXPECT_EQ(sum_dim("filesystem_cache_evictions_by_user_total"), by_user_off_before);

    const auto evictions_before = sum_dim("filesystem_cache_evictions_total");
    const auto bytes_before = sum_dim("filesystem_cache_evicted_bytes_total");
    const auto hits_before = sum_hist("filesystem_cache_evicted_segment_hits");
    const auto size_before = sum_hist("filesystem_cache_evicted_segment_size_bytes");
    const auto by_user_before = sum_dim("filesystem_cache_evictions_by_user_total");
    run_workload(aggregate_cache_name, /*expose=*/true, /*per_user=*/false);
    const auto evictions_delta = sum_dim("filesystem_cache_evictions_total") - evictions_before;
    EXPECT_GT(evictions_delta, 0.0);
    EXPECT_GT(sum_dim("filesystem_cache_evicted_bytes_total") - bytes_before, 0.0);
    EXPECT_EQ(static_cast<uint64_t>(evictions_delta), sum_hist("filesystem_cache_evicted_segment_hits") - hits_before);
    EXPECT_EQ(static_cast<uint64_t>(evictions_delta), sum_hist("filesystem_cache_evicted_segment_size_bytes") - size_before);
    EXPECT_EQ(sum_dim("filesystem_cache_evictions_by_user_total"), by_user_before);
    EXPECT_GT(dim_value_for_labels("filesystem_cache_evictions_total", {"cache_name"}, {aggregate_cache_name}), 0.0);
    EXPECT_GT(dim_value_for_labels("filesystem_cache_evicted_bytes_total", {"cache_name"}, {aggregate_cache_name}), 0.0);
    EXPECT_GT(hist_observations_for_labels("filesystem_cache_evicted_segment_hits", {"cache_name"}, {aggregate_cache_name}), 0u);
    EXPECT_GT(hist_observations_for_labels("filesystem_cache_evicted_segment_size_bytes", {"cache_name"}, {aggregate_cache_name}), 0u);

    const auto by_user_pre = sum_dim("filesystem_cache_evictions_by_user_total");
    run_workload(per_user_cache_name, /*expose=*/true, /*per_user=*/true);
    EXPECT_GT(sum_dim("filesystem_cache_evictions_by_user_total") - by_user_pre, 0.0);
    EXPECT_GT(dim_value_for_labels("filesystem_cache_evictions_by_user_total", {"cache_name", "user_id"}, {per_user_cache_name, user_id}), 0.0);
    EXPECT_GT(dim_value_for_labels("filesystem_cache_evicted_bytes_by_user_total", {"cache_name", "user_id"}, {per_user_cache_name, user_id}), 0.0);
    EXPECT_GT(hist_observations_for_labels("filesystem_cache_evicted_segment_hits_by_user", {"cache_name", "user_id"}, {per_user_cache_name, user_id}), 0u);
    EXPECT_GT(hist_observations_for_labels("filesystem_cache_evicted_segment_size_bytes_by_user", {"cache_name", "user_id"}, {per_user_cache_name, user_id}), 0u);
}

TEST_F(FileCacheTest, EvictionMetricsRuntimeToggle)
{
    /// The eviction-metric settings are advertised as runtime-reloadable
    /// (SYSTEM RELOAD CONFIG -> applySettingsIfPossible). Exercise the full cycle
    /// on a live cache: disabled -> no delta, enable -> metric advances,
    /// disable again -> no further delta. Guards against a regression where
    /// applySettingsIfPossible stops propagating these flags to the atomics.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(R"(<clickhouse></clickhouse>)");
    getMutableContext().context->setConfig(new Poco::Util::XMLConfiguration(document));
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("eviction_metrics_runtime_toggle_test");
    auto query_scope_holder = DB::QueryScope::create(query_context);

    auto sum_dim = [](const std::string & name)
    {
        double total = 0;
        ::DimensionalMetrics::Factory::instance().forEachFamily([&](::DimensionalMetrics::MetricFamily & f)
        {
            if (f.getName() != name) return;
            f.forEachMetric([&](const ::DimensionalMetrics::LabelValues &, const ::DimensionalMetrics::Metric & m) { total += m.get(); });
        });
        return total;
    };

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 40;
    settings[FileCacheSetting::max_elements] = 6;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::SLRU;
    settings[FileCacheSetting::slru_size_ratio] = 0.5;
    settings[FileCacheSetting::expose_prometheus_eviction_metrics] = false;

    auto cache = DB::FileCache("eviction_metrics_runtime_toggle", settings);
    cache.initialize();

    /// max_size=40, max_elements=6; 8 segments of size 5 force evictions.
    /// A fresh key per round avoids hits/promotions, so each round evicts anew.
    size_t round = 0;
    auto force_evictions = [&]
    {
        auto key = FileCacheKey::fromPath("toggle_key_" + std::to_string(round++));
        for (size_t i = 0; i < 8; ++i)
        {
            auto holder = cache.getOrSet(key, i * 10, 5, static_cast<size_t>(-1), {}, 0, FileCache::getCommonOrigin());
            ASSERT_EQ(holder->size(), 1u);
            download(*holder->begin());
        }
    };

    /// Flip the flag through the reload path. `current` tracks applied state,
    /// as applySettingsIfPossible updates its second (actual) argument in place.
    DB::FileCacheSettings current = settings;
    auto reload_expose = [&](bool value)
    {
        DB::FileCacheSettings new_settings = current;
        new_settings[FileCacheSetting::expose_prometheus_eviction_metrics] = value;
        ASSERT_NO_THROW(cache.applySettingsIfPossible(new_settings, current));
    };

    /// 1) Disabled: evictions happen, but the metric must not move.
    const auto before_disabled = sum_dim("filesystem_cache_evictions_total");
    force_evictions();
    EXPECT_EQ(sum_dim("filesystem_cache_evictions_total"), before_disabled);

    /// 2) Enable at runtime: the metric must now advance.
    reload_expose(true);
    const auto before_enabled = sum_dim("filesystem_cache_evictions_total");
    force_evictions();
    EXPECT_GT(sum_dim("filesystem_cache_evictions_total") - before_enabled, 0.0);

    /// 3) Disable again at runtime: the metric must stop advancing.
    reload_expose(false);
    const auto before_redisabled = sum_dim("filesystem_cache_evictions_total");
    force_evictions();
    EXPECT_EQ(sum_dim("filesystem_cache_evictions_total"), before_redisabled);
}

namespace
{
    /// Creators for SplitFileCachePriority inner queues used by the split-cache tests below.
    std::unique_ptr<IFileCachePriority> makeLRUInner(
        IFileCachePriority::QueueType queue_type, size_t max_size, size_t max_elements,
        double /* size_ratio */, size_t /* overcommit_step */, String desc)
    {
        return std::make_unique<LRUFileCachePriority>(queue_type, max_size, max_elements, desc);
    }

    std::unique_ptr<IFileCachePriority> makeSLRUInner(
        IFileCachePriority::QueueType queue_type, size_t max_size, size_t max_elements,
        double size_ratio, size_t /* overcommit_step */, String desc)
    {
        return std::make_unique<SLRUFileCachePriority>(queue_type, max_size, max_elements, size_ratio, desc);
    }
}

TEST_F(FileCacheTest, SLRUModifySizeLimitsRollbackOnThrow)
{
    /// `modifySizeLimits` must be all-or-nothing: when the probationary update throws
    /// (injected via failpoint), the already-applied protected limit is rolled back.
    ServerUUID::setRandomForUnitTests();

    const size_t max_size = 30;
    const size_t max_elements = 6;
    const double slru_size_ratio = 0.5; /// protected 15/3, probationary 15/3
    SLRUFileCachePriority priority(IFileCachePriority::QueueType::Main, max_size, max_elements, slru_size_ratio, "test_slru_modify_rollback");

    const std::string cache_path = caches_dir / "test_slru_modify_rollback";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path, 0, 0, false);

    const auto key = DB::FileCacheKey::fromPath("slru_modify_rollback_key");
    const auto & origin = FileCache::getCommonOrigin();
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(origin), &cache_metadata);

    CacheStateGuard state_guard;

    /// One small 5-byte entry in each sub-queue, fitting comfortably under old and new limits.
    {
        auto state_lock = state_guard.lock();
        auto lock = priority.getPriorityGuardForTests().writeLock();
        priority.addForRestore(key_metadata, 0, 5,
            IFileCachePriority::QueueEntryType::SLRU_Protected, lock, &state_lock);
        priority.addForRestore(key_metadata, 100, 5,
            IFileCachePriority::QueueEntryType::SLRU_Probationary, lock, &state_lock);
    }
    ASSERT_EQ(priority.getProtectedSize(state_guard.lock()), 5);
    ASSERT_EQ(priority.getProbationarySize(state_guard.lock()), 5);
    ASSERT_EQ(priority.getProtectedSizeLimit(state_guard.lock()), 15);

    /// Resize total to 20 (ratio 0.5 -> protected limit 10). The resize is valid by itself
    /// (current 5/1 fit 10/3); the only thing that throws is the injected failpoint.
    DB::FailPointInjection::enableFailPoint("file_cache_modify_size_limits_fail");
    SCOPE_EXIT({
        DB::FailPointInjection::disableFailPoint("file_cache_modify_size_limits_fail");
    });
    {
        auto state_lock = state_guard.lock();
        ASSERT_ANY_THROW(priority.modifySizeLimits(20, max_elements, slru_size_ratio, state_lock));
    }

    /// With the bug the protected limit was already shrunk to 10; with the fix it is
    /// rolled back to the original 15.
    ASSERT_EQ(priority.getProtectedSizeLimit(state_guard.lock()), 15);
}

TEST_F(FileCacheTest, LRUDecrementSizeToZeroDropsElement)
{
    /// An entry is counted as one element while its size is > 0 (`incrementSize`
    /// adds an element on a 0 -> size transition). `decrementSize` used to subtract
    /// only the size, so emptying an entry left it counted as an element; a later
    /// `remove` then saw size 0, assumed the element was already accounted, and
    /// leaked the count. `decrementSize` must drop the element when it empties the
    /// entry. No production path decrements an entry to exactly 0 today (the shrink
    /// path keeps at least `downloaded_size > 0`), so this exercises the invariant
    /// directly.
    ServerUUID::setRandomForUnitTests();

    LRUFileCachePriority priority(IFileCachePriority::QueueType::Main, /* max_size */100, /* max_elements */10, "lru_decrement_to_zero_test");

    const std::string cache_path = caches_dir / "test_lru_decrement_to_zero";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path,
                                 /* background_download_queue_size_limit */0,
                                 /* background_download_threads */0,
                                 /* write_cache_per_user_directory */false);

    const auto key = DB::FileCacheKey::fromPath("lru_decrement_to_zero_key");
    const auto & origin = FileCache::getCommonOrigin();
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(origin), &cache_metadata);

    CacheStateGuard state_guard;
    CachePriorityGuard cache_guard;

    IFileCachePriority::IteratorPtr it;
    {
        auto state_lock = state_guard.lock();
        it = priority.add(key_metadata, /* offset */0, /* size */5, &state_lock);
    }

    ASSERT_EQ(priority.getSize(state_guard.lock()), 5);
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 1);

    /// Emptying the entry must drop its element immediately (the invariant).
    it->decrementSize(5);
    ASSERT_EQ(priority.getSize(state_guard.lock()), 0);
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 0);

    /// Removing the now-empty entry must not double-subtract (would underflow).
    {
        auto write_lock = cache_guard.writeLock();
        it->remove(write_lock);
    }
    ASSERT_EQ(priority.getSize(state_guard.lock()), 0);
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 0);
}

TEST_F(FileCacheTest, SplitTotalSpaceCleanupReclaimsSystemQueue)
{
    /// Total-space cleanup must reclaim from the System sub-queue too. The bug dispatched
    /// by origin (General -> data), so keep-free-space never freed System-only space.
    ServerUUID::setRandomForUnitTests();

    const size_t max_size = 100;
    const size_t max_elements = 100;
    SplitFileCachePriority priority(
        IFileCachePriority::QueueType::Main, makeLRUInner, max_size, max_elements,
        /* slru_size_ratio */ 0.5, /* split_cache_ratio */ 0.5,
        "test_split_total_cleanup");

    const std::string cache_path = caches_dir / "test_split_total_cleanup";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path, 0, 0, false);

    FileCacheOriginInfo system_origin(FileCache::getCommonOrigin().user_id, 0, FileSegmentKeyType::System);
    auto key = DB::FileCacheKey::fromPath("split_total_cleanup_system_key");
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(system_origin), &cache_metadata);

    CacheStateGuard state_guard;

    /// Add entries only to the System sub-queue.
    {
        auto state_lock = state_guard.lock();
        priority.add(key_metadata, 0, 10, &state_lock);
        priority.add(key_metadata, 10, 10, &state_lock);
    }
    ASSERT_EQ(priority.getSize(state_guard.lock()), 20);

    /// Total-space cleanup wants to evict everything. The background thread invokes this
    /// with `getInternalOrigin()` (segment type `General`).
    EvictionInfoPtr eviction_info = priority.collectEvictionInfo(
        /* size */ 20, /* elements */ 2, /* reservee */ nullptr,
        /* is_total_space_cleanup */ true, FileCache::getInternalOrigin(), state_guard.lock());

    /// With the bug, dispatch goes to the empty data sub-queue and nothing is targeted.
    ASSERT_TRUE(eviction_info->requiresEviction());
    ASSERT_EQ(eviction_info->getSizeToEvict(), 20u);
}

TEST_F(FileCacheTest, SplitResizeCollectsSystemCandidates)
{
    /// During resize, collectEvictionInfoForResize targets both sub-queues, but
    /// collectCandidatesForEviction dispatched by origin (General -> data), so System
    /// eviction targets were ignored and the resize could not free System-held space.
    ServerUUID::setRandomForUnitTests();

    const size_t max_size = 100;
    const size_t max_elements = 100;
    SplitFileCachePriority priority(
        IFileCachePriority::QueueType::Main, makeLRUInner, max_size, max_elements,
        /* slru_size_ratio */ 0.5, /* split_cache_ratio */ 0.5,
        "test_split_resize");

    const std::string cache_path = caches_dir / "test_split_resize";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path, 0, 0, false);

    FileCacheOriginInfo system_origin(FileCache::getCommonOrigin().user_id, 0, FileSegmentKeyType::System);
    auto key = DB::FileCacheKey::fromPath("split_resize_system_key");
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(system_origin), &cache_metadata);

    CacheStateGuard state_guard;

    auto add_system_segment = [&](size_t offset, size_t size)
    {
        IFileCachePriority::IteratorPtr it;
        {
            auto state_lock = state_guard.lock();
            it = priority.add(key_metadata, offset, size, &state_lock);
        }
        auto path = cache_metadata.getFileSegmentPath(key, offset, FileSegmentKind::Regular, system_origin);
        if (std::filesystem::exists(path))
            std::filesystem::remove(path);
        std::filesystem::create_directories(std::filesystem::path(path).parent_path());
        std::string data(size, '0');
        WriteBufferFromFile wb(path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
        DB::writeString(data, wb);
        wb.finalize();
        auto file_segment = std::make_shared<FileSegment>(
            key, offset, size, FileSegment::State::DOWNLOADED,
            CreateFileSegmentSettings{}, false, nullptr, key_metadata, it);
        LockedKey(key_metadata).emplace(offset, std::make_shared<FileSegmentMetadata>(std::move(file_segment)));
        return it;
    };

    add_system_segment(0, 10);
    add_system_segment(10, 10);
    ASSERT_EQ(priority.getSize(state_guard.lock()), 20);

    /// Resize down so the System sub-queue must shed space.
    EvictionInfoPtr eviction_info;
    {
        auto state_lock = state_guard.lock();
        eviction_info = priority.collectEvictionInfoForResize(
            /* desired_max_size */ 4, /* desired_max_elements */ max_elements,
            FileCache::getInternalOrigin(), state_lock);
    }
    ASSERT_TRUE(eviction_info->requiresEviction());

    FileCacheReserveStat stat;
    EvictionCandidates evicted(IFileCachePriority::OnEvictCallback{});
    priority.collectCandidatesForEviction(
        *eviction_info, stat, evicted, /* reservee */ nullptr,
        IFileCachePriority::EvictionCursor::FromHead, /* max_candidates_size */ 0,
        /* is_total_space_cleanup */ true, FileCache::getInternalOrigin(), state_guard);

    /// With the bug, dispatch goes to the empty data sub-queue and no System candidates
    /// are collected. With the fix the System segments are collected for eviction.
    ASSERT_GT(evicted.size(), 0u);
}

TEST_F(FileCacheTest, SLRUDowngradeRollbackResetsEvictingOnSkippedFinalization)
{
    /// If the downgrade's state finalization is skipped (an exception between
    /// afterEvictWrite and afterEvictState), the rollback must reset the old protected
    /// entries from `Evicting` back to `Active` instead of leaving them stranded.
    ServerUUID::setRandomForUnitTests();

    const size_t max_size = 30;
    const size_t max_elements = 6;
    const double slru_size_ratio = 0.5; /// protected 15/3, probationary 15/3
    SLRUFileCachePriority priority(IFileCachePriority::QueueType::Main, max_size, max_elements, slru_size_ratio, "test_slru_downgrade_rollback");

    const std::string cache_path = caches_dir / "test_slru_downgrade_rollback";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path, 0, 0, false);

    const auto key = DB::FileCacheKey::fromPath("slru_downgrade_rollback_key");
    const auto & origin = FileCache::getCommonOrigin();
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(origin), &cache_metadata);

    CacheStateGuard state_guard;

    /// Fill the protected sub-queue with 3 releasable 5-byte entries (15 bytes = limit).
    std::vector<IFileCachePriority::IteratorPtr> protected_iters;
    auto add_protected_segment = [&](size_t offset, size_t size)
    {
        IFileCachePriority::IteratorPtr it;
        {
            auto state_lock = state_guard.lock();
            auto lock = priority.getPriorityGuardForTests().writeLock();
            it = priority.addForRestore(key_metadata, offset, size,
                IFileCachePriority::QueueEntryType::SLRU_Protected, lock, &state_lock);
        }
        auto path = cache_metadata.getFileSegmentPath(key, offset, FileSegmentKind::Regular, origin);
        if (std::filesystem::exists(path))
            std::filesystem::remove(path);
        std::filesystem::create_directories(std::filesystem::path(path).parent_path());
        std::string data(size, '0');
        WriteBufferFromFile wb(path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
        DB::writeString(data, wb);
        wb.finalize();
        auto file_segment = std::make_shared<FileSegment>(
            key, offset, size, FileSegment::State::DOWNLOADED,
            CreateFileSegmentSettings{}, false, nullptr, key_metadata, it);
        LockedKey(key_metadata).emplace(offset, std::make_shared<FileSegmentMetadata>(std::move(file_segment)));
        protected_iters.push_back(it);
        return it;
    };

    add_protected_segment(0, 5);
    add_protected_segment(5, 5);
    auto reservee = add_protected_segment(10, 5);
    ASSERT_EQ(priority.getProtectedSize(state_guard.lock()), 15);

    /// Reserve 5 more bytes for a protected entry: protected is full, so the eviction
    /// path must downgrade the oldest protected entry into probationary.
    EvictionInfoPtr eviction_info = priority.collectEvictionInfo(
        /* size */ 5, /* elements */ 0, reservee.get(),
        /* is_total_space_cleanup */ false, origin, state_guard.lock());
    ASSERT_TRUE(eviction_info->requiresEviction());

    FileCacheReserveStat stat;
    {
        auto evicted = std::make_unique<EvictionCandidates>(IFileCachePriority::OnEvictCallback{});
        priority.collectCandidatesForEviction(
            *eviction_info, stat, *evicted, reservee,
            IFileCachePriority::EvictionCursor::FromHead, /* max_candidates_size */ 0,
            /* is_total_space_cleanup */ false, origin, state_guard);

        /// Run only the write phase, then drop the candidates WITHOUT running the state
        /// phase -- simulating an exception between `afterEvictWrite` and `afterEvictState`.
        evicted->afterEvictWrite();
        eviction_info.reset();
        evicted.reset();
    }

    /// No protected entry must be left stuck in `Evicting`: with the bug, the downgraded
    /// entry stays `Evicting`; with the fix the rollback resets it to `Active`.
    for (const auto & it : protected_iters)
    {
        EXPECT_NE(it->getEntry()->getState(), IFileCachePriority::Entry::State::Evicting)
            << "A protected entry was left stuck in Evicting after a skipped downgrade finalization";
    }
}

TEST_F(FileCacheTest, SplitSLRUTotalSpaceCleanupSystemOnly)
{
    /// Regression for the default split-cache config (SLRU inner priorities). When only the
    /// System sub-queue has entries, total-space cleanup must not throw: the empty Data SLRU
    /// contributes no eviction info, and collectCandidatesForEviction must treat its absent
    /// queues as "nothing to collect" instead of throwing on a missing queue id. The other
    /// split tests only use LRU inners (which always register a queue id), so they miss this.
    ServerUUID::setRandomForUnitTests();

    const size_t max_size = 100;
    const size_t max_elements = 100;
    SplitFileCachePriority priority(
        IFileCachePriority::QueueType::Main, makeSLRUInner, max_size, max_elements,
        /* slru_size_ratio */ 0.5, /* split_cache_ratio */ 0.5,
        "test_split_slru_total_cleanup");

    const std::string cache_path = caches_dir / "test_split_slru_total_cleanup";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path, 0, 0, false);

    FileCacheOriginInfo system_origin(FileCache::getCommonOrigin().user_id, 0, FileSegmentKeyType::System);
    auto key = DB::FileCacheKey::fromPath("split_slru_total_cleanup_system_key");
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(system_origin), &cache_metadata);

    CacheStateGuard state_guard;

    auto add_system_segment = [&](size_t offset, size_t size)
    {
        IFileCachePriority::IteratorPtr it;
        {
            auto state_lock = state_guard.lock();
            it = priority.add(key_metadata, offset, size, &state_lock);
        }
        auto path = cache_metadata.getFileSegmentPath(key, offset, FileSegmentKind::Regular, system_origin);
        if (std::filesystem::exists(path))
            std::filesystem::remove(path);
        std::filesystem::create_directories(std::filesystem::path(path).parent_path());
        std::string data(size, '0');
        WriteBufferFromFile wb(path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
        DB::writeString(data, wb);
        wb.finalize();
        auto file_segment = std::make_shared<FileSegment>(
            key, offset, size, FileSegment::State::DOWNLOADED,
            CreateFileSegmentSettings{}, false, nullptr, key_metadata, it);
        LockedKey(key_metadata).emplace(offset, std::make_shared<FileSegmentMetadata>(std::move(file_segment)));
        return it;
    };

    /// Entries only in the System sub-queue; the Data sub-queue stays empty.
    add_system_segment(0, 10);
    add_system_segment(10, 10);
    ASSERT_EQ(priority.getSize(state_guard.lock()), 20);

    EvictionInfoPtr eviction_info = priority.collectEvictionInfo(
        /* size */ 20, /* elements */ 2, /* reservee */ nullptr,
        /* is_total_space_cleanup */ true, FileCache::getInternalOrigin(), state_guard.lock());
    ASSERT_TRUE(eviction_info->requiresEviction());

    FileCacheReserveStat stat;
    EvictionCandidates evicted(IFileCachePriority::OnEvictCallback{});
    /// Must not throw on the empty Data SLRU's absent queue ids.
    ASSERT_NO_THROW(priority.collectCandidatesForEviction(
        *eviction_info, stat, evicted, /* reservee */ nullptr,
        IFileCachePriority::EvictionCursor::FromHead, /* max_candidates_size */ 0,
        /* is_total_space_cleanup */ true, FileCache::getInternalOrigin(), state_guard));

    ASSERT_GT(evicted.size(), 0u);
}

TEST_F(FileCacheTest, PriorityQueueElementsMetrics)
{
    ServerUUID::setRandomForUnitTests();

    const auto cache_path = caches_dir / "test_queue_metrics";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path, 0, 0, false);
    const auto key = DB::FileCacheKey::fromPath("metrics_key");
    const auto & origin = FileCache::getCommonOrigin();
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(origin), &cache_metadata);

    CacheStateGuard state_guard;

    const auto elements_before = CurrentMetrics::get(CurrentMetrics::FilesystemCachePriorityQueueElements);
    const auto invalidated_before = CurrentMetrics::get(CurrentMetrics::FilesystemCacheInvalidatedElements);

    /// Only the Main queue contributes to the global gauges; the Query queue must not.
    auto run_cycle = [&](IFileCachePriority::QueueType queue_type)
    {
        const int delta = queue_type == IFileCachePriority::QueueType::Main ? 1 : 0;
        LRUFileCachePriority priority(queue_type, 100, 10);

        IFileCachePriority::IteratorPtr it;
        {
            auto state_lock = state_guard.lock();
            it = priority.add(key_metadata, 0, 10, &state_lock);
        }
        ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::FilesystemCachePriorityQueueElements), elements_before + delta);

        it->invalidate();
        ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::FilesystemCacheInvalidatedElements), invalidated_before + delta);

        it->remove();
        ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::FilesystemCachePriorityQueueElements), elements_before);
        ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::FilesystemCacheInvalidatedElements), invalidated_before);
    };

    run_cycle(IFileCachePriority::QueueType::Main);
    run_cycle(IFileCachePriority::QueueType::Query);
}

TEST_F(FileCacheTest, SLRUDowngradeMetric)
{
    ServerUUID::setRandomForUnitTests();

    /// protected = 10 bytes / 1 element, probationary = 20 bytes / 2 elements.
    SLRUFileCachePriority priority(IFileCachePriority::QueueType::Main, 30, 3, 1.0 / 3, "test_downgrade");

    const auto cache_path = caches_dir / "test_slru_downgrade";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path, 0, 0, false);
    const auto key = DB::FileCacheKey::fromPath("downgrade_key");
    const auto & origin = FileCache::getCommonOrigin();
    auto key_metadata = std::make_shared<KeyMetadata>(key, std::make_shared<const FileCacheOriginInfo>(origin), &cache_metadata);

    CacheStateGuard state_guard;

    auto add_segment = [&](size_t offset, size_t size, IFileCachePriority::QueueEntryType qtype)
    {
        IFileCachePriority::IteratorPtr it;
        {
            auto state_lock = state_guard.lock();
            auto lock = priority.getPriorityGuardForTests().writeLock();
            it = priority.addForRestore(key_metadata, offset, size, qtype, lock, &state_lock);
        }
        const auto path = cache_metadata.getFileSegmentPath(key, offset, FileSegmentKind::Regular, origin);
        /// The cache directory survives across runs and the file is opened with
        /// `O_APPEND`, so a leftover file from a previous run would double in size
        /// and fail the size check in the `FileSegment` constructor.
        if (fs::exists(path))
            fs::remove(path);
        fs::create_directories(fs::path(path).parent_path());
        WriteBufferFromFile wb(path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
        DB::writeString(std::string(size, '0'), wb);
        wb.finalize();
        auto file_segment = std::make_shared<FileSegment>(
            key, offset, size, FileSegment::State::DOWNLOADED, CreateFileSegmentSettings{}, false, nullptr, key_metadata, it);
        LockedKey(key_metadata).emplace(offset, std::make_shared<FileSegmentMetadata>(std::move(file_segment)));
        return it;
    };

    add_segment(0, 10, IFileCachePriority::QueueEntryType::SLRU_Protected);   /// fills protected
    auto prob_it = add_segment(10, 10, IFileCachePriority::QueueEntryType::SLRU_Probationary);

    auto & events = CurrentThread::getProfileEvents();
    const auto downgraded_before = events[ProfileEvents::FilesystemCacheDowngradedFileSegments];
    const auto evicted_before = events[ProfileEvents::FilesystemCacheEvictedFileSegments];

    /// Protected is full, so promoting the probationary entry downgrades (moves) the protected one, not evicts it.
    ASSERT_TRUE(priority.tryIncreasePriority(*prob_it, /* is_space_reservation_complete */true, state_guard));

    ASSERT_EQ(events[ProfileEvents::FilesystemCacheDowngradedFileSegments], downgraded_before + 1);
    ASSERT_EQ(events[ProfileEvents::FilesystemCacheEvictedFileSegments], evicted_before);
}

TEST_F(FileCacheTest, RenameToIncludeSizeInNameFailureKeepsSegmentConsistent)
{
    /// Regression: encoding the segment size in the file name (`<offset>` -> `<offset>_<size>`) is a
    /// best-effort startup optimization done from `setDownloadedUnlocked` while the segment is still
    /// `DOWNLOADING` with its downloader set. If the `rename` were allowed to throw, completion would
    /// abort before clearing the downloader, leaving the segment owned by the unwinding query (no other
    /// reader could acquire it), and `FileSegmentsHolder::reset` would hit its `chassert(false)`.
    /// Here we force the rename to fail and assert the segment still completes consistently: it becomes
    /// `DOWNLOADED`, the downloader is cleared, and the file keeps its legacy `<offset>` name.

    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("rename_size_in_name_failure");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 16;
    settings[FileCacheSetting::max_elements] = 4;
    settings[FileCacheSetting::max_file_segment_size] = 8;
    settings[FileCacheSetting::boundary_alignment] = 8;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("rename_size_in_name_failure", settings);
    cache->initialize();

    const auto & user = FileCache::getCommonOrigin();
    auto key = DB::FileCacheKey::fromPath("rename_size_in_name_failure_key");

    auto holder = cache->getOrSet(key, 0, 8, /*file_size=*/8, {}, 0, user);
    ASSERT_EQ(holder->size(), 1u);
    auto seg = *holder->begin();
    ASSERT_EQ(seg->state(), State::EMPTY);

    /// Fully download the segment but do not complete yet, so completion will trigger the rename.
    download(seg, /*complete=*/false);
    ASSERT_EQ(seg->state(), State::DOWNLOADING);
    ASSERT_EQ(seg->getDownloadedSize(), 8u);

    /// Make `fs::rename` fail: occupy the target `<offset>_<size>` name with a directory, so renaming
    /// the regular file onto it is rejected by the filesystem.
    const auto new_path = cache->getFileSegmentPath(key, 0, FileSegmentKind::Regular, user, /* size */8);
    const auto legacy_path = cache->getFileSegmentPath(key, 0, FileSegmentKind::Regular, user, /* size */std::nullopt);
    ASSERT_NE(new_path, legacy_path);
    fs::create_directories(new_path);

    /// Completion must not propagate the rename failure.
    ASSERT_NO_THROW(FileSegment::complete(FileSegmentPtr(seg), /*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false));

    /// The segment is fully completed and not stranded under a stale downloader.
    ASSERT_EQ(seg->state(), State::DOWNLOADED);
    ASSERT_TRUE(seg->getDownloader().empty());

    /// The size could not be encoded, so the file keeps its legacy `<offset>` name.
    ASSERT_FALSE(seg->hasSizeInFileName());
    ASSERT_EQ(seg->getPath(), legacy_path);
    ASSERT_TRUE(fs::is_regular_file(legacy_path));
    ASSERT_EQ(fs::file_size(legacy_path), 8u);

    /// Drop all in-memory references and destroy the first cache instance before reopening the same
    /// directory below. `FileCache::initialize` acquires an exclusive `status` lock on the cache
    /// directory (and holds it for the cache's lifetime); a second live `FileCache` on the same path
    /// would otherwise fail with "Another server instance in same directory is already running".
    /// Destroying the cache only releases the lock — it keeps the persisted files on disk — which is
    /// exactly the restart we want to model here.
    /// `holder` is a `FileSegmentsHolderPtr` (a `std::unique_ptr<FileSegmentsHolder>`) whose pointee
    /// also has a `reset` method, so a bare `holder.reset()` is an ambiguous call (flagged by
    /// `readability-ambiguous-smartptr-reset-call`); assign `nullptr` to unambiguously destroy the
    /// holder (which completes and releases its segments via `~FileSegmentsHolder`).
    holder = nullptr;
    seg.reset();
    cache.reset();

    /// Reopen the cache from disk (a real restart). The persisted state is now the real segment under
    /// its legacy `<offset>` name next to the stale `<offset>_<size>` directory. `loadMetadataForKey`
    /// must restore the segment from the legacy file and must not treat the directory as a second
    /// segment for the same offset — otherwise it hits the duplicate-offset `chassert(false)` in
    /// debug/sanitizer builds or nondeterministically deletes the real file in release.
    auto reloaded = std::make_shared<DB::FileCache>("rename_size_in_name_failure_reload", settings);
    reloaded->initialize();

    /// Exactly one segment is restored (from the legacy file); the directory artifact is ignored.
    ASSERT_EQ(reloaded->getFileSegmentsNum(), 1u);
    ASSERT_EQ(reloaded->getUsedCacheSize(), 8u);

    /// Startup kept the legacy `<offset>` file intact.
    ASSERT_TRUE(fs::is_regular_file(legacy_path));
    ASSERT_EQ(fs::file_size(legacy_path), 8u);

    /// The restored segment is fully downloaded and reusable.
    auto reloaded_holder = reloaded->getOrSet(key, 0, 8, /*file_size=*/8, {}, 0, user);
    ASSERT_EQ(reloaded_holder->size(), 1u);
    ASSERT_EQ((*reloaded_holder->begin())->state(), State::DOWNLOADED);
}

TEST_F(FileCacheTest, QueryLimitContextRevivedDuringRelease)
{
    /// Regression test: while one holder for a query_id releases its query context,
    /// another holder for the same query_id must keep it alive, and a later release must not fail
    /// with "Attempt to release query context that does not exist".

    FileCacheQueryLimit query_limit;

    const std::string query_id = "query_id_revive";
    FilesystemCacheSettings cache_settings;
    cache_settings.max_download_size_per_query = 1024;

    FileCacheQueryLimit::QueryContextPtr context1;
    FileCacheQueryLimit::QueryContextPtr context2;
    {
        auto lock = query_limit.lock();
        /// Take the context for the first time; `query_map` and `context1` reference it.
        context1 = query_limit.getOrSetQueryContext(query_id, cache_settings, lock);
        ASSERT_TRUE(context1 != nullptr);
        /// A second holder revives the same context: `getOrSetQueryContext` returns the existing entry.
        context2 = query_limit.getOrSetQueryContext(query_id, cache_settings, lock);
    }
    ASSERT_EQ(context1.get(), context2.get());
    ASSERT_EQ(context1.use_count(), 3);
    const auto * context_raw = context1.get();

    /// `~QueryContextHolder` does not use its `cache` member, so tests pass nullptr; the holders
    /// and `query_map` hold the only references once the local copies are dropped.
    auto holder1 = std::make_unique<FileCacheQueryLimit::QueryContextHolder>(
        query_id, /* cache */ nullptr, &query_limit, context1);
    auto holder2 = std::make_unique<FileCacheQueryLimit::QueryContextHolder>(
        query_id, /* cache */ nullptr, &query_limit, context2);
    context1.reset();
    context2.reset();

    /// The first holder releases. Another holder is still alive, so the entry must be kept (no erase,
    /// no throw) and enforcement preserved: the context is still discoverable.
    ASSERT_NO_THROW(holder1.reset());
    {
        DB::ThreadStatus thread_status;
        auto query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId(query_id);
        auto query_scope_holder = DB::QueryScope::create(query_context);

        ASSERT_EQ(query_limit.tryGetQueryContext().get(), context_raw);
    }

    /// The second holder is now the last one; releasing it removes the entry, once, without throwing.
    ASSERT_NO_THROW(holder2.reset());
    {
        DB::ThreadStatus thread_status;
        auto query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId(query_id);
        auto query_scope_holder = DB::QueryScope::create(query_context);

        ASSERT_EQ(query_limit.tryGetQueryContext().get(), nullptr);
    }
}

TEST_F(FileCacheTest, QueryLimitConcurrentReleaseNoLeak)
{
    /// Regression for #109508: parallel read streams create several holders for one query_id.
    /// `~QueryContextHolder` drops its own reference under `query_limit->lock()` before checking
    /// `use_count`, so two holders releasing concurrently cannot both skip the erase and orphan
    /// `query_map[query_id]`. This test releases sequentially and only pins the single-erase,
    /// no-throw outcome; the concurrent case is not reproduced here.

    FileCacheQueryLimit query_limit;

    const std::string query_id = "query_id_concurrent_release";
    FilesystemCacheSettings cache_settings;
    cache_settings.max_download_size_per_query = 1024;

    /// Two holders take the same context; query_map + both holders reference it (use_count == 3).
    FileCacheQueryLimit::QueryContextPtr context1;
    FileCacheQueryLimit::QueryContextPtr context2;
    {
        auto lock = query_limit.lock();
        context1 = query_limit.getOrSetQueryContext(query_id, cache_settings, lock);
        context2 = query_limit.getOrSetQueryContext(query_id, cache_settings, lock);
    }
    ASSERT_EQ(context1.get(), context2.get());
    ASSERT_EQ(context1.use_count(), 3);

    {
        /// `~QueryContextHolder` does not use its `cache` member, so tests pass nullptr.
        FileCacheQueryLimit::QueryContextHolder holder1(query_id, /* cache */ nullptr, &query_limit, context1);
        FileCacheQueryLimit::QueryContextHolder holder2(query_id, /* cache */ nullptr, &query_limit, context2);
        context1.reset();
        context2.reset();
        /// At scope end holder2 releases first (another holder still alive, entry kept), then holder1
        /// releases and erases the entry once. Neither throws.
    }

    /// The entry must be gone: a leaked entry (both releases skipping the erase, as described in
    /// the comment at the top of this test) would still be found by `tryGetQueryContext`.
    {
        DB::ThreadStatus thread_status;
        auto query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId(query_id);
        auto query_scope_holder = DB::QueryScope::create(query_context);

        ASSERT_EQ(query_limit.tryGetQueryContext().get(), nullptr);
    }
}

/// Concurrent readBigAt calls on AsynchronousBoundedReadBuffer over a cached buffer, with a
/// prefetch in flight: the callers must not race on consuming it.
TEST_F(FileCacheTest, CachedReadBufferConcurrentReadBigAtWithPrefetch)
{
    TestQueryScope query_scope;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

    const std::string data = makeSourceData(300);
    std::string file_path = fs::current_path() / "test_concurrent_read_big_at";
    writeSourceFile(file_path, data);

    auto read_buffer_creator = [&]() -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<FakeRemoteReadBuffer>(createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt));
    };

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_file_segment_size] = 16;
    settings[FileCacheSetting::max_size] = 1000;
    settings[FileCacheSetting::max_elements] = 100;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("concurrent_read_big_at", settings);
    cache->initialize();

    auto key = DB::FileCacheKey::fromPath(file_path);

    ThreadPoolRemoteFSReader remote_fs_reader(4, 0);

    constexpr size_t num_threads = 4;
    constexpr size_t num_iterations = 100;
    /// Smaller than the file, so the prefetch is usually still in flight when the reads run.
    constexpr size_t buffer_size = 64;

    for (size_t iteration = 0; iteration < num_iterations; ++iteration)
    {
        auto cached_buffer = std::make_unique<CachedOnDiskReadBufferFromFile>(
            file_path, key, cache, FileCache::getCommonOrigin(), read_buffer_creator,
            read_settings.filesystem_cache_settings, buffer_size, buffer_size,
            "test", data.size(), false, false, std::nullopt, nullptr);

        AsynchronousBoundedReadBuffer read_buffer(
            std::move(cached_buffer), remote_fs_reader, buffer_size,
            /* min_bytes_for_seek */ 0, Priority{0}, /* page_cache_block_size */ 0, /* enable_prefetches_log */ false);

        read_buffer.prefetch(Priority{0});

        std::atomic<size_t> ready{0};
        std::array<std::string, num_threads> errors;
        std::vector<std::thread> threads;

        for (size_t t = 0; t < num_threads; ++t)
        {
            threads.emplace_back([&, t]
            {
                /// Barrier, to maximize the chance that the readBigAt calls overlap.
                ready.fetch_add(1);
                while (ready.load() < num_threads)
                    ;

                try
                {
                    /// Threads read different, partially overlapping ranges.
                    const size_t offset = (t < 2) ? 10 * (t + 1) : 50 * t;
                    const size_t count = 150;
                    std::string buf(count, 0);
                    size_t total = 0;
                    while (total < count)
                    {
                        size_t read = read_buffer.readBigAt(buf.data() + total, count - total, offset + total, nullptr);
                        if (read == 0)
                            break;
                        total += read;
                    }
                    if (total != count)
                        errors[t] = fmt::format("short read: {} instead of {}", total, count);
                    else if (memcmp(buf.data(), data.data() + offset, count) != 0)
                        errors[t] = "read data does not match file contents";
                }
                catch (...)
                {
                    errors[t] = getCurrentExceptionMessage(true);
                }
            });
        }

        for (auto & thread : threads)
            thread.join();

        for (size_t t = 0; t < num_threads; ++t)
            ASSERT_EQ(errors[t], "") << "thread " << t << ", iteration " << iteration;
    }
}

/// Concurrent readBigAt calls on a cached buffer constructed with unknown file size: they race
/// on the lazy initialization of file_size (tryGetFileSize), which must be synchronized.
TEST_F(FileCacheTest, CachedReadBufferConcurrentReadBigAtUnknownFileSize)
{
    TestQueryScope query_scope;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;

    const std::string data = makeSourceData(300);
    std::string file_path = fs::current_path() / "test_concurrent_read_big_at_unknown_size";
    writeSourceFile(file_path, data);

    auto read_buffer_creator = [&]() -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<FakeRemoteReadBuffer>(createReadBufferFromFileBase(file_path, read_settings, std::nullopt, std::nullopt));
    };

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_file_segment_size] = 16;
    settings[FileCacheSetting::max_size] = 1000;
    settings[FileCacheSetting::max_elements] = 100;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("concurrent_read_big_at_unknown_size", settings);
    cache->initialize();

    auto key = DB::FileCacheKey::fromPath(file_path);

    constexpr size_t num_threads = 4;
    constexpr size_t num_iterations = 100;

    for (size_t iteration = 0; iteration < num_iterations; ++iteration)
    {
        /// Zero size is treated as unknown: the first readBigAt calls resolve it lazily.
        auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
            file_path, key, cache, FileCache::getCommonOrigin(), read_buffer_creator,
            read_settings.filesystem_cache_settings, DBMS_DEFAULT_BUFFER_SIZE, DBMS_DEFAULT_BUFFER_SIZE,
            "test", /* file_size */ 0, false, false, std::nullopt, nullptr);

        std::atomic<size_t> ready{0};
        std::array<std::string, num_threads> errors;
        std::vector<std::thread> threads;

        for (size_t t = 0; t < num_threads; ++t)
        {
            threads.emplace_back([&, t]
            {
                /// Barrier, to maximize the chance that the readBigAt calls overlap.
                ready.fetch_add(1);
                while (ready.load() < num_threads)
                    ;

                try
                {
                    const size_t offset = 50 * t;
                    const size_t count = 150;
                    std::string buf(count, 0);
                    size_t total = 0;
                    while (total < count)
                    {
                        size_t read = cached_buffer->readBigAt(buf.data() + total, count - total, offset + total, nullptr);
                        if (read == 0)
                            break;
                        total += read;
                    }
                    if (total != count)
                        errors[t] = fmt::format("short read: {} instead of {}", total, count);
                    else if (memcmp(buf.data(), data.data() + offset, count) != 0)
                        errors[t] = "read data does not match file contents";
                }
                catch (...)
                {
                    errors[t] = getCurrentExceptionMessage(true);
                }
            });
        }

        for (auto & thread : threads)
            thread.join();

        for (size_t t = 0; t < num_threads; ++t)
            ASSERT_EQ(errors[t], "") << "thread " << t << ", iteration " << iteration;
    }
}
