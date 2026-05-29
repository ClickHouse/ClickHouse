#include <Columns/IColumn.h>
#include <IO/copyData.h>
#include <Interpreters/FileCache/IFileCachePriority.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>


#include <algorithm>
#include <atomic>
#include <random>
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
#if CLICKHOUSE_CLOUD
#include <Interpreters/FileCache/OvercommitFileCachePriority.h>
#endif
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
#include <Common/VectorWithMemoryTracking.h>

#include <Poco/ConsoleChannel.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Interpreters/FileCache/WriteBufferToFileSegment.h>

#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Interpreters/DatabaseCatalog.h>
#include <base/scope_guard.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;
using namespace DB;

static constexpr auto TEST_LOG_LEVEL = "debug";

namespace DB::ErrorCodes
{
    extern const int FILECACHE_ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
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
}

void printRanges(const auto & segments)
{
    std::cerr << "\nHaving file segments: ";
    for (const auto & segment : segments)
        std::cerr << '\n' << segment->range().toString() << " (state: " + DB::FileSegment::stateToString(segment->state()) + ")" << "\n";
}

String getFileSegmentPath(const String & base_path, const DB::FileCache::Key & key, size_t offset)
{
    auto key_str = key.toString();
    return fs::path(base_path) / key_str.substr(0, 3) / key_str / DB::toString(offset);
}

void download(const std::string & cache_base_path, DB::FileSegment & file_segment)
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


void assertEqual(const FileSegmentsHolderPtr & file_segments, const Ranges & expected_ranges, const States & expected_states = {})
{
    std::cerr << "\nFile segments: ";
    for (const auto & file_segment : *file_segments)
        std::cerr << file_segment->range().toString() << ", ";
    std::cerr << "\nExpected: ";
    for (const auto & r : expected_ranges)
        std::cerr << r.toString() << ", ";

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

void assertEqual(const std::vector<FileSegment::Info> & file_segments, const Ranges & expected_ranges, const States & expected_states = {})
{
    std::cerr << "\nFile segments: ";
    for (const auto & file_segment : file_segments)
        std::cerr << FileSegment::Range(file_segment.range_left, file_segment.range_right).toString() << ", ";
    std::cerr << "\nExpected: ";
    for (const auto & r : expected_ranges)
        std::cerr << r.toString() << ", ";

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

void assertEqual(const IFileCachePriority::PriorityDumpPtr & dump, const Ranges & expected_ranges, const States & expected_states = {})
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

void assertProtectedOrProbationary(const std::vector<FileSegmentInfo> & file_segments, const Ranges & expected, bool assert_protected)
{
    std::cerr << "\nFile segments: ";
    std::vector<Range> res;
    for (const auto & f : file_segments)
    {
        auto range = FileSegment::Range(f.range_left, f.range_right);
        bool is_protected = (f.queue_entry_type == FileCacheQueueEntryType::SLRU_Protected);
        bool is_probationary = (f.queue_entry_type == FileCacheQueueEntryType::SLRU_Probationary);
        ASSERT_TRUE(is_probationary || is_protected);

        std::cerr << fmt::format("{} (protected: {})", range.toString(), is_protected) <<  ", ";

        if ((is_protected && assert_protected) || (!is_protected && !assert_protected))
        {
            res.push_back(range);
        }
    }
    std::cerr << "\nExpected: ";
    for (const auto & range : expected)
    {
        std::cerr << range.toString() << ", ";
    }

    ASSERT_EQ(res.size(), expected.size());
    for (size_t i = 0; i < res.size(); ++i)
    {
        ASSERT_EQ(res[i], expected[i]);
    }
}

void assertProtected(const std::vector<FileSegmentInfo> & file_segments, const Ranges & expected)
{
    std::cerr << "\nAssert protected";
    assertProtectedOrProbationary(file_segments, expected, true);
}

void assertProbationary(const std::vector<FileSegmentInfo> & file_segments, const Ranges & expected)
{
    std::cerr << "\nAssert probationary";
    assertProtectedOrProbationary(file_segments, expected, false);
}

void assertProtected(const IFileCachePriority::PriorityDumpPtr & dump, const Ranges & expected)
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

void assertProbationary(const IFileCachePriority::PriorityDumpPtr & dump, const Ranges & expected)
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

FileSegmentPtr get(const HolderPtr & holder, int i)
{
    auto it = std::next(holder->begin(), i);
    if (it == holder->end())
        std::terminate();
    return *it;
}

void download(FileSegmentPtr file_segment, bool complete = true)
{
    std::cerr << "\nDownloading range " << file_segment->range().toString() << "\n";

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

void assertDownloadFails(FileSegmentPtr file_segment)
{
    ASSERT_EQ(file_segment->getOrSetDownloader(), FileSegment::getCallerId());
    ASSERT_EQ(file_segment->getDownloadedSize(), 0);
    std::string failure_reason;
    ASSERT_FALSE(file_segment->reserve(file_segment->range().size(), 1000, failure_reason));
    FileSegment::complete(FileSegmentPtr(file_segment), /*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
}

void download(const HolderPtr & holder)
{
    for (auto & it : *holder)
    {
        download(it);
    }
}

void increasePriority(const HolderPtr & holder)
{
    for (auto & it : *holder)
        it->increasePriority();
}

void increasePriority(const HolderPtr & holder, size_t pos)
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
        std::cerr << "Step 1\n";
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

        std::cerr << "Step 2\n";

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

        std::cerr << "Step 3\n";

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

        std::cerr << "Step 4\n";

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

        std::cerr << "Step 5\n";
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

        std::cerr << "Step 6\n";

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

        std::cerr << "Step 7\n";
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

        std::cerr << "Step 8\n";
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

        std::cerr << "Step 9\n";

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

                file_segment2->wait(file_segment2->range().right);
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

        std::cerr << "Step 10\n";
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

                file_segment2->wait(file_segment2->range().left);
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

    std::cerr << "Step 11\n";
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

    std::cerr << "Step 12\n";
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

    std::cerr << "Step 13\n";
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
        ASSERT_TRUE(fs::exists(cache.getFileSegmentPath(key, 0, FileSegmentKind::Regular, user)));

        cache.removeAllReleasable(user.user_id);
        ASSERT_EQ(cache.getUsedCacheSize(), 0);
        ASSERT_TRUE(!fs::exists(key_path));
        ASSERT_TRUE(!fs::exists(cache.getFileSegmentPath(key, 0, FileSegmentKind::Regular, user)));
    }

    std::cerr << "Step 14\n";
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

            std::cerr << "\nFile segments: " << holder2->toString() << "\n";

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

    size_t size_used_with_temporary_data;
    size_t segments_used_with_temporary_data;


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
    std::cerr << getCurrentExceptionMessage(true) << std::endl;
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
            std::cerr << "Add [" << offset << ", " << offset + size - 1 << "]" << std::endl;

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
    SLRUFileCachePriority priority(max_size, max_elements, slru_size_ratio, "test_104307");

    const std::string cache_path = caches_dir / "test_slru_104307";
    fs::create_directories(cache_path);
    CacheMetadata cache_metadata(cache_path,
                                 /* background_download_queue_size_limit */0,
                                 /* background_download_threads */0,
                                 /* write_cache_per_user_directory */false);

    const auto key = DB::FileCacheKey::fromPath("104307_protected_only_key");
    const auto & origin = FileCache::getCommonOrigin();
    auto key_metadata = std::make_shared<KeyMetadata>(key, origin, &cache_metadata);

    CacheStateGuard state_guard;
    CachePriorityGuard cache_guard;

    /// Add 3 entries of 5 bytes each (15 bytes total) directly to the protected queue,
    /// leaving probationary empty. This is the precondition that used to trigger the
    /// chassert in `collectEvictionInfo`.
    {
        auto write_lock = cache_guard.writeLock();
        auto state_lock = state_guard.lock();
        priority.addForRestore(key_metadata, /* offset */0, /* size */5,
                               IFileCachePriority::QueueEntryType::SLRU_Protected,
                               write_lock, &state_lock);
        priority.addForRestore(key_metadata, /* offset */5, /* size */5,
                               IFileCachePriority::QueueEntryType::SLRU_Protected,
                               write_lock, &state_lock);
        priority.addForRestore(key_metadata, /* offset */10, /* size */5,
                               IFileCachePriority::QueueEntryType::SLRU_Protected,
                               write_lock, &state_lock);
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

    LRUFileCachePriority priority(max_size, max_elements);

    std::string cache_path = std::filesystem::path(caches_dir) / "test_eviction_pos";
    CacheMetadata cache_metadata(cache_path, 0, 0, false);

    auto key = DB::FileCacheKey::fromPath("evict_key");
    auto origin = FileCache::getCommonOrigin();

    CacheStateGuard state_guard;
    CachePriorityGuard cache_guard;
    auto key_metadata = std::make_shared<KeyMetadata>(key, origin, &cache_metadata);

    auto add_file_segment = [&](size_t offset, size_t size)
    {
        IFileCachePriority::IteratorPtr it;
        {
            auto write_lock = cache_guard.writeLock();
            auto state_lock = state_guard.lock();
            it = priority.add(key_metadata, offset, size, write_lock, &state_lock);
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
    ASSERT_EQ(priority.getEvictionPosCount(), 2); /// queue.end()

    FileCacheReserveStat stat;
    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;
    auto evicted = std::make_unique<EvictionCandidates>();

    auto eviction_info = priority.collectEvictionInfo(10, 1, nullptr, false, origin, state_guard.lock());
    priority.collectCandidatesForEviction(*eviction_info, stat, *evicted, invalidated_entries, nullptr, true, 0, false, origin, cache_guard, state_guard);
    eviction_info.reset();

    ASSERT_EQ(evicted->size(), 0); /// Nothing is evicted.
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 2);
    ASSERT_EQ(priority.getEvictionPosCount(), 2); /// queue.end()

    auto it3 = add_file_segment(20, 10);

    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 3);
    ASSERT_EQ(priority.getEvictionPosCount(), 3); /// queue.end()

    evicted = std::make_unique<EvictionCandidates>();
    stat = {};
    eviction_info = priority.collectEvictionInfo(10, 1, nullptr, false, origin, state_guard.lock());
    priority.collectCandidatesForEviction(*eviction_info, stat, *evicted, invalidated_entries, nullptr, true, 0, false, origin, cache_guard, state_guard);

    ASSERT_EQ(evicted->size(), 1);
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 3);
    ASSERT_EQ(priority.getEvictionPosCount(), 0); /// queue.begin()

    {
        evicted->evict();
        evicted->afterEvictState(state_guard.lock());
        evicted->afterEvictWrite(cache_guard.writeLock());
        IFileCachePriority::removeEntries(invalidated_entries, cache_guard.writeLock());
        evicted.reset();
    }
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 2);
    ASSERT_EQ(priority.getEvictionPosCount(), 0); /// still queue.begin(), but it2

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
    ASSERT_EQ(priority.getEvictionPosCount(), 0);

    evicted = std::make_unique<EvictionCandidates>();
    stat = {};
    eviction_info = priority.collectEvictionInfo(10, 1, nullptr, false, origin, state_guard.lock());
    priority.collectCandidatesForEviction(*eviction_info, stat, *evicted, invalidated_entries, nullptr, true, 0, false, origin, cache_guard, state_guard);

    ASSERT_EQ(evicted->size(), 1);
    ASSERT_EQ(priority.getElementsCount(state_guard.lock()), 3);
    ASSERT_EQ(priority.getEvictionPosCount(), 3); /// 3 and not 2, because 1 entry is invalidated.

    fs2.reset();
    fs3.reset();

    priority.resetEvictionPos();
    ASSERT_EQ(priority.getEvictionPosCount(), 0); /// queue.begin()
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

/// ----- ReaderExecutor + DiskCacheProvider tests -----

#include <IO/BufferSourceReader.h>
#include <IO/DiskCacheProvider.h>
#include <IO/LocalSourceReader.h>
#include <IO/ReaderExecutor.h>
#include <IO/PipelineReadBuffer.h>
#include <IO/Rope.h>

TEST_F(FileCacheTest, DiskCacheProviderReadPopulatesCache)
{
    ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_file_segment_size] = 10;
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 20;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("dc_provider_1", settings);
    cache->initialize();

    /// Write a 30-byte test file.
    std::string file_path = fs::current_path() / "test_dc_provider";
    std::string data(30, 'A');
    {
        auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
        wb->write(data.data(), data.size());
        wb->next();
        wb->finalize();
    }
    SCOPE_EXIT({ fs::remove(file_path); });

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    auto disk_cache_provider = std::make_shared<DiskCacheProvider>(cache, cache_settings);
    auto source_reader = std::make_shared<LocalSourceReader>();

    StoredObjects objects;
    objects.emplace_back(file_path, "", data.size());

    /// First read: cache miss, populates cache.
    {
        auto executor = std::make_unique<ReaderExecutor>(
            source_reader, objects,
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{disk_cache_provider},
            /*window_size=*/30,
            /*min_bytes_for_seek=*/0,
            /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
            file_path);

        PipelineReadBuffer buf(std::move(executor));
        WriteBufferFromOwnString result;
        copyData(buf, result);
        ASSERT_EQ(result.str(), data);
    }

    /// Verify cache segments are populated (30 bytes / 10 segment size = 3 segments).
    assertEqual(cache->dumpQueue(), {FileSegment::Range(0, 9), FileSegment::Range(10, 19), FileSegment::Range(20, 29)});

    /// Second read: should hit cache. Use a broken source to prove data comes from cache.
    auto broken_source = std::make_shared<BufferSourceReader>(
        [](const StoredObject &) -> std::unique_ptr<ReadBufferFromFileBase>
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Source should not be called on cache hit");
        },
        "BrokenSource");

    {
        auto executor = std::make_unique<ReaderExecutor>(
            broken_source, objects,
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{disk_cache_provider},
            /*window_size=*/30,
            /*min_bytes_for_seek=*/0,
            /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
            file_path);

        PipelineReadBuffer buf(std::move(executor));
        WriteBufferFromOwnString result;
        copyData(buf, result);
        ASSERT_EQ(result.str(), data);
    }
}

TEST_F(FileCacheTest, DiskCacheProviderHonoursFullRangeWhenBatchSizeIsOne)
{
    /// Regression: with `filesystem_cache_segments_batch_size = 1`, an earlier
    /// version of `DiskCacheHandle` forwarded that limit to `FileCache::getOrSet`
    /// and only saw the FIRST segment of the requested range. `status()` then
    /// under-reported misses and `ReaderExecutor` returned short data (off-by-one
    /// row in `00009_uniq_distributed` / `00060_move_to_prewhere_and_sets`,
    /// 41/41 reproducibility). The provider must always get full coverage.
    ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_file_segment_size] = 10;  /// → 3 segments for the 30-byte file
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 20;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("dc_provider_batch_1", settings);
    cache->initialize();

    const std::string file_path = fs::current_path() / "test_dc_provider_batch_1";
    const std::string data(30, 'Z');
    {
        auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
        wb->write(data.data(), data.size());
        wb->next();
        wb->finalize();
    }
    SCOPE_EXIT({ fs::remove(file_path); });

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
    /// The trigger: tells the provider it may only see ONE segment per call.
    /// The provider must ignore this (it is a one-shot lookup, not a streaming
    /// reader); otherwise the read returns only the first 10 bytes.
    cache_settings.segments_batch_size = 1;

    auto provider = std::make_shared<DiskCacheProvider>(cache, cache_settings);
    auto source_reader = std::make_shared<LocalSourceReader>();

    StoredObjects objects;
    objects.emplace_back(file_path, "", data.size());

    auto executor = std::make_unique<ReaderExecutor>(
        source_reader, objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{provider},
        /*window_size=*/30,
        /*min_bytes_for_seek=*/0,
        /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
        file_path);

    PipelineReadBuffer buf(std::move(executor));
    WriteBufferFromOwnString result;
    copyData(buf, result);
    ASSERT_EQ(result.str(), data);

    /// All 3 segments must end up populated, not just the first.
    assertEqual(
        cache->dumpQueue(),
        {FileSegment::Range(0, 9), FileSegment::Range(10, 19), FileSegment::Range(20, 29)});
}

TEST_F(FileCacheTest, DiskCacheProviderPartialRead)
{
    ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path2;
    settings[FileCacheSetting::max_file_segment_size] = 10;
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 20;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("dc_provider_2", settings);
    cache->initialize();

    /// Write a 30-byte test file with distinct content per segment.
    std::string file_path = fs::current_path() / "test_dc_provider_partial";
    std::string data = "AAAAAAAAAA" "BBBBBBBBBB" "CCCCCCCCCC";
    {
        auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
        wb->write(data.data(), data.size());
        wb->next();
        wb->finalize();
    }
    SCOPE_EXIT({ fs::remove(file_path); });

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    auto disk_cache_provider = std::make_shared<DiskCacheProvider>(cache, cache_settings);
    auto source_reader = std::make_shared<LocalSourceReader>();

    StoredObjects objects;
    objects.emplace_back(file_path, "", data.size());

    /// Read with small window to exercise multiple readNextWindow calls.
    {
        auto executor = std::make_unique<ReaderExecutor>(
            source_reader, objects,
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{disk_cache_provider},
            /*window_size=*/10,
            /*min_bytes_for_seek=*/0,
            /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
            file_path);

        PipelineReadBuffer buf(std::move(executor));
        WriteBufferFromOwnString result;
        copyData(buf, result);
        ASSERT_EQ(result.str(), data);
    }

    assertEqual(cache->dumpQueue(), {FileSegment::Range(0, 9), FileSegment::Range(10, 19), FileSegment::Range(20, 29)});

    /// Seek read: read only the middle segment.
    {
        auto executor = std::make_unique<ReaderExecutor>(
            source_reader, objects,
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{disk_cache_provider},
            /*window_size=*/10,
            /*min_bytes_for_seek=*/0,
            /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
            file_path);

        PipelineReadBuffer buf(std::move(executor));
        buf.seek(10, SEEK_SET);

        char tmp[10];
        size_t n = buf.read(tmp, 10);
        ASSERT_EQ(n, 10u);
        ASSERT_EQ(std::string(tmp, 10), "BBBBBBBBBB");
    }
}

/// Full B behaviour at the handle level: when `put` is called with a `Rope`
/// that does not fully cover the segment range (a gap somewhere after the
/// segment's `getCurrentWriteOffset`), the handle must:
///   - write only the contiguous prefix that the rope covers,
///   - leave the segment PARTIALLY_DOWNLOADED with `downloaded_size` matching
///     the prefix (not silently pad with zeros to seg.size),
///   - report the prefix as a hit on the next `status()` call,
///   - serve the prefix bytes via `get()`.
///
/// Without these properties, a small-file workload (file_size < segment_size)
/// — or any cache miss whose underlying read is partial — would either poison
/// the cache with zero bytes (pre-fix behaviour) or never cache the file at
/// all (Option A behaviour).
TEST_F(FileCacheTest, DiskCacheProviderPartialPutSegmentIsCacheable)
{
    ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path2;
    settings[FileCacheSetting::max_file_segment_size] = 10;
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 20;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("dc_partial_put", settings);
    cache->initialize();

    /// Use an object whose declared size is 10 — FileCache will allocate a
    /// full 10-byte segment for the [0, 10) range — but only put 5 bytes
    /// into it via the rope. This is the exact shape Full B targets.
    const std::string object_path = "/synthetic/path/partial_put_object";
    const size_t object_size = 10;

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    auto provider = std::make_shared<DiskCacheProvider>(cache, cache_settings);

    StoredObject object{object_path, "", object_size};

    /// Build a rope covering only [0, 5) — the first half of the segment.
    /// The rope's logical_offset is file-level, but since object_file_offset
    /// is 0 in this test, file-level == object-local.
    auto buf = std::make_shared<OwnedRopeBuffer>(5);
    std::memcpy(buf->data(), "HELLO", 5);
    Rope rope_to_put;
    rope_to_put.append(RopeNode{buf, 0, 5, 0});

    auto handle = provider->lookup(object, /*object_file_offset=*/0, ByteRange{0, 10});
    ASSERT_NE(handle, nullptr);

    /// The whole segment is initially a miss.
    {
        auto initial = handle->status();
        ASSERT_TRUE(initial.hit_ranges.empty());
        ASSERT_EQ(initial.miss_ranges.size(), 1u);
        ASSERT_EQ(initial.miss_ranges[0].offset, 0u);
        ASSERT_EQ(initial.miss_ranges[0].size, 10u);
    }

    /// Put the partial rope. Must write 5 bytes (the contiguous prefix);
    /// the 5-byte tail of the segment must NOT be zero-padded on disk.
    size_t written = handle->put(ByteRange{0, 10}, std::move(rope_to_put));
    ASSERT_EQ(written, 5u);

    /// Release the holder so the segment finalizes — the holder destructor
    /// triggers `FileSegment::complete`, which can split a partial segment
    /// into a fully-downloaded prefix + empty tail. The exact internal
    /// shape doesn't matter for the contract; what matters is what
    /// subsequent `status()` and `get()` calls report.
    handle.reset();

    /// Re-acquire a handle and verify the public contract: the prefix is
    /// a hit, the tail is a miss.
    auto handle2 = provider->lookup(object, /*object_file_offset=*/0, ByteRange{0, 10});
    auto after_put = handle2->status();

    /// Aggregate hits / misses (FileCache may split into multiple segments).
    auto total_size = [](const auto & rs)
    {
        size_t s = 0;
        for (const auto & r : rs)
            s += r.size;
        return s;
    };
    ASSERT_EQ(total_size(after_put.hit_ranges), 5u);
    ASSERT_EQ(total_size(after_put.miss_ranges), 5u);
    /// And the hit range starts at 0 (i.e. the prefix, not some random
    /// reshuffled offset).
    ASSERT_FALSE(after_put.hit_ranges.empty());
    ASSERT_EQ(after_put.hit_ranges[0].offset, 0u);

    /// `get()` must return the real 5 bytes "HELLO" — not zero-padded —
    /// and must not return anything past offset 5.
    Rope served = handle2->get(ByteRange{0, 10});
    ASSERT_TRUE(served.covers(ByteRange{0, 5}));
    ASSERT_FALSE(served.covers(ByteRange{5, 5}));
    char buf_out[5] = {};
    served.copyTo(buf_out, ByteRange{0, 5});
    ASSERT_EQ(std::string(buf_out, 5), "HELLO");
}

/// `status()` clamps miss tails to the requested range and skips miss
/// emission when the cached prefix already covers the request. The miss
/// HEAD is intentionally left at the segment boundary so source overread
/// fills the segment's existing prefix.
TEST_F(FileCacheTest, DiskCacheProviderStatusClampsMissTailToRequest)
{
    ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path3;
    settings[FileCacheSetting::max_file_segment_size] = 10;
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 20;
    /// Align segments to 10-byte boundaries so a request that starts
    /// mid-segment exercises the "miss head extends back to seg.start"
    /// behaviour (Case 4).
    settings[FileCacheSetting::boundary_alignment] = 10;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("dc_status_clamp", settings);
    cache->initialize();

    const std::string object_path = "/synthetic/path/status_clamp_object";
    const size_t object_size = 30;

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    auto provider = std::make_shared<DiskCacheProvider>(cache, cache_settings);
    StoredObject object{object_path, "", object_size};

    /// Populate first segment [0, 10) with 5 bytes (partial fill, cwo=5).
    {
        auto buf = std::make_shared<OwnedRopeBuffer>(5);
        std::memcpy(buf->data(), "HELLO", 5);
        Rope rope;
        rope.append(RopeNode{buf, 0, 5, 0});
        auto h = provider->lookup(object, /*object_file_offset=*/0, ByteRange{0, 10});
        ASSERT_NE(h, nullptr);
        h->put(ByteRange{0, 10}, std::move(rope));
    }

    auto total_size = [](const auto & rs)
    {
        size_t s = 0;
        for (const auto & r : rs) s += r.size;
        return s;
    };

    /// Case 1: cached prefix `[0, 5)` fully covers request `[0, 3)` ⇒
    /// only hit, no miss.
    {
        auto h = provider->lookup(object, 0, ByteRange{0, 3});
        ASSERT_NE(h, nullptr);
        auto s = h->status();
        EXPECT_TRUE(s.miss_ranges.empty())
            << "miss should be empty when cached prefix covers request";
        EXPECT_GE(total_size(s.hit_ranges), 3u);
    }

    /// Case 2: request `[0, 8)` extends past cwo=5 within the same segment ⇒
    /// miss tail clamped to req.end=8 (no fetching past the request).
    {
        auto h = provider->lookup(object, 0, ByteRange{0, 8});
        ASSERT_NE(h, nullptr);
        auto s = h->status();
        ASSERT_FALSE(s.miss_ranges.empty());
        for (const auto & r : s.miss_ranges)
            EXPECT_LE(r.end(), 8u) << "miss tail must not extend past req.end";
    }

    /// Case 3: cold segment `[10, 20)`, request `[10, 13)` ⇒ miss tail
    /// clamped to req.end=13. Head naturally at the segment boundary.
    {
        auto h = provider->lookup(object, 0, ByteRange{10, 3});
        ASSERT_NE(h, nullptr);
        auto s = h->status();
        ASSERT_FALSE(s.miss_ranges.empty());
        for (const auto & r : s.miss_ranges)
            EXPECT_LE(r.end(), 13u) << "miss tail must not extend past req.end";
    }

    /// Case 4: cold segment `[10, 20)`, request `[12, 15)` starts mid-segment ⇒
    /// miss HEAD is NOT cut — it extends back to seg.start=10 so the
    /// source overread can fill the segment prefix.
    {
        auto h = provider->lookup(object, 0, ByteRange{12, 3});
        ASSERT_NE(h, nullptr);
        auto s = h->status();
        ASSERT_FALSE(s.miss_ranges.empty());
        EXPECT_EQ(s.miss_ranges.front().offset, 10u)
            << "miss head must NOT be cut — should extend back to seg.start";
        for (const auto & r : s.miss_ranges)
            EXPECT_LE(r.end(), 15u) << "miss tail must not extend past req.end";
    }

    /// Case 5: partial segment `[0, 10)` cwo=5, request `[6, 9)` starts past
    /// cwo ⇒ the cached-prefix hit `[0, 5)` is reported segment-aligned
    /// (executor clamps it to its window, where it intersects empty);
    /// the meaningful payload is the miss starting at cwo=5 (head not
    /// cut, tail clamped to 9).
    {
        auto h = provider->lookup(object, 0, ByteRange{6, 3});
        ASSERT_NE(h, nullptr);
        auto s = h->status();
        for (const auto & r : s.hit_ranges)
            EXPECT_LE(r.end(), 5u) << "any hit must be within the cached prefix";
        ASSERT_FALSE(s.miss_ranges.empty());
        EXPECT_EQ(s.miss_ranges.front().offset, 5u)
            << "miss head must extend back to cwo";
        for (const auto & r : s.miss_ranges)
            EXPECT_LE(r.end(), 9u) << "miss tail must not extend past req.end";
    }

    /// Case 6: request `[3, 18)` spans cached prefix of partial seg `[0, 10)`
    /// + cold seg `[10, 20)` ⇒ hit for the prefix (segment-aligned, may
    /// extend back to seg.start=0), misses for the partial tail and the
    /// cold segment, both tail-clamped to req.end=18.
    {
        auto h = provider->lookup(object, 0, ByteRange{3, 15});
        ASSERT_NE(h, nullptr);
        auto s = h->status();
        EXPECT_FALSE(s.hit_ranges.empty());
        for (const auto & r : s.hit_ranges)
            EXPECT_LE(r.end(), 10u) << "hit must be within the partial segment";
        ASSERT_FALSE(s.miss_ranges.empty());
        for (const auto & r : s.miss_ranges)
            EXPECT_LE(r.end(), 18u) << "miss tail must not extend past req.end";
    }

    /// Case 7: a fully `DOWNLOADED` segment. Hit may exceed the request
    /// (segment-aligned by design); no miss should be emitted.
    {
        /// Populate `[20, 30)` fully.
        auto buf = std::make_shared<OwnedRopeBuffer>(10);
        std::memset(buf->data(), 'Z', 10);
        Rope rope;
        rope.append(RopeNode{buf, 0, 10, 20});
        auto h_put = provider->lookup(object, 0, ByteRange{20, 10});
        ASSERT_NE(h_put, nullptr);
        h_put->put(ByteRange{20, 10}, std::move(rope));
        h_put.reset();

        auto h = provider->lookup(object, 0, ByteRange{22, 4});
        ASSERT_NE(h, nullptr);
        auto s = h->status();
        EXPECT_TRUE(s.miss_ranges.empty())
            << "no miss for a fully downloaded segment containing the request";
        EXPECT_GE(total_size(s.hit_ranges), 4u);
    }
}

TEST_F(FileCacheTest, DiskCacheProviderGetReadsCommittedPrefixWhileDownloading)
{
    /// Regression: `status()` reports a `PARTIALLY_DOWNLOADED` segment's
    /// prefix as a hit, but if another reader becomes downloader between
    /// `status()` and `get()`, the segment transitions to `DOWNLOADING`
    /// and the pre-fix `get()` skipped it — returning no bytes for what
    /// `status()` had promised. The committed prefix is on disk and must
    /// remain readable while another thread is downloading further bytes.
    ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path2;
    settings[FileCacheSetting::max_file_segment_size] = 10;
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 20;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("dc_dl_prefix", settings);
    cache->initialize();

    const auto & user = FileCache::getCommonOrigin();
    auto key = DB::FileCacheKey::fromPath("dc_dl_prefix_key");

    /// Stage 1: write 5 of 10 bytes ("HELLO") and release the downloader
    /// so the segment is in `PARTIALLY_DOWNLOADED`. Keep `setup_holder`
    /// alive for the rest of the test — letting it destruct here would
    /// run `FileSegment::complete` and split the segment into a
    /// fully-downloaded prefix + a fresh empty tail, which the bug
    /// scenario doesn't apply to (DOWNLOADED can't transition to
    /// DOWNLOADING).
    auto setup_holder = cache->getOrSet(key, 0, 10, /*file_size=*/10, {}, 0, user);
    ASSERT_EQ(setup_holder->size(), 1u);
    auto setup_seg = *setup_holder->begin();
    ASSERT_EQ(setup_seg->state(), FileSegment::State::EMPTY);

    ASSERT_EQ(setup_seg->getOrSetDownloader(), FileSegment::getCallerId());
    std::string failure_reason;
    ASSERT_TRUE(setup_seg->reserve(/*size_to_reserve=*/10, /*lock_wait_timeout_milliseconds=*/1000, failure_reason));

    auto key_str = key.toString();
    auto subdir = fs::path(cache_base_path2) / key_str.substr(0, 3) / key_str;
    if (!fs::exists(subdir))
        fs::create_directories(subdir);
    char data[] = "HELLO";
    setup_seg->write(data, 5, setup_seg->getCurrentWriteOffset());
    setup_seg->completePartAndResetDownloader();
    ASSERT_EQ(setup_seg->state(), FileSegment::State::PARTIALLY_DOWNLOADED);
    ASSERT_EQ(setup_seg->getDownloadedSize(), 5u);

    /// Stage 2: transition the segment into `DOWNLOADING` by becoming its
    /// downloader from a separate holder. The same `FileSegment` instance
    /// (pinned by `setup_holder`) is shared across both holders.
    auto downloader_holder = cache->getOrSet(key, 0, 10, /*file_size=*/10, {}, 0, user);
    ASSERT_EQ(downloader_holder->size(), 1u);
    auto downloader_seg = *downloader_holder->begin();
    ASSERT_EQ(downloader_seg.get(), setup_seg.get());
    ASSERT_EQ(downloader_seg->getOrSetDownloader(), FileSegment::getCallerId());
    ASSERT_EQ(downloader_seg->state(), FileSegment::State::DOWNLOADING);
    /// No explicit `completePartAndResetDownloader` cleanup: the
    /// `DiskCacheHandle` destructor below holds the same caller-id and
    /// will clear the downloader claim as it walks its own holder.

    /// Stage 3: a fresh `DiskCacheHandle` for the same range. Its own
    /// holder pins the same segment; `get()` must read the committed
    /// 5-byte prefix from disk despite the `DOWNLOADING` state.
    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
    auto provider = std::make_shared<DiskCacheProvider>(
        cache, cache_settings, /*query_id_=*/String{},
        /*local_throttler_=*/nullptr,
        /*cache_log_=*/nullptr,
        /*custom_cache_key_=*/std::optional<FileCacheKey>(key),
        /*custom_origin_=*/std::nullopt);

    StoredObject object{"dc_dl_prefix_obj", "", 10};
    auto handle = provider->lookup(object, /*object_file_offset=*/0, ByteRange{0, 10});
    ASSERT_NE(handle, nullptr);

    Rope served = handle->get(ByteRange{0, 5});
    ASSERT_EQ(served.totalBytes(), 5u)
        << "DOWNLOADING segment's committed prefix must be readable";
    char buf_out[5] = {};
    served.copyTo(buf_out, ByteRange{0, 5});
    ASSERT_EQ(std::string(buf_out, 5), "HELLO");
}

/// `DiskCacheHandle::pinSegmentAt` returns a bare `FileSegmentPtr` for a
/// `PARTIALLY_DOWNLOADED` segment and null otherwise. The bare ref makes the
/// segment non-releasable (`releasable()` == `isSharedPtrUnique`), so eviction
/// pressure from another key cannot evict the pinned partial prefix.
TEST_F(FileCacheTest, DiskCacheHandleStatusCreditsDownloadingPrefix)
{
    /// While one reader is mid-download of a segment (state DOWNLOADING, with a
    /// committed prefix), a second reader's `status` must credit that prefix as
    /// a hit and miss only the tail — mirroring what `get` already serves — so
    /// the concurrent reader reads the committed prefix from the cache instead
    /// of re-fetching it from the source.
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("disk_cache_downloading_status");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 1024;
    settings[FileCacheSetting::max_elements] = 5;
    settings[FileCacheSetting::max_file_segment_size] = 20;
    settings[FileCacheSetting::boundary_alignment] = 20;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("disk_cache_downloading_status", settings);
    cache->initialize();

    const auto & origin = FileCache::getCommonOrigin();
    auto key = DB::FileCacheKey::fromPath("downloading_object_key");

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
    auto provider = std::make_shared<DiskCacheProvider>(
        cache, cache_settings, /*query_id_=*/String{},
        /*local_throttler_=*/nullptr,
        /*cache_log_=*/nullptr,
        /*custom_cache_key_=*/std::optional<FileCacheKey>(key),
        /*custom_origin_=*/std::nullopt);

    constexpr size_t file_size = 20;
    constexpr size_t prefix = 8;

    /// Become the downloader and write a committed prefix WITHOUT completing,
    /// leaving the segment DOWNLOADING with current-write-offset == prefix.
    auto dl_holder = cache->getOrSet(key, 0, file_size, file_size, DB::CreateFileSegmentSettings{}, 0, origin);
    auto segment = dl_holder->getSingleFileSegment();
    ASSERT_EQ(segment->getOrSetDownloader(), DB::FileSegment::getCallerId());
    std::string failure_reason;
    ASSERT_TRUE(segment->reserve(prefix, 1000, failure_reason));
    std::string payload(prefix, 'X');
    segment->write(payload.data(), prefix, segment->getCurrentWriteOffset());
    ASSERT_EQ(segment->state(), DB::FileSegment::State::DOWNLOADING);
    ASSERT_EQ(segment->getCurrentWriteOffset(), prefix);

    /// A concurrent reader's status over the same segment.
    StoredObject object{"downloading_object", "", file_size};
    auto handle = provider->lookup(object, /*object_file_offset=*/0, ByteRange{0, file_size});
    auto status = handle->status();

    bool prefix_hit = false;
    for (const auto & h : status.hit_ranges)
        if (h.offset == 0 && h.size == prefix)
            prefix_hit = true;
    EXPECT_TRUE(prefix_hit) << "status must credit the DOWNLOADING committed prefix [0, " << prefix << ") as a hit";

    bool tail_miss = false;
    for (const auto & m : status.miss_ranges)
        if (m.offset == prefix && m.end() == file_size)
            tail_miss = true;
    EXPECT_TRUE(tail_miss) << "status must miss only the tail past the committed prefix";

    segment->completePartAndResetDownloader();
}

TEST_F(FileCacheTest, DiskCacheHandlePinSurvivesEviction)
{
    ServerUUID::setRandomForUnitTests();
    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("disk_cache_handle_pin");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_size] = 30;          /// tiny: forces eviction
    settings[FileCacheSetting::max_elements] = 5;
    settings[FileCacheSetting::max_file_segment_size] = 10;
    /// Alignment == segment size so a partially-filled segment is NOT shrunk to
    /// its downloaded size on `complete` (which would flip it to DOWNLOADED);
    /// it stays PARTIALLY_DOWNLOADED across the full [0, 10) range — the state
    /// the pin protects.
    settings[FileCacheSetting::boundary_alignment] = 10;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("disk_cache_handle_pin", settings);
    cache->initialize();

    const auto & user = FileCache::getCommonOrigin();
    auto key = DB::FileCacheKey::fromPath("pin_object_key");

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
    auto provider = std::make_shared<DiskCacheProvider>(
        cache, cache_settings, /*query_id_=*/String{},
        /*local_throttler_=*/nullptr,
        /*cache_log_=*/nullptr,
        /*custom_cache_key_=*/std::optional<FileCacheKey>(key),
        /*custom_origin_=*/std::nullopt);

    /// `object_size` is 20 so a fresh lookup spans the partial segment [0, 10)
    /// and an EMPTY segment [10, 20).
    StoredObject object{"pin_object", "", /*bytes_size=*/20};

    ICacheHandle::CacheSegmentPin pin;
    {
        auto handle = provider->lookup(object, /*object_file_offset=*/0, ByteRange{0, 20});
        ASSERT_NE(handle, nullptr);
        ASSERT_FALSE(handle->status().miss_ranges.empty());

        /// Fill segment [0, 10) to 5 of 10 bytes THROUGH the handle's own
        /// `put` — the executor's real flow. `put` pops every written segment
        /// from this handle's holder (`completeAndPopFront`), so after this
        /// call the holder is empty: a holder-based `pinSegmentAt` would find
        /// nothing. The fix re-fetches the segment read-only instead.
        auto buf = std::make_shared<DB::OwnedRopeBuffer>(5);
        std::memset(buf->data(), 'X', 5);
        DB::Rope data;
        data.append(DB::RopeNode{buf, 0, 5, 0});
        ASSERT_EQ(handle->put(ByteRange{0, 10}, std::move(data)), 5u);

        /// Assertion 3 (the bug catcher): the just-filled partial segment must
        /// be pinnable on the SAME handle whose `put` consumed the holder.
        pin = handle->pinSegmentAt(/*file_offset=*/2);
        ASSERT_TRUE(pin != nullptr)
            << "pinSegmentAt must find the partial segment after put consumed the holder";

        /// Assertion 4: an offset outside this handle's object yields null —
        /// guards the executor calling pinSegmentAt on another object's handle.
        EXPECT_EQ(handle->pinSegmentAt(/*file_offset=*/50), nullptr)
            << "out-of-object offset must not be pinnable";
    }   /// handle (and its holder) destroyed here; only `pin` holds the segment.

    /// Eviction pressure from another key: download enough full segments to
    /// overflow `max_size`. The unpinned segments may be evicted; the pinned
    /// partial prefix must survive because the bare ref keeps it non-releasable.
    auto flood_key = DB::FileCacheKey::fromPath("flood_key");
    for (size_t off = 0; off < 60; off += 10)
    {
        auto h = cache->getOrSet(flood_key, off, 10, /*file_size=*/60, {}, 0, user);
        for (auto & seg : *h)
        {
            if (seg->state() != FileSegment::State::EMPTY)
                continue;
            if (seg->getOrSetDownloader() != FileSegment::getCallerId())
                continue;
            std::string failure_reason;
            if (!seg->reserve(seg->range().size(), 1000, failure_reason))
            {
                seg->completePartAndResetDownloader();
                continue;
            }
            download(cache_base_path, *seg);
            FileSegment::complete(FileSegmentPtr(seg), /*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
        }
    }

    /// The flood (60 bytes against `max_size` 30) must have overflowed the
    /// cache, so eviction necessarily ran — this is what makes assertion 5
    /// non-vacuous (an un-pinned segment would have been a candidate).
    ASSERT_LE(cache->getUsedCacheSize(), 30u);

    /// Assertion 5: the pinned partial segment's prefix is still a hit.
    {
        auto handle = provider->lookup(object, 0, ByteRange{0, 20});
        auto status = handle->status();
        bool has_hit_at_zero = false;
        for (const auto & hit : status.hit_ranges)
            if (hit.offset == 0 && hit.size >= 5)
                has_hit_at_zero = true;
        EXPECT_TRUE(has_hit_at_zero) << "pinned partial segment was evicted";
    }

    /// After releasing the pin the segment becomes evictable again.
    pin.reset();
    SUCCEED();
}

/// End-to-end version of the partial-segment scenario, exercising the
/// full unknown-size path through `ReaderExecutor` + `DiskCacheProvider`.
///
/// With `StoredObject::UnknownSize`, `OffsetMap` cannot clamp fetch ranges
/// to the file's true size, so the source EOFs mid-segment (5 of 10 bytes
/// delivered). Before Full B, the executor's `put` would have either
/// zero-padded the segment (silent cache poisoning) or thrown on the
/// `PARTIALLY_DOWNLOADED` continuation. After Full B, the partial fill
/// is committed correctly and the second read serves the real bytes
/// from cache.
TEST_F(FileCacheTest, DiskCacheProviderUnknownSizeShortReadIsCacheable)
{
    ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_file_segment_size] = 10;
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 20;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("dc_unknown_size", settings);
    cache->initialize();

    /// Tiny file — 5 bytes, less than one segment. Real on-disk file so
    /// the source delivers the real content; the executor's view of it
    /// is `UnknownSize`, so it can't pre-clamp the read range.
    std::string file_path = fs::current_path() / "test_dc_unknown_size";
    std::string data = "HELLO";
    {
        auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
        wb->write(data.data(), data.size());
        wb->next();
        wb->finalize();
    }
    SCOPE_EXIT({ fs::remove(file_path); });

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    auto disk_cache_provider = std::make_shared<DiskCacheProvider>(cache, cache_settings);

    /// Counting source factory — to confirm the second read gets the
    /// prefix from cache rather than re-fetching it.
    auto source_open_count = std::make_shared<std::atomic<size_t>>(0);
    auto source = std::make_shared<BufferSourceReader>(
        [file_path, source_open_count](const StoredObject &) -> std::unique_ptr<ReadBufferFromFileBase>
        {
            source_open_count->fetch_add(1);
            return std::make_unique<ReadBufferFromFile>(file_path);
        },
        "CountingSource");

    StoredObjects objects;
    /// UnknownSize forces the executor through the unknown-size code path.
    objects.emplace_back(file_path, "", StoredObject::UnknownSize);

    /// First read: source delivers 5 bytes, EOF latched, cache populated
    /// with a partial segment. The executor's new contiguity check would
    /// throw LOGICAL_ERROR if the assembled rope had a hole here.
    {
        auto executor = std::make_unique<ReaderExecutor>(
            source, objects,
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{disk_cache_provider},
            /*window_size=*/20,
            /*min_bytes_for_seek=*/0,
            /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
            file_path);

        PipelineReadBuffer buf(std::move(executor));
        WriteBufferFromOwnString result;
        copyData(buf, result);
        ASSERT_EQ(result.str(), data);
    }
    ASSERT_GE(source_open_count->load(), 1u);

    /// Second read: a fresh executor with the same source. The prefix
    /// `[0, 5)` lives in the cache as a partial fill — `status()` must
    /// report it as a hit and `get()` must serve the real bytes, not
    /// zero-padding. The source still gets opened to verify there are
    /// no more bytes past EOF (UnknownSize ⇒ executor doesn't know
    /// where to stop without asking), but the 5 cached bytes must
    /// match the original data.
    source_open_count->store(0);
    {
        auto executor = std::make_unique<ReaderExecutor>(
            source, objects,
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{disk_cache_provider},
            /*window_size=*/20,
            /*min_bytes_for_seek=*/0,
            /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
            file_path);

        PipelineReadBuffer buf(std::move(executor));
        WriteBufferFromOwnString result;
        copyData(buf, result);
        /// If the partial segment had been zero-padded by `put`, this
        /// assertion would catch the corruption (would see "\0\0\0\0\0"
        /// or some hybrid instead of "HELLO").
        ASSERT_EQ(result.str(), data);
    }
}

/// Stacked `CachedObjectStorage` over `CachedObjectStorage`: each layer's
/// `prepareRead` delegates to the wrapped storage first and then appends
/// its own cache stage, so `ReadPipeline::filesystem_caches` ends up
/// inner-to-outer (`[inner, outer]`). The legacy single-object builder
/// wraps caches inside-out, which makes the OUTER layer the FIRST one a
/// read traverses: outer hit serves directly, miss falls through to inner,
/// then to source. Mirror that order on the executor path —
/// `ReadPipeline::tryBuildReaderExecutor` reverse-iterates so the
/// outermost provider lands at `caches[0]`, which is what the executor
/// queries first.
///
/// This test exercises the contract directly at the executor level: pass
/// `{outer, inner}` in the cache vector with the outer pre-populated, and
/// confirm the inner is never touched. A broken source asserts no source
/// read occurs (would mean the outer hit was missed and the request
/// fell through).
TEST_F(FileCacheTest, DiskCacheProviderStackedQueryOrderOuterFirst)
{
    ServerUUID::setRandomForUnitTests();

    auto make_cache = [](const String & name, const String & path)
    {
        DB::FileCacheSettings settings;
        settings[FileCacheSetting::path] = path;
        settings[FileCacheSetting::max_file_segment_size] = 10;
        settings[FileCacheSetting::max_size] = 100;
        settings[FileCacheSetting::max_elements] = 20;
        settings[FileCacheSetting::boundary_alignment] = 1;
        settings[FileCacheSetting::load_metadata_asynchronously] = false;
        settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;
        auto cache = std::make_shared<DB::FileCache>(name, settings);
        cache->initialize();
        return cache;
    };

    auto outer_cache = make_cache("dc_outer", cache_base_path);
    auto inner_cache = make_cache("dc_inner", cache_base_path2);

    /// Set up a 30-byte source file. Used only for the warm-up read; the
    /// real test runs with a broken source.
    const std::string file_path = fs::current_path() / "test_dc_stacked_order";
    const std::string data = "AAAAAAAAAA" "BBBBBBBBBB" "CCCCCCCCCC";  /// 3 x 10
    {
        auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
        wb->write(data.data(), data.size());
        wb->next();
        wb->finalize();
    }
    SCOPE_EXIT({ fs::remove(file_path); });

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    StoredObjects objects;
    objects.emplace_back(file_path, "", data.size());

    /// Warm-up: populate the outer cache (only) with all three segments.
    {
        auto outer_provider = std::make_shared<DiskCacheProvider>(outer_cache, cache_settings);
        auto source_reader = std::make_shared<LocalSourceReader>();

        auto executor = std::make_unique<ReaderExecutor>(
            source_reader, objects,
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{outer_provider},
            /*window_size=*/30,
            /*min_bytes_for_seek=*/0,
            /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
            file_path);

        PipelineReadBuffer buf(std::move(executor));
        WriteBufferFromOwnString result;
        copyData(buf, result);
        ASSERT_EQ(result.str(), data);
    }

    /// Sanity: outer has 3 segments, inner has none.
    assertEqual(outer_cache->dumpQueue(),
        {FileSegment::Range(0, 9), FileSegment::Range(10, 19), FileSegment::Range(20, 29)});
    assertEqual(inner_cache->dumpQueue(), {});

    /// Real test: stack `{outer, inner}` — the executor must query outer
    /// first. A broken source asserts no source read happens; an empty
    /// inner cache after the read asserts inner was never consulted as
    /// the next layer either.
    auto outer_provider = std::make_shared<DiskCacheProvider>(outer_cache, cache_settings);
    auto inner_provider = std::make_shared<DiskCacheProvider>(inner_cache, cache_settings);
    auto broken_source = std::make_shared<BufferSourceReader>(
        [](const StoredObject &) -> std::unique_ptr<ReadBufferFromFileBase>
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Source must not be called when outer cache covers the range");
        },
        "BrokenSource");

    {
        auto executor = std::make_unique<ReaderExecutor>(
            broken_source, objects,
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{outer_provider, inner_provider},
            /*window_size=*/30,
            /*min_bytes_for_seek=*/0,
            /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
            file_path);

        PipelineReadBuffer buf(std::move(executor));
        WriteBufferFromOwnString result;
        copyData(buf, result);
        ASSERT_EQ(result.str(), data);
    }

    /// Inner stays empty: outer hit fully served the read, the inner
    /// layer (which would have populated if it had seen a miss) was
    /// never consulted.
    /// Inner must still be empty: outer hit fully served the read; the
    /// inner layer (which would have populated if it had seen a miss)
    /// was never consulted.
    assertEqual(inner_cache->dumpQueue(), {});
}

/// Bypass mode (`read_from_filesystem_cache_if_exists_otherwise_bypass_cache=1`)
/// uses `cache->get` (read-only) instead of `getOrSet`, so the returned holder
/// contains only segments that already exist. `DiskCacheHandle::status` must
/// still report sub-ranges of the requested range that no segment covers
/// (including the case where the holder is null) as `miss_ranges`, otherwise
/// `ReaderExecutor` silently skips those bytes and returns truncated data.
TEST_F(FileCacheTest, DiskCacheProviderBypassReportsUncoveredRangesAsMiss)
{
    ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path3;
    settings[FileCacheSetting::max_file_segment_size] = 10;
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 20;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("dc_bypass_gaps", settings);
    cache->initialize();

    const std::string file_path = fs::current_path() / "test_dc_bypass_gaps";
    const std::string data = "AAAAAAAAAA" "BBBBBBBBBB" "CCCCCCCCCC";  /// 3 × 10 bytes
    {
        auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
        wb->write(data.data(), data.size());
        wb->next();
        wb->finalize();
    }
    SCOPE_EXIT({ fs::remove(file_path); });

    FilesystemCacheSettings non_bypass_settings;
    non_bypass_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    /// Prime the cache with the middle segment only — read `[10, 20)` through
    /// a normal (non-bypass) provider.
    {
        auto provider = std::make_shared<DiskCacheProvider>(cache, non_bypass_settings);
        auto source_reader = std::make_shared<LocalSourceReader>();

        StoredObjects objects;
        objects.emplace_back(file_path, "", data.size());

        auto executor = std::make_unique<ReaderExecutor>(
            source_reader, objects,
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{provider},
            /*window_size=*/10,
            /*min_bytes_for_seek=*/0,
            /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
            file_path);

        PipelineReadBuffer buf(std::move(executor));
        buf.seek(10, SEEK_SET);

        char out[10];
        ASSERT_EQ(buf.read(out, 10), 10u);
        ASSERT_EQ(std::string(out, 10), "BBBBBBBBBB");
    }

    /// Now lookup [0, 30) in BYPASS mode. Only [10, 20) is cached; the
    /// surrounding two 10-byte ranges must surface as misses.
    FilesystemCacheSettings bypass_settings;
    bypass_settings.read_if_exists_otherwise_bypass = true;
    bypass_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    auto bypass_provider = std::make_shared<DiskCacheProvider>(cache, bypass_settings);

    StoredObject object{file_path, "", data.size()};
    auto handle = bypass_provider->lookup(object, /*object_file_offset=*/0, ByteRange{0, 30});
    ASSERT_NE(handle, nullptr);

    auto status = handle->status();

    /// Aggregate, since FileCache may split ranges into multiple segments
    /// internally.
    auto total = [](const auto & rs)
    {
        size_t s = 0;
        for (const auto & r : rs)
            s += r.size;
        return s;
    };
    ASSERT_EQ(total(status.hit_ranges), 10u);
    ASSERT_EQ(total(status.miss_ranges), 20u);

    /// And the hit covers the middle slot.
    ASSERT_FALSE(status.hit_ranges.empty());
    ASSERT_EQ(status.hit_ranges[0].offset, 10u);

    /// Bonus: a lookup for a range that has NO overlap with the one cached
    /// segment must report the entire range as a miss (holder may be null
    /// in this case).
    auto tail_handle = bypass_provider->lookup(object, /*object_file_offset=*/0, ByteRange{20, 10});
    ASSERT_NE(tail_handle, nullptr);
    auto tail_status = tail_handle->status();
    ASSERT_EQ(total(tail_status.hit_ranges), 0u);
    ASSERT_EQ(total(tail_status.miss_ranges), 10u);
    ASSERT_FALSE(tail_status.miss_ranges.empty());
    ASSERT_EQ(tail_status.miss_ranges[0].offset, 20u);
}

TEST_F(FileCacheTest, PipelineReadBufferReadBigAtConcurrent)
{
    /// Regression-guard for `03988_cached_read_big_at`: PipelineReadBuffer must
    /// implement `supportsReadAt`/`readBigAt`, otherwise Parquet's prefetcher
    /// serializes every read under one mutex.

    ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path;
    settings[FileCacheSetting::max_file_segment_size] = 10;
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 30;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("read_big_at", settings);
    cache->initialize();

    /// 256-byte file with one distinct character per nibble — gives us a
    /// deterministic expected value for any slice we read.
    std::string file_path = fs::current_path() / "test_read_big_at";
    constexpr size_t file_size = 256;
    std::string data(file_size, '\0');
    for (size_t i = 0; i < file_size; ++i)
        data[i] = static_cast<char>(i);
    {
        auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
        wb->write(data.data(), data.size());
        wb->next();
        wb->finalize();
    }
    SCOPE_EXIT({ fs::remove(file_path); });

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    auto disk_cache_provider = std::make_shared<DiskCacheProvider>(cache, cache_settings);
    auto source_reader = std::make_shared<LocalSourceReader>();

    StoredObjects objects;
    objects.emplace_back(file_path, "", data.size());

    auto executor = std::make_unique<ReaderExecutor>(
        source_reader, objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{disk_cache_provider},
        /*window_size=*/ReaderExecutor::DEFAULT_WINDOW_SIZE,
        /*min_bytes_for_seek=*/0,
        /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
        file_path);

    PipelineReadBuffer buf(std::move(executor));

    ASSERT_TRUE(buf.supportsReadAt());

    /// 8 threads doing random readBigAt calls. Each thread compares its
    /// returned bytes against the expected slice. TSan / ASan should stay
    /// quiet because readAt only touches caches/source/immutable state.
    constexpr size_t threads = 8;
    constexpr size_t calls_per_thread = 200;
    std::atomic<size_t> failures{0};
    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (size_t t = 0; t < threads; ++t)
    {
        workers.emplace_back([&, t]()
        {
            std::mt19937 rng(static_cast<uint32_t>(t * 1009 + 17));
            std::vector<char> tmp(file_size);
            for (size_t i = 0; i < calls_per_thread; ++i)
            {
                size_t offset = rng() % file_size;
                size_t want = 1 + (rng() % (file_size - offset));
                size_t got = buf.readBigAt(tmp.data(), want, offset, nullptr);
                if (got != want)
                {
                    ++failures;
                    return;
                }
                if (std::memcmp(tmp.data(), data.data() + offset, want) != 0)
                {
                    ++failures;
                    return;
                }
            }
        });
    }
    for (auto & w : workers)
        w.join();

    ASSERT_EQ(failures.load(), 0u) << "readBigAt returned wrong bytes or short read under concurrency";
}

TEST_F(FileCacheTest, PipelineReadBufferReadBigAtPreservesMainCursor)
{
    /// Sanity: readBigAt must NOT disturb the main read cursor or buffered
    /// data. Mix sequential reads with random readBigAt calls and verify the
    /// sequential side keeps returning correct bytes.

    ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings settings;
    settings[FileCacheSetting::path] = cache_base_path2;
    settings[FileCacheSetting::max_file_segment_size] = 10;
    settings[FileCacheSetting::max_size] = 100;
    settings[FileCacheSetting::max_elements] = 30;
    settings[FileCacheSetting::boundary_alignment] = 1;
    settings[FileCacheSetting::load_metadata_asynchronously] = false;
    settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("read_big_at_cursor", settings);
    cache->initialize();

    std::string file_path = fs::current_path() / "test_read_big_at_cursor";
    constexpr size_t file_size = 128;
    std::string data(file_size, '\0');
    for (size_t i = 0; i < file_size; ++i)
        data[i] = static_cast<char>(i);
    {
        auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
        wb->write(data.data(), data.size());
        wb->next();
        wb->finalize();
    }
    SCOPE_EXIT({ fs::remove(file_path); });

    FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    auto disk_cache_provider = std::make_shared<DiskCacheProvider>(cache, cache_settings);
    auto source_reader = std::make_shared<LocalSourceReader>();
    StoredObjects objects;
    objects.emplace_back(file_path, "", data.size());

    auto executor = std::make_unique<ReaderExecutor>(
        source_reader, objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{disk_cache_provider},
        /*window_size=*/16,
        /*min_bytes_for_seek=*/0,
        /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
        file_path);
    PipelineReadBuffer buf(std::move(executor));

    /// Read first 32 bytes sequentially.
    std::vector<char> seq(32);
    ASSERT_EQ(buf.read(seq.data(), 32), 32u);
    ASSERT_EQ(std::string(seq.begin(), seq.end()), std::string(data.begin(), data.begin() + 32));

    /// Random readBigAt in between.
    char rnd[20];
    ASSERT_EQ(buf.readBigAt(rnd, 20, /*offset=*/60, nullptr), 20u);
    ASSERT_EQ(std::string(rnd, 20), std::string(data.begin() + 60, data.begin() + 80));

    /// Continue sequential reads — must pick up where we left off.
    std::vector<char> seq2(32);
    ASSERT_EQ(buf.read(seq2.data(), 32), 32u);
    ASSERT_EQ(std::string(seq2.begin(), seq2.end()), std::string(data.begin() + 32, data.begin() + 64));
}

/// Recording ICacheProvider: reports a configurable hit range and records the
/// exact range passed to handle->get. Used to verify ReaderExecutor clamps the
/// cache-hit range to the requested window before calling get().
namespace
{
    struct RecordingHandle : public ICacheHandle
    {
        ByteRange hit_range;
        std::vector<ByteRange> & recorded;
        std::string data;

        RecordingHandle(ByteRange hit_, std::vector<ByteRange> & rec_, std::string data_)
            : hit_range(hit_), recorded(rec_), data(std::move(data_)) {}

        CacheLookupResult status() const override
        {
            return CacheLookupResult{{hit_range}, {}};
        }
        Rope get(ByteRange range) override
        {
            recorded.push_back(range);
            /// Return a single rope node sized to the requested range
            /// (mirrors DiskCacheHandle::get, which allocates overlap_size).
            size_t lo = std::max(hit_range.offset, range.offset);
            size_t hi = std::min(hit_range.end(), range.end());
            if (lo >= hi)
                return {};
            auto buf = std::make_shared<OwnedRopeBuffer>(hi - lo);
            std::memcpy(buf->data(), data.data() + (lo - hit_range.offset), hi - lo);
            Rope r;
            r.append(RopeNode{std::move(buf), 0, hi - lo, lo});
            return r;
        }
        size_t put(ByteRange, Rope) override { return 0; }
    };

    struct RecordingCacheProvider : public ICacheProvider
    {
        ByteRange hit_range;
        std::vector<ByteRange> recorded_gets;
        std::string data;

        RecordingCacheProvider(ByteRange hit_, std::string data_)
            : hit_range(hit_), data(std::move(data_)) {}

        std::unique_ptr<ICacheHandle> lookup(const StoredObject &, size_t, ByteRange) override
        {
            return std::make_unique<RecordingHandle>(hit_range, recorded_gets, data);
        }
        String name() const override { return "Recording"; }
    };
}

TEST_F(FileCacheTest, ReaderExecutorClampsHitToRequestedWindow)
{
    /// Regression: `readPhysicalWindow` used to call `handle->get(hit)` with the
    /// cache's full segment range. With large segments (default
    /// `max_file_segment_size` = 4 MiB) and many concurrent readers, allocations
    /// in `DiskCacheHandle::get` (sized to the segment, not the window) blew
    /// past per-query memory limits — `INSERT INTO test.hits_s3 SELECT *
    /// FROM test.hits` hit `MEMORY_LIMIT_EXCEEDED` at 27.94 GiB on master.
    ///
    /// After the fix, `readPhysicalWindow` clamps the hit range to the
    /// requested window before calling `get`, so the allocation is at most
    /// `window_size` regardless of segment size.

    ServerUUID::setRandomForUnitTests();

    /// File content is 30 bytes of distinct values; the recording provider
    /// pretends the whole [0, 30) range is one cached segment.
    std::string data(30, 'X');
    std::string file_path = fs::current_path() / "test_clamp_dummy";
    {
        auto wb = std::make_unique<WriteBufferFromFile>(file_path, DBMS_DEFAULT_BUFFER_SIZE);
        wb->write(data.data(), data.size());
        wb->next();
        wb->finalize();
    }
    SCOPE_EXIT({ fs::remove(file_path); });

    auto recording = std::make_shared<RecordingCacheProvider>(ByteRange{0, 30}, data);
    auto source_reader = std::make_shared<LocalSourceReader>();

    StoredObjects objects;
    objects.emplace_back(file_path, "", data.size());

    /// window_size = 10, segment "hit" size = 30. Each readNextWindow should
    /// trigger get() with a range no larger than the window, not the segment.
    auto executor = std::make_unique<ReaderExecutor>(
        source_reader, objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{recording},
        /*window_size=*/10,
        /*min_bytes_for_seek=*/0,
        /*block_size=*/ReaderExecutor::ROPE_BLOCK_SIZE,
        file_path);

    PipelineReadBuffer buf(std::move(executor));
    WriteBufferFromOwnString result;
    copyData(buf, result);
    ASSERT_EQ(result.str(), data);

    /// Verify every recorded get() asked for at most window_size bytes.
    ASSERT_FALSE(recording->recorded_gets.empty());
    for (const auto & rg : recording->recorded_gets)
        ASSERT_LE(rg.size, 10u)
            << "ReaderExecutor passed an unclamped segment range to handle->get: "
            << "[" << rg.offset << ", " << rg.end() << "), size " << rg.size;
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

    /// Both priority entries account for reserved size.
    ASSERT_EQ(cache->getUsedCacheSize(), 16u);
    ASSERT_EQ(cache->getFileSegmentsNum(), 2u);

    /// Force the failed-eviction restore loop to run.
    {
        DB::FailPointInjection::enableFailPoint("file_cache_dynamic_resize_fail_to_evict");
        SCOPE_EXIT({
            DB::FailPointInjection::disableFailPoint("file_cache_dynamic_resize_fail_to_evict");
        });

        /// Trigger resize. The restore path must keep total queue size at 16.
        DB::FileCacheSettings new_settings = settings;
        new_settings[FileCacheSetting::max_size] = 4;
        DB::FileCacheSettings actual_settings = settings;

        ASSERT_NO_THROW(cache->applySettingsIfPossible(new_settings, actual_settings));

        /// Failed eviction reverts limits to the previous value.
        ASSERT_EQ(actual_settings[FileCacheSetting::max_size].value, 16u);

        /// Release-visible check for restored reserved-size accounting.
        ASSERT_EQ(cache->getUsedCacheSize(), 16u);
        ASSERT_EQ(cache->getFileSegmentsNum(), 2u);

        /// All segments must still be reachable from the priority queue.
        {
            auto infos = cache->getFileSegmentInfos(key, user.user_id);
            ASSERT_EQ(infos.size(), 2u);
            for (const auto & info : infos)
                ASSERT_NE(info.queue_entry_type, FileCacheQueueEntryType::None);
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
