#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>


#include <algorithm>
#include <numeric>
#include <thread>
#include <chrono>

#include <Core/ServerUUID.h>
#include <Common/iota.h>
#include <Common/randomSeed.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/SLRUFileCachePriority.h>
#include <Interpreters/Context.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <base/hex.h>
#include <base/sleep.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/CurrentThread.h>
#include <Common/SipHash.h>
#include <Common/filesystemHelpers.h>
#include <Common/scope_guard_safe.h>
#include <Common/tests/gtest_global_context.h>

#include <Poco/ConsoleChannel.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Interpreters/Cache/WriteBufferToFileSegment.h>

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
    if (const auto * lru = dynamic_cast<const LRUFileCachePriority::LRUPriorityDump *>(dump.get()))
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
    if (const auto * lru = dynamic_cast<const LRUFileCachePriority::LRUPriorityDump *>(dump.get()))
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
    if (const auto * lru = dynamic_cast<const LRUFileCachePriority::LRUPriorityDump *>(dump.get()))
    {
        assertProbationary(lru->infos, expected);
    }
    else
    {
        ASSERT_TRUE(false);
    }
}

FileSegment & get(const HolderPtr & holder, int i)
{
    auto it = std::next(holder->begin(), i);
    if (it == holder->end())
        std::terminate();
    return **it;
}

void download(FileSegment & file_segment)
{
    std::cerr << "\nDownloading range " << file_segment.range().toString() << "\n";

    ASSERT_EQ(file_segment.getOrSetDownloader(), FileSegment::getCallerId());
    ASSERT_EQ(file_segment.state(), State::DOWNLOADING);
    ASSERT_EQ(file_segment.getDownloadedSize(), 0);

    std::string failure_reason;
    ASSERT_TRUE(file_segment.reserve(file_segment.range().size(), 1000, failure_reason));
    download(cache_base_path, file_segment);
    ASSERT_EQ(file_segment.state(), State::DOWNLOADING);

    file_segment.complete(false);
    ASSERT_EQ(file_segment.state(), State::DOWNLOADED);
}

void assertDownloadFails(FileSegment & file_segment)
{
    ASSERT_EQ(file_segment.getOrSetDownloader(), FileSegment::getCallerId());
    ASSERT_EQ(file_segment.getDownloadedSize(), 0);
    std::string failure_reason;
    ASSERT_FALSE(file_segment.reserve(file_segment.range().size(), 1000, failure_reason));
    file_segment.complete(false);
}

void download(const HolderPtr & holder)
{
    for (auto & it : *holder)
    {
        download(*it);
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
    FileCacheTest() {
        /// Context has to be created before calling cache.initialize();
        /// Otherwise the tests which run before FileCacheTest.get are failed
        /// It is logical to call destroyContext() at destructor.
        /// But that wouldn't work because for proper initialization and destruction global/static objects
        /// testing::Environment has to be used.
        getContext();
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
        fs::create_directories(cache_base_path);
        fs::create_directories(cache_base_path2);
    }

    void TearDown() override
    {
        if (fs::exists(cache_base_path))
            fs::remove_all(cache_base_path);
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
    DB::CurrentThread::QueryScope query_scope_holder(query_context);

    DB::FileCacheSettings settings;
    settings.base_path = cache_base_path;
    settings.max_size = 30;
    settings.max_elements = 5;
    settings.boundary_alignment = 1;
    settings.load_metadata_asynchronously = false;

    const size_t file_size = INT_MAX; // the value doesn't really matter because boundary_alignment == 1.


    const auto user = FileCache::getCommonUser();
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
            download(holder->front());
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
                        { State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADED, State::DETACHED, State::DOWNLOADED });

            /// Range [27, 27] must be evicted in previous getOrSet [0, 25].
            /// Let's not invalidate pointers to returned segments from range [0, 25] and
            /// as max elements size is reached, next attempt to put something in cache should fail.
            /// This will also check that [27, 27] was indeed evicted.
            auto holder2 = get_or_set(27, 1);
            assertEqual(holder2, { Range(27, 27) }, { State::EMPTY });
            assertDownloadFails(holder2->front());
            assertEqual(holder2, { Range(27, 27) }, { State::DETACHED });

            auto holder3 = get_or_set(28, 3);
            assertEqual(holder3, { Range(28, 30) }, { State::EMPTY });
            assertDownloadFails(holder3->front());
            assertEqual(holder3, { Range(28, 30) }, { State::DETACHED });

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

            auto & file_segment = get(holder, 2);
            ASSERT_TRUE(file_segment.getOrSetDownloader() == FileSegment::getCallerId());
            ASSERT_TRUE(file_segment.state() == State::DOWNLOADING);

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
                DB::CurrentThread::QueryScope query_scope_holder_1(query_context_1);

                auto holder2 = get_or_set(25, 5); /// Get [25, 29] once again.
                assertEqual(holder2,
                            { Range(24, 26),     Range(27, 27),     Range(28, 29) },
                            { State::DOWNLOADED, State::DOWNLOADED, State::DOWNLOADING });

                auto & file_segment2 = get(holder2, 2);
                ASSERT_TRUE(file_segment2.getOrSetDownloader() != FileSegment::getCallerId());
                ASSERT_EQ(file_segment2.state(), State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                file_segment2.wait(file_segment2.range().right);
                ASSERT_EQ(file_segment2.getDownloadedSize(), file_segment2.range().size());
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&]{ return lets_start_download; });
            }

            download(file_segment);
            ASSERT_EQ(file_segment.state(), State::DOWNLOADED);

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

            auto & file_segment = get(holder, 1);
            ASSERT_TRUE(file_segment.getOrSetDownloader() == FileSegment::getCallerId());
            ASSERT_TRUE(file_segment.state() == State::DOWNLOADING);

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
                DB::CurrentThread::QueryScope query_scope_holder_1(query_context_1);

                auto holder2 = get_or_set(3, 23); /// get [3, 25] once again.
                assertEqual(holder,
                            { Range(2, 4),       Range(5, 23),       Range(24, 26) },
                            { State::DOWNLOADED, State::DOWNLOADING, State::DOWNLOADED });

                auto & file_segment2 = get(holder, 1);
                ASSERT_TRUE(file_segment2.getDownloader() != FileSegment::getCallerId());

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                file_segment2.wait(file_segment2.range().left);
                ASSERT_EQ(file_segment2.state(), DB::FileSegment::State::EMPTY);
                ASSERT_EQ(file_segment2.getOrSetDownloader(), DB::FileSegment::getCallerId());
                download(file_segment2);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&]{ return lets_start_download; });
            }

            holder.reset();
            other_1.join();
            ASSERT_TRUE(file_segment.state() == DB::FileSegment::State::DOWNLOADED);
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
        settings2.max_file_segment_size = 10;
        settings2.base_path = caches_dir / "cache2";
        fs::create_directories(settings2.base_path);
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
    settings.max_size = 100;
    settings.max_elements = 5;
    settings.max_file_segment_size = 5;
    settings.base_path = cache_base_path;
    settings.load_metadata_asynchronously = false;

    FileCache cache("6", settings);
    cache.initialize();
    const auto user = FileCache::getCommonUser();

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

static size_t readAllTemporaryData(TemporaryFileStream & stream)
{
    Block block;
    size_t read_rows = 0;
    do
    {
        block = stream.read();
        read_rows += block.rows();
    } while (block);
    return read_rows;
}

TEST_F(FileCacheTest, temporaryData)
{
    ServerUUID::setRandomForUnitTests();
    DB::FileCacheSettings settings;
    settings.max_size = 10_KiB;
    settings.max_file_segment_size = 1_KiB;
    settings.base_path = cache_base_path;
    settings.load_metadata_asynchronously = false;

    DB::FileCache file_cache("7", settings);
    file_cache.initialize();

    const auto user = FileCache::getCommonUser();
    auto tmp_data_scope = std::make_shared<TemporaryDataOnDiskScope>(nullptr, &file_cache, TemporaryDataOnDiskSettings{});

    auto some_data_holder = file_cache.getOrSet(FileCacheKey::fromPath("some_data"), 0, 5_KiB, 5_KiB, CreateFileSegmentSettings{}, 0, user);

    {
        ASSERT_EQ(some_data_holder->size(), 5);
        std::string failure_reason;
        for (auto & segment : *some_data_holder)
        {
            ASSERT_TRUE(segment->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(segment->reserve(segment->range().size(), 1000, failure_reason));
            download(*segment);
            segment->complete(false);
        }
    }

    size_t size_used_before_temporary_data = file_cache.getUsedCacheSize();
    size_t segments_used_before_temporary_data = file_cache.getFileSegmentsNum();
    ASSERT_GT(size_used_before_temporary_data, 0);
    ASSERT_GT(segments_used_before_temporary_data, 0);

    size_t size_used_with_temporary_data;
    size_t segments_used_with_temporary_data;
    {
        auto tmp_data = std::make_unique<TemporaryDataOnDisk>(tmp_data_scope);

        auto & stream = tmp_data->createStream(generateBlock());

        ASSERT_GT(stream.write(generateBlock(100)), 0);

        ASSERT_GT(file_cache.getUsedCacheSize(), 0);
        ASSERT_GT(file_cache.getFileSegmentsNum(), 0);

        size_t used_size_before_attempt = file_cache.getUsedCacheSize();
        /// data can't be evicted because it is still held by `some_data_holder`
        ASSERT_THROW({
            stream.write(generateBlock(2000));
            stream.flush();
        }, DB::Exception);

        ASSERT_EQ(file_cache.getUsedCacheSize(), used_size_before_attempt);
    }

    {
        size_t before_used_size = file_cache.getUsedCacheSize();
        auto tmp_data = std::make_unique<TemporaryDataOnDisk>(tmp_data_scope);

        auto write_buf_stream = tmp_data->createRawStream();

        write_buf_stream->write("1234567890", 10);
        write_buf_stream->write("abcde", 5);
        auto read_buf = dynamic_cast<IReadableWriteBuffer *>(write_buf_stream.get())->tryGetReadBuffer();

        ASSERT_GT(file_cache.getUsedCacheSize(), before_used_size + 10);

        char buf[15];
        size_t read_size = read_buf->read(buf, 15);
        ASSERT_EQ(read_size, 15);
        ASSERT_EQ(std::string(buf, 15), "1234567890abcde");
        read_size = read_buf->read(buf, 15);
        ASSERT_EQ(read_size, 0);
    }

    {
        auto tmp_data = std::make_unique<TemporaryDataOnDisk>(tmp_data_scope);
        auto & stream = tmp_data->createStream(generateBlock());

        ASSERT_GT(stream.write(generateBlock(100)), 0);

        some_data_holder.reset();

        stream.write(generateBlock(2000));

        auto stat = stream.finishWriting();

        ASSERT_TRUE(fs::exists(stream.getPath()));
        ASSERT_GT(fs::file_size(stream.getPath()), 100);

        ASSERT_EQ(stat.num_rows, 2100);
        ASSERT_EQ(readAllTemporaryData(stream), 2100);

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
    DB::CurrentThread::QueryScope query_scope_holder(query_context);

    DB::FileCacheSettings settings;
    settings.base_path = cache_base_path;
    settings.max_file_segment_size = 5;
    settings.max_size = 30;
    settings.max_elements = 10;
    settings.boundary_alignment = 1;
    settings.load_metadata_asynchronously = false;

    ReadSettings read_settings;
    read_settings.enable_filesystem_cache = true;
    read_settings.local_fs_method = LocalFSReadMethod::pread;

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
    const auto user = FileCache::getCommonUser();

    {
        auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
            file_path, key, cache, user, read_buffer_creator, read_settings, "test", s.size(), false, false, std::nullopt, nullptr);

        WriteBufferFromOwnString result;
        copyData(*cached_buffer, result);
        ASSERT_EQ(result.str(), s);

        assertEqual(cache->dumpQueue(), { Range(0, 4), Range(5, 9), Range(10, 14), Range(15, 19), Range(20, 24), Range(25, 29) });
    }

    {
        ReadSettings modified_settings{read_settings};
        modified_settings.local_fs_buffer_size = 10;
        modified_settings.remote_fs_buffer_size = 10;

        auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
            file_path, key, cache, user, read_buffer_creator, modified_settings, "test", s.size(), false, false, std::nullopt, nullptr);

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
        settings.max_size = 10_KiB;
        settings.max_file_segment_size = 1_KiB;
        settings.base_path = cache_base_path;
        settings.load_metadata_asynchronously = false;

        DB::FileCache file_cache("cache", settings);
        file_cache.initialize();

        auto tmp_data_scope = std::make_shared<TemporaryDataOnDiskScope>(/*volume=*/nullptr, &file_cache, /*settings=*/TemporaryDataOnDiskSettings{});

        auto tmp_data = std::make_unique<TemporaryDataOnDisk>(tmp_data_scope);

        auto block = generateBlock(/*size=*/3);
        auto & stream = tmp_data->createStream(block);
        stream.write(block);
        stream.finishWriting();

        /// We allocate buffer of size min(getSize(), DBMS_DEFAULT_BUFFER_SIZE)
        /// We do care about buffer size because realistic external group by could generate 10^5 temporary files
        ASSERT_EQ(stream.getSize(), 62);
    }

    /// Temporary data stored on disk
    {
        DiskPtr disk;
        SCOPE_EXIT_SAFE(destroyDisk(disk));

        disk = createDisk("temporary_data_read_buffer_size_test_dir");
        VolumePtr volume = std::make_shared<SingleDiskVolume>("volume", disk);

        auto tmp_data_scope = std::make_shared<TemporaryDataOnDiskScope>(/*volume=*/volume, /*cache=*/nullptr, /*settings=*/TemporaryDataOnDiskSettings{});

        auto tmp_data = std::make_unique<TemporaryDataOnDisk>(tmp_data_scope);

        auto block = generateBlock(/*size=*/3);
        auto & stream = tmp_data->createStream(block);
        stream.write(block);
        stream.finishWriting();

        ASSERT_EQ(stream.getSize(), 62);
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
    DB::CurrentThread::QueryScope query_scope_holder(query_context);

    DB::FileCacheSettings settings;
    settings.base_path = cache_base_path;
    settings.max_size = 40;
    settings.max_elements = 6;
    settings.boundary_alignment = 1;
    settings.load_metadata_asynchronously = false;

    settings.cache_policy = "SLRU";
    settings.slru_size_ratio = 0.5;

    const size_t file_size = -1; // the value doesn't really matter because boundary_alignment == 1.
    size_t file_cache_name = 0;
    const auto user = FileCache::getCommonUser();

    {
        auto cache = DB::FileCache(std::to_string(++file_cache_name), settings);
        cache.initialize();
        auto key = FileCacheKey::fromPath("key1");

        auto add_range = [&](size_t offset, size_t size)
        {
            std::cerr << "Add [" << offset << ", " << offset + size - 1 << "]" << std::endl;

            auto holder = cache.getOrSet(key, offset, size, file_size, {}, 0, user);
            assertEqual(holder, { Range(offset, offset + size - 1) }, { State::EMPTY });
            download(holder->front());
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
        read_settings.local_fs_method = LocalFSReadMethod::pread;

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
        settings2.base_path = cache_base_path2;
        settings2.max_file_segment_size = 5;
        settings2.max_size = 30;
        settings2.max_elements = 6;
        settings2.boundary_alignment = 1;
        settings2.cache_policy = "SLRU";
        settings2.slru_size_ratio = 0.5;
        settings.load_metadata_asynchronously = false;

        auto cache = std::make_shared<DB::FileCache>("slru_2", settings2);
        cache->initialize();

        auto read_and_check = [&](const std::string & file, const FileCacheKey & key, const std::string & expect_result)
        {
            auto read_buffer_creator = [&]()
            {
                return createReadBufferFromFileBase(file, read_settings, std::nullopt, std::nullopt);
            };

            auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
                file, key, cache, user, read_buffer_creator, read_settings, "test", expect_result.size(), false, false, std::nullopt, nullptr);

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

        const auto & infos = dynamic_cast<const LRUFileCachePriority::LRUPriorityDump *>(dump.get())->infos;
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

        const auto & infos2 = dynamic_cast<const LRUFileCachePriority::LRUPriorityDump *>(dump.get())->infos;
        ASSERT_EQ(infos2[0].key, key1);
        ASSERT_EQ(infos2[1].key, key1);
        ASSERT_EQ(infos2[2].key, key1);
        ASSERT_EQ(infos2[3].key, key2);
        ASSERT_EQ(infos2[4].key, key2);

        assertProbationary(cache->dumpQueue(), { Range(0, 4), Range(5, 9) });
        assertProtected(cache->dumpQueue(), { Range(10, 14), Range(0, 4), Range(5, 9)  });
    }
}
