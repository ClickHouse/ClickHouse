#include <iomanip>
#include <iostream>
#include <gtest/gtest.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Common/CurrentThread.h>
#include <Common/filesystemHelpers.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/SipHash.h>
#include <base/hex.h>
#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <filesystem>
#include <thread>
#include <DataTypes/DataTypesNumber.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/DOMParser.h>
#include <base/sleep.h>

#include <Poco/ConsoleChannel.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Interpreters/Cache/WriteBufferToFileSegment.h>

namespace fs = std::filesystem;
using namespace DB;

static constexpr auto TEST_LOG_LEVEL = "debug";

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


void assertEqual(FileSegments::const_iterator segments_begin, FileSegments::const_iterator segments_end, size_t segments_size, const Ranges & expected_ranges, const States & expected_states = {})
{
    std::cerr << "File segments: ";
    for (auto it = segments_begin; it != segments_end; ++it)
        std::cerr << (*it)->range().toString() << ", ";

    ASSERT_EQ(segments_size, expected_ranges.size());

    if (!expected_states.empty())
        ASSERT_EQ(segments_size, expected_states.size());

    auto get_expected_state = [&](size_t i)
    {
        if (expected_states.empty())
            return State::DOWNLOADED;
        else
            return expected_states[i];
    };

    size_t i = 0;
    for (auto it = segments_begin; it != segments_end; ++it)
    {
        const auto & file_segment = *it;
        ASSERT_EQ(file_segment->range(), expected_ranges[i]);
        ASSERT_EQ(file_segment->state(), get_expected_state(i));
        ++i;
    }
}

void assertEqual(const FileSegments & file_segments, const Ranges & expected_ranges, const States & expected_states = {})
{
    assertEqual(file_segments.begin(), file_segments.end(), file_segments.size(), expected_ranges, expected_states);
}

void assertEqual(const FileSegmentsHolderPtr & file_segments, const Ranges & expected_ranges, const States & expected_states = {})
{
    assertEqual(file_segments->begin(), file_segments->end(), file_segments->size(), expected_ranges, expected_states);
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
    std::cerr << "Downloading range " << file_segment.range().toString() << "\n";

    ASSERT_EQ(file_segment.getOrSetDownloader(), FileSegment::getCallerId());
    ASSERT_EQ(file_segment.state(), State::DOWNLOADING);
    ASSERT_EQ(file_segment.getDownloadedSize(), 0);

    ASSERT_TRUE(file_segment.reserve(file_segment.range().size()));
    download(cache_base_path, file_segment);
    ASSERT_EQ(file_segment.state(), State::DOWNLOADING);

    file_segment.complete();
    ASSERT_EQ(file_segment.state(), State::DOWNLOADED);
}

void assertDownloadFails(FileSegment & file_segment)
{
    ASSERT_EQ(file_segment.getOrSetDownloader(), FileSegment::getCallerId());
    ASSERT_EQ(file_segment.getDownloadedSize(), 0);
    ASSERT_FALSE(file_segment.reserve(file_segment.range().size()));
    file_segment.complete();
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
        it->use();
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

        if (fs::exists(cache_base_path))
            fs::remove_all(cache_base_path);
        fs::create_directories(cache_base_path);
    }

    void TearDown() override
    {
        if (fs::exists(cache_base_path))
            fs::remove_all(cache_base_path);
    }

};

TEST_F(FileCacheTest, get)
{
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

    const size_t file_size = -1; // the value doesn't really matter because boundary_alignment == 1.

    {
        std::cerr << "Step 1\n";
        auto cache = DB::FileCache("1", settings);
        cache.initialize();
        auto key = cache.createKeyForPath("key1");

        {
            auto holder = cache.getOrSet(key, 0, 10, file_size, {}); /// Add range [0, 9]
            assertEqual(holder, { Range(0, 9) }, { State::EMPTY });
            download(holder->front());
            assertEqual(holder, { Range(0, 9) }, { State::DOWNLOADED });
            increasePriority(holder);
        }

        /// Current cache:    [__________]
        ///                   ^          ^
        ///                   0          9
        assertEqual(cache.getSnapshot(key), { Range(0, 9) });
        assertEqual(cache.dumpQueue(), { Range(0, 9) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 1);
        ASSERT_EQ(cache.getUsedCacheSize(), 10);

        std::cerr << "Step 2\n";

        {
            /// Want range [5, 14], but [0, 9] already in cache, so only [10, 14] will be put in cache.
            auto holder = cache.getOrSet(key, 5, 10, file_size, {});
            assertEqual(holder, { Range(0, 9), Range(10, 14) }, { State::DOWNLOADED, State::EMPTY });
            download(get(holder, 1));
            assertEqual(holder, { Range(0, 9), Range(10, 14) }, { State::DOWNLOADED, State::DOWNLOADED });
            increasePriority(holder);
        }

        /// Current cache:    [__________][_____]
        ///                   ^          ^^     ^
        ///                   0          910    14
        assertEqual(cache.getSnapshot(key), { Range(0, 9), Range(10, 14) });
        assertEqual(cache.dumpQueue(), { Range(0, 9), Range(10, 14) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 2);
        ASSERT_EQ(cache.getUsedCacheSize(), 15);

        std::cerr << "Step 3\n";

        /// Get [9, 9]
        {
            auto holder = cache.getOrSet(key, 9, 1, file_size, {});
            assertEqual(holder, { Range(0, 9) }, { State::DOWNLOADED });
            increasePriority(holder);
        }

        assertEqual(cache.dumpQueue(), { Range(10, 14), Range(0, 9) });
        /// Get [9, 10]
        assertEqual(cache.getOrSet(key, 9, 2, file_size, {}), {Range(0, 9), Range(10, 14)}, {State::DOWNLOADED, State::DOWNLOADED});

        /// Get [10, 10]
        {
            auto holder = cache.getOrSet(key, 10, 1, file_size, {});
            assertEqual(holder, { Range(10, 14) }, { State::DOWNLOADED });
            increasePriority(holder);
        }

        assertEqual(cache.getSnapshot(key), { Range(0, 9), Range(10, 14) });
        assertEqual(cache.dumpQueue(), { Range(0, 9), Range(10, 14) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 2);
        ASSERT_EQ(cache.getUsedCacheSize(), 15);

        std::cerr << "Step 4\n";

        {
            auto holder = cache.getOrSet(key, 17, 4, file_size, {});
            download(holder); /// Get [17, 20]
            increasePriority(holder);
        }

        {
            auto holder = cache.getOrSet(key, 24, 3, file_size, {});
            download(holder); /// Get [24, 26]
            increasePriority(holder);
        }

        {
            auto holder = cache.getOrSet(key, 27, 1, file_size, {});
            download(holder); /// Get [27, 27]
            increasePriority(holder);
        }

        /// Current cache:    [__________][_____]   [____]    [___][]
        ///                   ^          ^^     ^   ^    ^    ^   ^^^
        ///                   0          910    14  17   20   24  2627
        ///
        assertEqual(cache.getSnapshot(key), { Range(0, 9), Range(10, 14), Range(17, 20), Range(24, 26), Range(27, 27) });
        assertEqual(cache.dumpQueue(), { Range(0, 9), Range(10, 14), Range(17, 20), Range(24, 26), Range(27, 27) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 5);
        ASSERT_EQ(cache.getUsedCacheSize(), 23);

        std::cerr << "Step 5\n";
        {
            auto holder = cache.getOrSet(key, 0, 26, file_size, {}); /// Get [0, 25]
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
            auto holder2 = cache.getOrSet(key, 27, 1, file_size, {});
            assertEqual(holder2, { Range(27, 27) }, { State::EMPTY });
            assertDownloadFails(holder2->front());
            assertEqual(holder2, { Range(27, 27) }, { State::DETACHED });

            auto holder3 = cache.getOrSet(key, 28, 3, file_size, {});
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
        assertEqual(cache.getSnapshot(key), { Range(0, 9), Range(10, 14), Range(15, 16), Range(17, 20), Range(24, 26) });
        assertEqual(cache.dumpQueue(), { Range(0, 9), Range(10, 14), Range(15, 16), Range(17, 20), Range(24, 26) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 5);
        ASSERT_EQ(cache.getUsedCacheSize(), 24);

        std::cerr << "Step 6\n";

        {
            auto holder = cache.getOrSet(key, 12, 10, file_size, {}); /// Get [12, 21]
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
        assertEqual(cache.getSnapshot(key), { Range(10, 14), Range(15, 16), Range(17, 20), Range(21, 21), Range(24, 26) });
        assertEqual(cache.dumpQueue(), { Range(24, 26), Range(10, 14), Range(15, 16), Range(17, 20), Range(21, 21) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 5);
        ASSERT_EQ(cache.getUsedCacheSize(), 15);

        std::cerr << "Step 7\n";
        {
            auto holder = cache.getOrSet(key, 23, 5, file_size, {}); /// Get [23, 27]
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
        assertEqual(cache.getSnapshot(key), { Range(17, 20), Range(21, 21), Range(23, 23), Range(24, 26), Range(27, 27) });
        assertEqual(cache.dumpQueue(), { Range(17, 20), Range(21, 21), Range(23, 23), Range(24, 26), Range(27, 27) });
        ASSERT_EQ(cache.getFileSegmentsNum(), 5);
        ASSERT_EQ(cache.getUsedCacheSize(), 10);

        std::cerr << "Step 8\n";
        {
            auto holder = cache.getOrSet(key, 2, 3, file_size, {}); /// Get [2, 4]
            assertEqual(holder, { Range(2, 4) }, { State::EMPTY });

            auto holder2 = cache.getOrSet(key, 30, 2, file_size, {}); /// Get [30, 31]
            assertEqual(holder2, { Range(30, 31) }, { State::EMPTY });

            download(get(holder, 0));
            download(get(holder2, 0));

            auto holder3 = cache.getOrSet(key, 23, 1, file_size, {}); /// Get [23, 23]
            assertEqual(holder3, { Range(23, 23) }, { State::DOWNLOADED });

            auto holder4 = cache.getOrSet(key, 24, 3, file_size, {}); /// Get [24, 26]
            assertEqual(holder4, { Range(24, 26) }, { State::DOWNLOADED });

            auto holder5 = cache.getOrSet(key, 27, 1, file_size, {}); /// Get [27, 27]
            assertEqual(holder5, { Range(27, 27) }, { State::DOWNLOADED });

            auto holder6 = cache.getOrSet(key, 0, 40, file_size, {});
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
        assertEqual(cache.getSnapshot(key), { Range(2, 4), Range(23, 23), Range(24, 26), Range(27, 27), Range(30, 31) });
        assertEqual(cache.dumpQueue(), { Range(2, 4), Range(23, 23), Range(24, 26), Range(27, 27), Range(30, 31) });

        std::cerr << "Step 9\n";

        /// Get [2, 4]
        {
            auto holder = cache.getOrSet(key, 2, 3, file_size, {});
            assertEqual(holder, { Range(2, 4) }, { State::DOWNLOADED });
            increasePriority(holder);
        }


        {
            auto holder = cache.getOrSet(key, 25, 5, file_size, {}); /// Get [25, 29]
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

                auto holder2 = cache.getOrSet(key, 25, 5, file_size, {}); /// Get [25, 29] once again.
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
        assertEqual(cache.getSnapshot(key), { Range(2, 4), Range(24, 26), Range(27, 27), Range(28, 29), Range(30, 31) });
        assertEqual(cache.dumpQueue(), { Range(30, 31), Range(2, 4), Range(24, 26), Range(27, 27), Range(28, 29) });

        std::cerr << "Step 10\n";
        {
            /// Now let's check the similar case but getting ERROR state after segment->wait(), when
            /// state is changed not manually via segment->completeWithState(state) but from destructor of holder
            /// and notify_all() is also called from destructor of holder.

            auto holder = cache.getOrSet(key, 3, 23, file_size, {}); /// Get [3, 25]
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

                auto holder2 = cache.getOrSet(key, 3, 23, file_size, {}); /// Get [3, 25] once again
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
        auto key = cache2.createKeyForPath("key1");

        /// Get [2, 29]
        assertEqual(
            cache2.getOrSet(key, 2, 28, file_size, {}),
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
        auto key = cache2.createKeyForPath("key1");

        /// Get [0, 24]
        assertEqual(
            cache2.getOrSet(key, 0, 25, file_size, {}),
            {Range(0, 9), Range(10, 19), Range(20, 24)},
            {State::EMPTY, State::EMPTY, State::EMPTY});
    }

    std::cerr << "Step 13\n";
    {
        /// Test delayed cleanup

        auto cache = FileCache("4", settings);
        cache.initialize();
        const auto key = cache.createKeyForPath("key10");
        const auto key_path = cache.getPathInLocalCache(key);

        cache.removeAllReleasable();
        ASSERT_EQ(cache.getUsedCacheSize(), 0);
        ASSERT_TRUE(!fs::exists(key_path));
        ASSERT_TRUE(!fs::exists(fs::path(key_path).parent_path()));

        download(cache.getOrSet(key, 0, 10, file_size, {}));
        ASSERT_EQ(cache.getUsedCacheSize(), 10);
        ASSERT_TRUE(fs::exists(cache.getPathInLocalCache(key, 0, FileSegmentKind::Regular)));

        cache.removeAllReleasable();
        ASSERT_EQ(cache.getUsedCacheSize(), 0);
        ASSERT_TRUE(!fs::exists(key_path));
        ASSERT_TRUE(!fs::exists(cache.getPathInLocalCache(key, 0, FileSegmentKind::Regular)));
    }

    std::cerr << "Step 14\n";
    {
        /// Test background thread delated cleanup

        auto cache = DB::FileCache("5", settings);
        cache.initialize();
        const auto key = cache.createKeyForPath("key10");
        const auto key_path = cache.getPathInLocalCache(key);

        cache.removeAllReleasable();
        ASSERT_EQ(cache.getUsedCacheSize(), 0);
        ASSERT_TRUE(!fs::exists(key_path));
        ASSERT_TRUE(!fs::exists(fs::path(key_path).parent_path()));

        download(cache.getOrSet(key, 0, 10, file_size, {}));
        ASSERT_EQ(cache.getUsedCacheSize(), 10);
        ASSERT_TRUE(fs::exists(key_path));

        cache.removeAllReleasable();
        ASSERT_EQ(cache.getUsedCacheSize(), 0);
        sleepForSeconds(2);
        ASSERT_TRUE(!fs::exists(key_path));
    }
}

TEST_F(FileCacheTest, writeBuffer)
{
    FileCacheSettings settings;
    settings.max_size = 100;
    settings.max_elements = 5;
    settings.max_file_segment_size = 5;
    settings.base_path = cache_base_path;

    FileCache cache("6", settings);
    cache.initialize();

    auto write_to_cache = [&cache](const String & key, const Strings & data, bool flush)
    {
        CreateFileSegmentSettings segment_settings;
        segment_settings.kind = FileSegmentKind::Temporary;
        segment_settings.unbounded = true;

        auto cache_key = cache.createKeyForPath(key);
        auto holder = cache.set(cache_key, 0, 3, segment_settings);
        /// The same is done in TemporaryDataOnDisk::createStreamToCacheFile.
        std::filesystem::create_directories(cache.getPathInLocalCache(cache_key));
        EXPECT_EQ(holder->size(), 1);
        auto & segment = holder->front();
        WriteBufferToFileSegment out(&segment);
        std::list<std::thread> threads;
        std::mutex mu;
        for (const auto & s : data)
        {
            /// Write from diffetent threads to check
            /// that no assertions inside cache related to downloaderId are triggered
            threads.emplace_back([&]
            {
                std::unique_lock lock(mu);
                out.write(s.data(), s.size());
                /// test different buffering scenarios
                if (flush)
                {
                    out.next();
                }
            });
        }
        for (auto & t : threads)
            t.join();
        out.finalize();
        return holder;
    };

    std::vector<fs::path> file_segment_paths;
    {
        auto holder = write_to_cache("key1", {"abc", "defg"}, false);
        file_segment_paths.emplace_back(holder->front().getPathInLocalCache());

        ASSERT_EQ(fs::file_size(file_segment_paths.back()), 7);
        ASSERT_TRUE(holder->front().range() == FileSegment::Range(0, 7));
        ASSERT_EQ(cache.getUsedCacheSize(), 7);

        {
            auto holder2 = write_to_cache("key2", {"1", "22", "333", "4444", "55555"}, true);
            file_segment_paths.emplace_back(holder2->front().getPathInLocalCache());

            ASSERT_EQ(fs::file_size(file_segment_paths.back()), 15);
            ASSERT_TRUE(holder2->front().range() == FileSegment::Range(0, 15));
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
    DB::FileCacheSettings settings;
    settings.max_size = 10_KiB;
    settings.max_file_segment_size = 1_KiB;
    settings.base_path = cache_base_path;

    DB::FileCache file_cache("7", settings);
    file_cache.initialize();

    auto tmp_data_scope = std::make_shared<TemporaryDataOnDiskScope>(nullptr, &file_cache, 0);

    auto some_data_holder = file_cache.getOrSet(file_cache.createKeyForPath("some_data"), 0, 5_KiB, 5_KiB, CreateFileSegmentSettings{});

    {
        ASSERT_EQ(some_data_holder->size(), 5);
        for (auto & segment : *some_data_holder)
        {
            ASSERT_TRUE(segment->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(segment->reserve(segment->range().size()));
            download(*segment);
            segment->complete();
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
    auto key = cache->createKeyForPath(file_path);

    {
        auto cached_buffer = std::make_shared<CachedOnDiskReadBufferFromFile>(
            file_path, key, cache, read_buffer_creator, read_settings, "test", s.size(), false, false, std::nullopt, nullptr);

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
            file_path, key, cache, read_buffer_creator, modified_settings, "test", s.size(), false, false, std::nullopt, nullptr);

        cached_buffer->next();
        assertEqual(cache->dumpQueue(), { Range(5, 9), Range(10, 14), Range(15, 19), Range(20, 24), Range(25, 29), Range(0, 4) });

        cached_buffer->position() = cached_buffer->buffer().end();
        cached_buffer->next();
        assertEqual(cache->dumpQueue(), {Range(10, 14), Range(15, 19), Range(20, 24), Range(25, 29), Range(0, 4), Range(5, 9) });
    }
}
