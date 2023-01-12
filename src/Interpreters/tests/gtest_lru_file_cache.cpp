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
#include <Common/hex.h>
#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <filesystem>
#include <thread>
#include <DataTypes/DataTypesNumber.h>

#include <Poco/ConsoleChannel.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>
#include <Interpreters/Cache/WriteBufferToFileSegment.h>

namespace fs = std::filesystem;
using namespace DB;

static constexpr auto TEST_LOG_LEVEL = "debug";

void assertRange(
    [[maybe_unused]] size_t assert_n, DB::FileSegmentPtr file_segment,
    const DB::FileSegment::Range & expected_range, DB::FileSegment::State expected_state)
{
    auto range = file_segment->range();

    std::cerr << fmt::format("\nAssert #{} : {} == {} (state: {} == {})\n", assert_n,
                             range.toString(), expected_range.toString(),
                             toString(file_segment->state()), toString(expected_state));

    ASSERT_EQ(range.left, expected_range.left);
    ASSERT_EQ(range.right, expected_range.right);
    ASSERT_EQ(file_segment->state(), expected_state);
}

void printRanges(const auto & segments)
{
    std::cerr << "\nHaving file segments: ";
    for (const auto & segment : segments)
        std::cerr << '\n' << segment->range().toString() << " (state: " + DB::FileSegment::stateToString(segment->state()) + ")" << "\n";
}

std::vector<DB::FileSegmentPtr> fromHolder(const DB::FileSegmentsHolder & holder)
{
    return std::vector<DB::FileSegmentPtr>(holder.file_segments.begin(), holder.file_segments.end());
}

String getFileSegmentPath(const String & base_path, const DB::FileCache::Key & key, size_t offset)
{
    auto key_str = key.toString();
    return fs::path(base_path) / key_str.substr(0, 3) / key_str / DB::toString(offset);
}

void download(const std::string & cache_base_path, DB::FileSegmentPtr file_segment)
{
    const auto & key = file_segment->key();
    size_t size = file_segment->range().size();

    auto key_str = key.toString();
    auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) / key_str;
    if (!fs::exists(subdir))
        fs::create_directories(subdir);

    std::string data(size, '0');
    file_segment->write(data.data(), size, file_segment->getCurrentWriteOffset());
}

void prepareAndDownload(const std::string & cache_base_path, DB::FileSegmentPtr file_segment)
{
    ASSERT_TRUE(file_segment->reserve(file_segment->range().size()));
    download(cache_base_path, file_segment);
}

void complete(const std::string & cache_base_path, const DB::FileSegmentsHolder & holder)
{
    for (const auto & file_segment : holder.file_segments)
    {
        ASSERT_TRUE(file_segment->getOrSetDownloader() == DB::FileSegment::getCallerId());
        prepareAndDownload(cache_base_path, file_segment);
        file_segment->completeWithoutState();
    }
}

class FileCacheTest : public ::testing::Test
{
public:

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

    fs::path caches_dir = fs::current_path() / "lru_cache_test";
    std::string cache_base_path = caches_dir / "cache1" / "";
};

TEST_F(FileCacheTest, get)
{
    DB::ThreadStatus thread_status;

    /// To work with cache need query_id and query context.
    std::string query_id = "query_id";
    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(query_id);
    DB::CurrentThread::QueryScope query_scope_holder(query_context);

    DB::FileCacheSettings settings;
    settings.max_size = 30;
    settings.max_elements = 5;

    {
        auto cache = DB::FileCache(cache_base_path, settings);
        cache.initialize();
        auto key = cache.hash("key1");

        {
            auto holder = cache.getOrSet(key, 0, 10, {});  /// Add range [0, 9]
            auto segments = fromHolder(holder);
            /// Range was not present in cache. It should be added in cache as one while file segment.
            ASSERT_EQ(segments.size(), 1);

            assertRange(1, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::EMPTY);

            /// Exception because space not reserved.
            /// EXPECT_THROW(download(segments[0]), DB::Exception);
            /// Exception because space can be reserved only by downloader
            /// EXPECT_THROW(segments[0]->reserve(segments[0]->range().size()), DB::Exception);

            ASSERT_TRUE(segments[0]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(segments[0]->reserve(segments[0]->range().size()));
            assertRange(2, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADING);

            download(cache_base_path, segments[0]);
            segments[0]->completeWithoutState();
            assertRange(3, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADED);
        }

        /// Current cache:    [__________]
        ///                   ^          ^
        ///                   0          9
        ASSERT_EQ(cache.getFileSegmentsNum(), 1);
        ASSERT_EQ(cache.getUsedCacheSize(), 10);

        {
            /// Want range [5, 14], but [0, 9] already in cache, so only [10, 14] will be put in cache.
            auto holder = cache.getOrSet(key, 5, 10, {});
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 2);

            assertRange(4, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADED);
            assertRange(5, segments[1], DB::FileSegment::Range(10, 14), DB::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[1]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            prepareAndDownload(cache_base_path, segments[1]);
            segments[1]->completeWithoutState();
            assertRange(6, segments[1], DB::FileSegment::Range(10, 14), DB::FileSegment::State::DOWNLOADED);
        }

        /// Current cache:    [__________][_____]
        ///                   ^          ^^     ^
        ///                   0          910    14
        ASSERT_EQ(cache.getFileSegmentsNum(), 2);
        ASSERT_EQ(cache.getUsedCacheSize(), 15);

        {
            auto holder = cache.getOrSet(key, 9, 1, {});  /// Get [9, 9]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assertRange(7, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADED);
        }

        {
            auto holder = cache.getOrSet(key, 9, 2, {});  /// Get [9, 10]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 2);
            assertRange(8, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADED);
            assertRange(9, segments[1], DB::FileSegment::Range(10, 14), DB::FileSegment::State::DOWNLOADED);
        }

        {
            auto holder = cache.getOrSet(key, 10, 1, {});  /// Get [10, 10]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assertRange(10, segments[0], DB::FileSegment::Range(10, 14), DB::FileSegment::State::DOWNLOADED);
        }

        complete(cache_base_path, cache.getOrSet(key, 17, 4, {})); /// Get [17, 20]
        complete(cache_base_path, cache.getOrSet(key, 24, 3, {})); /// Get [24, 26]
        /// completeWithState(cache.getOrSet(key, 27, 1, false)); /// Get [27, 27]

        /// Current cache:    [__________][_____]   [____]    [___][]
        ///                   ^          ^^     ^   ^    ^    ^   ^^^
        ///                   0          910    14  17   20   24  2627
        ///
        ASSERT_EQ(cache.getFileSegmentsNum(), 4);
        ASSERT_EQ(cache.getUsedCacheSize(), 22);

        {
            auto holder = cache.getOrSet(key, 0, 26, {}); /// Get [0, 25]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 6);

            assertRange(11, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADED);
            assertRange(12, segments[1], DB::FileSegment::Range(10, 14), DB::FileSegment::State::DOWNLOADED);

            /// Missing [15, 16] should be added in cache.
            assertRange(13, segments[2], DB::FileSegment::Range(15, 16), DB::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[2]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            prepareAndDownload(cache_base_path, segments[2]);

            segments[2]->completeWithoutState();

            assertRange(14, segments[3], DB::FileSegment::Range(17, 20), DB::FileSegment::State::DOWNLOADED);

            /// New [21, 23], but will not be added in cache because of elements limit (5)
            assertRange(15, segments[4], DB::FileSegment::Range(21, 23), DB::FileSegment::State::EMPTY);
            ASSERT_TRUE(segments[4]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_FALSE(segments[4]->reserve(1));

            assertRange(16, segments[5], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);

            /// Current cache:    [__________][_____][   ][____]    [___]
            ///                   ^                            ^    ^
            ///                   0                            20   24
            ///

            /// Range [27, 27] must be evicted in previous getOrSet [0, 25].
            /// Let's not invalidate pointers to returned segments from range [0, 25] and
            /// as max elements size is reached, next attempt to put something in cache should fail.
            /// This will also check that [27, 27] was indeed evicted.

            auto holder1 = cache.getOrSet(key, 27, 1, {});
            auto segments_1 = fromHolder(holder1); /// Get [27, 27]
            ASSERT_EQ(segments_1.size(), 1);
            assertRange(17, segments_1[0], DB::FileSegment::Range(27, 27), DB::FileSegment::State::EMPTY);
        }

        {
            auto holder = cache.getOrSet(key, 12, 10, {}); /// Get [12, 21]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 4);

            assertRange(18, segments[0], DB::FileSegment::Range(10, 14), DB::FileSegment::State::DOWNLOADED);
            assertRange(19, segments[1], DB::FileSegment::Range(15, 16), DB::FileSegment::State::DOWNLOADED);
            assertRange(20, segments[2], DB::FileSegment::Range(17, 20), DB::FileSegment::State::DOWNLOADED);

            assertRange(21, segments[3], DB::FileSegment::Range(21, 21), DB::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[3]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            prepareAndDownload(cache_base_path, segments[3]);

            segments[3]->completeWithoutState();
            ASSERT_TRUE(segments[3]->state() == DB::FileSegment::State::DOWNLOADED);
        }

        /// Current cache:    [_____][__][____][_]   [___]
        ///                   ^          ^       ^   ^   ^
        ///                   10         17      21  24  26

        ASSERT_EQ(cache.getFileSegmentsNum(), 5);

        {
            auto holder = cache.getOrSet(key, 23, 5, {}); /// Get [23, 28]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 3);

            assertRange(22, segments[0], DB::FileSegment::Range(23, 23), DB::FileSegment::State::EMPTY);
            assertRange(23, segments[1], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);
            assertRange(24, segments[2], DB::FileSegment::Range(27, 27), DB::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[0]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(segments[2]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            prepareAndDownload(cache_base_path, segments[0]);
            prepareAndDownload(cache_base_path, segments[2]);
            segments[0]->completeWithoutState();
            segments[2]->completeWithoutState();
        }

        /// Current cache:    [____][_]  [][___][__]
        ///                   ^       ^  ^^^   ^^  ^
        ///                   17      21 2324  26  28

        {
            auto holder5 = cache.getOrSet(key, 2, 3, {}); /// Get [2, 4]
            auto s5 = fromHolder(holder5);
            ASSERT_EQ(s5.size(), 1);
            assertRange(25, s5[0], DB::FileSegment::Range(2, 4), DB::FileSegment::State::EMPTY);

            auto holder1 = cache.getOrSet(key, 30, 2, {}); /// Get [30, 31]
            auto s1 = fromHolder(holder1);
            ASSERT_EQ(s1.size(), 1);
            assertRange(26, s1[0], DB::FileSegment::Range(30, 31), DB::FileSegment::State::EMPTY);

            ASSERT_TRUE(s5[0]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(s1[0]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            prepareAndDownload(cache_base_path, s5[0]);
            prepareAndDownload(cache_base_path, s1[0]);
            s5[0]->completeWithoutState();
            s1[0]->completeWithoutState();

            /// Current cache:    [___]       [_][___][_]   [__]
            ///                   ^   ^       ^  ^   ^  ^   ^  ^
            ///                   2   4       23 24  26 27  30 31

            auto holder2 = cache.getOrSet(key, 23, 1, {}); /// Get [23, 23]
            auto s2 = fromHolder(holder2);
            ASSERT_EQ(s2.size(), 1);

            auto holder3 = cache.getOrSet(key, 24, 3, {}); /// Get [24, 26]
            auto s3 = fromHolder(holder3);
            ASSERT_EQ(s3.size(), 1);

            auto holder4 = cache.getOrSet(key, 27, 1, {}); /// Get [27, 27]
            auto s4 = fromHolder(holder4);
            ASSERT_EQ(s4.size(), 1);

            /// All cache is now unreleasable because pointers are still hold
            auto holder6 = cache.getOrSet(key, 0, 40, {});
            auto f = fromHolder(holder6);
            ASSERT_EQ(f.size(), 9);

            assertRange(27, f[0], DB::FileSegment::Range(0, 1), DB::FileSegment::State::EMPTY);
            assertRange(28, f[2], DB::FileSegment::Range(5, 22), DB::FileSegment::State::EMPTY);
            assertRange(29, f[6], DB::FileSegment::Range(28, 29), DB::FileSegment::State::EMPTY);
            assertRange(30, f[8], DB::FileSegment::Range(32, 39), DB::FileSegment::State::EMPTY);

            ASSERT_TRUE(f[0]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(f[2]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(f[6]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(f[8]->getOrSetDownloader() == DB::FileSegment::getCallerId());

            ASSERT_FALSE(f[0]->reserve(1));
            ASSERT_FALSE(f[2]->reserve(1));
            ASSERT_FALSE(f[6]->reserve(1));
            ASSERT_FALSE(f[8]->reserve(1));
        }

        {
            auto holder = cache.getOrSet(key, 2, 3, {}); /// Get [2, 4]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assertRange(31, segments[0], DB::FileSegment::Range(2, 4), DB::FileSegment::State::DOWNLOADED);
        }

        /// Current cache:    [___]       [_][___][_]   [__]
        ///                   ^   ^       ^  ^   ^  ^   ^  ^
        ///                   2   4       23 24  26 27  30 31

        {
            auto holder = cache.getOrSet(key, 25, 5, {}); /// Get [25, 29]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 3);

            assertRange(32, segments[0], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);
            assertRange(33, segments[1], DB::FileSegment::Range(27, 27), DB::FileSegment::State::DOWNLOADED);

            assertRange(34, segments[2], DB::FileSegment::Range(28, 29), DB::FileSegment::State::EMPTY);
            ASSERT_TRUE(segments[2]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(segments[2]->state() == DB::FileSegment::State::DOWNLOADING);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&]
            {
                DB::ThreadStatus thread_status_1;
                auto query_context_1 = DB::Context::createCopy(getContext().context);
                query_context_1->makeQueryContext();
                query_context_1->setCurrentQueryId("query_id_1");
                DB::CurrentThread::QueryScope query_scope_holder_1(query_context_1);
                thread_status_1.attachQueryContext(query_context_1);

                auto holder_2 = cache.getOrSet(key, 25, 5, {}); /// Get [25, 29] once again.
                auto segments_2 = fromHolder(holder_2);
                ASSERT_EQ(segments.size(), 3);

                assertRange(35, segments_2[0], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);
                assertRange(36, segments_2[1], DB::FileSegment::Range(27, 27), DB::FileSegment::State::DOWNLOADED);
                assertRange(37, segments_2[2], DB::FileSegment::Range(28, 29), DB::FileSegment::State::DOWNLOADING);

                ASSERT_TRUE(segments[2]->getOrSetDownloader() != DB::FileSegment::getCallerId());
                ASSERT_TRUE(segments[2]->state() == DB::FileSegment::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                segments_2[2]->wait();
                ASSERT_TRUE(segments_2[2]->state() == DB::FileSegment::State::DOWNLOADED);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&]{ return lets_start_download; });
            }

            prepareAndDownload(cache_base_path, segments[2]);
            segments[2]->completeWithoutState();
            ASSERT_TRUE(segments[2]->state() == DB::FileSegment::State::DOWNLOADED);

            other_1.join();
        }

        /// Current cache:    [___]       [___][_][__][__]
        ///                   ^   ^       ^   ^  ^^  ^^  ^
        ///                   2   4       24  26 27  2930 31

        {
            /// Now let's check the similar case but getting ERROR state after segment->wait(), when
            /// state is changed not manually via segment->completeWithState(state) but from destructor of holder
            /// and notify_all() is also called from destructor of holder.

            std::optional<DB::FileSegmentsHolder> holder;
            holder.emplace(cache.getOrSet(key, 3, 23, {})); /// Get [3, 25]

            auto segments = fromHolder(*holder);
            ASSERT_EQ(segments.size(), 3);

            assertRange(38, segments[0], DB::FileSegment::Range(2, 4), DB::FileSegment::State::DOWNLOADED);

            assertRange(39, segments[1], DB::FileSegment::Range(5, 23), DB::FileSegment::State::EMPTY);
            ASSERT_TRUE(segments[1]->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(segments[1]->state() == DB::FileSegment::State::DOWNLOADING);

            assertRange(40, segments[2], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&]
            {
                DB::ThreadStatus thread_status_1;
                auto query_context_1 = DB::Context::createCopy(getContext().context);
                query_context_1->makeQueryContext();
                query_context_1->setCurrentQueryId("query_id_1");
                DB::CurrentThread::QueryScope query_scope_holder_1(query_context_1);
                thread_status_1.attachQueryContext(query_context_1);

                auto holder_2 = cache.getOrSet(key, 3, 23, {}); /// Get [3, 25] once again
                auto segments_2 = fromHolder(*holder);
                ASSERT_EQ(segments_2.size(), 3);

                assertRange(41, segments_2[0], DB::FileSegment::Range(2, 4), DB::FileSegment::State::DOWNLOADED);
                assertRange(42, segments_2[1], DB::FileSegment::Range(5, 23), DB::FileSegment::State::DOWNLOADING);
                assertRange(43, segments_2[2], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);

                ASSERT_TRUE(segments_2[1]->getDownloader() != DB::FileSegment::getCallerId());
                ASSERT_TRUE(segments_2[1]->state() == DB::FileSegment::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                segments_2[1]->wait();
                printRanges(segments_2);
                ASSERT_TRUE(segments_2[1]->state() == DB::FileSegment::State::PARTIALLY_DOWNLOADED);

                ASSERT_TRUE(segments_2[1]->getOrSetDownloader() == DB::FileSegment::getCallerId());
                prepareAndDownload(cache_base_path, segments_2[1]);
                segments_2[1]->completeWithoutState();
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&]{ return lets_start_download; });
            }

            holder.reset();
            other_1.join();
            printRanges(segments);
            ASSERT_TRUE(segments[1]->state() == DB::FileSegment::State::DOWNLOADED);
        }
    }

    /// Current cache:    [___][        ][___][_][__]
    ///                   ^   ^^         ^   ^^  ^  ^
    ///                   2   45       24  2627 28 29

    {
        /// Test LRUCache::restore().

        auto cache2 = DB::FileCache(cache_base_path, settings);
        cache2.initialize();
        auto key = cache2.hash("key1");

        auto holder1 = cache2.getOrSet(key, 2, 28, {}); /// Get [2, 29]

        auto segments1 = fromHolder(holder1);
        ASSERT_EQ(segments1.size(), 5);

        assertRange(44, segments1[0], DB::FileSegment::Range(2, 4), DB::FileSegment::State::DOWNLOADED);
        assertRange(45, segments1[1], DB::FileSegment::Range(5, 23), DB::FileSegment::State::DOWNLOADED);
        assertRange(45, segments1[2], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);
        assertRange(46, segments1[3], DB::FileSegment::Range(27, 27), DB::FileSegment::State::DOWNLOADED);
        assertRange(47, segments1[4], DB::FileSegment::Range(28, 29), DB::FileSegment::State::DOWNLOADED);
    }

    {
        /// Test max file segment size

        auto settings2 = settings;
        settings2.max_file_segment_size = 10;
        auto cache2 = DB::FileCache(caches_dir / "cache2", settings2);
        cache2.initialize();
        auto key = cache2.hash("key1");

        auto holder1 = cache2.getOrSet(key, 0, 25, {}); /// Get [0, 24]
        auto segments1 = fromHolder(holder1);

        ASSERT_EQ(segments1.size(), 3);
        assertRange(48, segments1[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::EMPTY);
        assertRange(49, segments1[1], DB::FileSegment::Range(10, 19), DB::FileSegment::State::EMPTY);
        assertRange(50, segments1[2], DB::FileSegment::Range(20, 24), DB::FileSegment::State::EMPTY);
    }

}

TEST_F(FileCacheTest, writeBuffer)
{
    DB::FileCacheSettings settings;
    settings.max_size = 100;
    settings.max_elements = 5;
    settings.max_file_segment_size = 5;

    DB::FileCache cache(cache_base_path, settings);
    cache.initialize();

    auto write_to_cache = [&cache](const String & key, const Strings & data)
    {
        CreateFileSegmentSettings segment_settings;
        segment_settings.kind = FileSegmentKind::Temporary;
        segment_settings.unbounded = true;

        auto holder = cache.set(cache.hash(key), 0, 3, segment_settings);
        EXPECT_EQ(holder.file_segments.size(), 1);
        auto & segment = holder.file_segments.front();
        WriteBufferToFileSegment out(segment.get());
        for (const auto & s : data)
            out.write(s.data(), s.size());
        return holder;
    };

    std::vector<fs::path> file_segment_paths;
    {
        auto holder = write_to_cache("key1", {"abc", "defg"});
        file_segment_paths.emplace_back(holder.file_segments.front()->getPathInLocalCache());

        ASSERT_EQ(fs::file_size(file_segment_paths.back()), 7);
        ASSERT_TRUE(holder.file_segments.front()->range() == FileSegment::Range(0, 7));
        ASSERT_EQ(cache.getUsedCacheSize(), 7);

        {
            auto holder2 = write_to_cache("key2", {"1", "22", "333", "4444", "55555"});
            file_segment_paths.emplace_back(holder2.file_segments.front()->getPathInLocalCache());

            ASSERT_EQ(fs::file_size(file_segment_paths.back()), 15);
            ASSERT_TRUE(holder2.file_segments.front()->range() == FileSegment::Range(0, 15));
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

    DB::FileCache file_cache(cache_base_path, settings);
    file_cache.initialize();

    auto tmp_data_scope = std::make_shared<TemporaryDataOnDiskScope>(nullptr, &file_cache, 0);

    auto some_data_holder = file_cache.getOrSet(file_cache.hash("some_data"), 0, 5_KiB, CreateFileSegmentSettings{});

    {
        auto segments = fromHolder(some_data_holder);
        ASSERT_EQ(segments.size(), 5);
        for (auto & segment : segments)
        {
            ASSERT_TRUE(segment->getOrSetDownloader() == DB::FileSegment::getCallerId());
            ASSERT_TRUE(segment->reserve(segment->range().size()));
            download(cache_base_path, segment);
            segment->completeWithoutState();
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
