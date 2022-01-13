#include <iomanip>
#include <iostream>
#include <gtest/gtest.h>
#include <Common/FileCache.h>
#include <Common/filesystemHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <filesystem>
#include <thread>

namespace fs = std::filesystem;

String cache_base_path = fs::current_path() / "test_lru_file_cache" / "";

void assertRange(
    size_t assert_n, DB::FileSegmentPtr file_segment,
    const DB::FileSegment::Range & expected_range, DB::FileSegment::State expected_state)
{
    auto range = file_segment->range();

    std::cerr << fmt::format("\nAssert #{} : {} == {} (state: {} == {})\n", assert_n,
                             range.toString(), expected_range.toString(),
                             toString(file_segment->state()), toString(expected_state));

    ASSERT_EQ(range.left, expected_range.left);
    ASSERT_EQ(range.right, expected_range.right);
    ASSERT_EQ(file_segment->state(), expected_state);
};

void printRanges(const auto & segments)
{
    std::cerr << "\nHaving file segments: ";
    for (const auto & segment : segments)
        std::cerr << '\n' << segment->range().toString() << "\n";
}

std::vector<DB::FileSegmentPtr> fromHolder(const DB::FileSegmentsHolder & holder)
{
    return std::vector<DB::FileSegmentPtr>(holder.file_segments.begin(), holder.file_segments.end());
}

String getFileSegmentPath(const String & base_path, const String & key, size_t offset)
{
    return fs::path(base_path) / key.substr(0, 3) / key / DB::toString(offset);
}

void download(DB::FileSegmentPtr file_segment)
{
    const auto & key = file_segment->key();
    auto offset = file_segment->range().left;
    size_t size = file_segment->range().size();

    auto path = getFileSegmentPath(cache_base_path, key, offset);
    auto subdir = fs::path(cache_base_path) / key.substr(0, 3) / key;
    if (!fs::exists(subdir))
        fs::create_directories(subdir);

    DB::WriteBufferFromFile file_buf(path);
    std::string data(size, '0');
    DB::writeString(data, file_buf);
}

void complete(const DB::FileSegmentsHolder & holder)
{
    for (const auto & file_segment : holder.file_segments)
    {
        download(file_segment);
        file_segment->complete(DB::FileSegment::State::DOWNLOADED);
    }
}


TEST(LRUFileCache, get)
{
    if (fs::exists(cache_base_path))
        fs::remove_all(cache_base_path);
    fs::create_directory(cache_base_path);

    auto cache = DB::LRUFileCache(cache_base_path, 30, 5);
    auto key = "key1";

    {
        auto holder = cache.getOrSet(key, 0, 10);  /// Add range [0, 9]
        auto segments = fromHolder(holder);
        /// Range was not present in cache. It should be added in cache as one while file segment.
        ASSERT_EQ(segments.size(), 1);

        assertRange(1, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADING);

        download(segments[0]);
        segments[0]->complete(DB::FileSegment::State::DOWNLOADED);
    }

    /// Current cache:    [__________]
    ///                   ^          ^
    ///                   0          9

    {
        /// Want range [5, 14], but [0, 9] already in cache, so only [10, 14] will be put in cache.
        auto holder = cache.getOrSet(key, 5, 10);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 2);

        assertRange(2, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADED);
        assertRange(3, segments[1], DB::FileSegment::Range(10, 14), DB::FileSegment::State::DOWNLOADING);

        download(segments[1]);
        segments[1]->complete(DB::FileSegment::State::DOWNLOADED);
    }

    /// Current cache:    [__________][_____]
    ///                   ^          ^^     ^
    ///                   0          910    14

    {
        auto holder = cache.getOrSet(key, 9, 1);  /// Get [9, 9]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assertRange(4, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADED);
    }

    {
        auto holder = cache.getOrSet(key, 9, 2);  /// Get [9, 10]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 2);
        assertRange(5, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADED);
        assertRange(6, segments[1], DB::FileSegment::Range(10, 14), DB::FileSegment::State::DOWNLOADED);
    }

    {
        auto holder = cache.getOrSet(key, 10, 1);  /// Get [10, 10]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assertRange(7, segments[0], DB::FileSegment::Range(10, 14), DB::FileSegment::State::DOWNLOADED);
    }

    complete(cache.getOrSet(key, 17, 4)); /// Get [17, 20]
    complete(cache.getOrSet(key, 24, 3)); /// Get [24, 26]
    complete(cache.getOrSet(key, 27, 1)); /// Get [27, 27]

    /// Current cache:    [__________][_____]   [____]    [___][]
    ///                   ^          ^^     ^   ^    ^    ^   ^^^
    ///                   0          910    14  17   20   24  2627
    ///

    {
        auto holder = cache.getOrSet(key, 0, 26); /// Get [0, 25]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 6);

        assertRange(8, segments[0], DB::FileSegment::Range(0, 9), DB::FileSegment::State::DOWNLOADED);
        assertRange(9, segments[1], DB::FileSegment::Range(10, 14), DB::FileSegment::State::DOWNLOADED);

        /// Missing [15, 16] should be added in cache.
        assertRange(10, segments[2], DB::FileSegment::Range(15, 16), DB::FileSegment::State::DOWNLOADING);
        download(segments[2]);
        segments[2]->complete(DB::FileSegment::State::DOWNLOADED);

        assertRange(11, segments[3], DB::FileSegment::Range(17, 20), DB::FileSegment::State::DOWNLOADED);

        /// New [21, 23], but will not be added in cache because of elements limit (5)
        assertRange(12, segments[4], DB::FileSegment::Range(21, 23), DB::FileSegment::State::EMPTY);

        assertRange(13, segments[5], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);

        /// Current cache:    [__________][_____][   ][____]    [___]
        ///                   ^                            ^    ^
        ///                   0                            20   24
        ///

        /// Range [27, 27] must be evicted in previous getOrSet [0, 25].
        /// Let's not invalidate pointers to returned segments from range [0, 25] and
        /// as max elements size is reached, next attempt to put something in cache should fail.
        /// This will also check that [27, 27] was indeed evicted.

        auto holder1 = cache.getOrSet(key, 27, 1);
        auto segments_1 = fromHolder(holder1); /// Get [27, 27]
        ASSERT_EQ(segments_1.size(), 1);
        assertRange(12, segments_1[0], DB::FileSegment::Range(27, 27), DB::FileSegment::State::EMPTY);
    }

    {
        auto holder = cache.getOrSet(key, 12, 10); /// Get [12, 21]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 4);

        assertRange(14, segments[0], DB::FileSegment::Range(10, 14), DB::FileSegment::State::DOWNLOADED);
        assertRange(15, segments[1], DB::FileSegment::Range(15, 16), DB::FileSegment::State::DOWNLOADED);
        assertRange(16, segments[2], DB::FileSegment::Range(17, 20), DB::FileSegment::State::DOWNLOADED);

        assertRange(17, segments[3], DB::FileSegment::Range(21, 21), DB::FileSegment::State::DOWNLOADING);
        download(segments[3]);
        segments[3]->complete(DB::FileSegment::State::DOWNLOADED);
    }

    /// Current cache:    [_____][__][____][_]   [___]
    ///                   ^          ^       ^   ^   ^
    ///                   10         17      21  24  26

    ASSERT_EQ(cache.getStat().size, 5);

    {
        auto holder = cache.getOrSet(key, 23, 5); /// Get [23, 28]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 3);

        assertRange(18, segments[0], DB::FileSegment::Range(23, 23), DB::FileSegment::State::DOWNLOADING);
        assertRange(19, segments[1], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);
        assertRange(20, segments[2], DB::FileSegment::Range(27, 27), DB::FileSegment::State::DOWNLOADING);

        download(segments[0]);
        download(segments[2]);
        segments[0]->complete(DB::FileSegment::State::DOWNLOADED);
        segments[2]->complete(DB::FileSegment::State::DOWNLOADED);
    }

    /// Current cache:    [____][_]  [][___][__]
    ///                   ^       ^  ^^^   ^^  ^
    ///                   17      21 2324  26  28

    {
        auto holder5 = cache.getOrSet(key, 2, 3); /// Get [2, 4]
        auto s5 = fromHolder(holder5);
        ASSERT_EQ(s5.size(), 1);
        assertRange(21, s5[0], DB::FileSegment::Range(2, 4), DB::FileSegment::State::DOWNLOADING);

        auto holder1 = cache.getOrSet(key, 30, 2); /// Get [30, 31]
        auto s1 = fromHolder(holder1);
        ASSERT_EQ(s1.size(), 1);
        assertRange(22, s1[0], DB::FileSegment::Range(30, 31), DB::FileSegment::State::DOWNLOADING);

        auto holder1_1 = cache.getOrSet(key, 30, 2); /// Get [30, 31] once again.
        auto s1_1 = fromHolder(holder1);
        ASSERT_EQ(s1.size(), 1);
        assertRange(22, s1_1[0], DB::FileSegment::Range(30, 31), DB::FileSegment::State::DOWNLOADING);

        download(s5[0]);
        download(s1[0]);
        s5[0]->complete(DB::FileSegment::State::DOWNLOADED);
        s1[0]->complete(DB::FileSegment::State::DOWNLOADED);

        /// Current cache:    [___]       [_][___][_]   [__]
        ///                   ^   ^       ^  ^   ^  ^   ^  ^
        ///                   2   4       23 24  26 27  30 31

        auto holder2 = cache.getOrSet(key, 23, 1); /// Get [23, 23]
        auto s2 = fromHolder(holder2);
        ASSERT_EQ(s2.size(), 1);
        auto holder3 = cache.getOrSet(key, 24, 3); /// Get [24, 26]
        auto s3 = fromHolder(holder3);
        ASSERT_EQ(s3.size(), 1);
        auto holder4 = cache.getOrSet(key, 27, 1); /// Get [27, 27]
        auto s4 = fromHolder(holder4);
        ASSERT_EQ(s4.size(), 1);

        /// All cache is now unreleasable because pointers are stil hold
        auto holder6 = cache.getOrSet(key, 0, 40);
        auto f = fromHolder(holder6);
        ASSERT_EQ(f.size(), 9);

        assertRange(23, f[0], DB::FileSegment::Range(0, 1), DB::FileSegment::State::EMPTY);
        assertRange(24, f[2], DB::FileSegment::Range(5, 22), DB::FileSegment::State::EMPTY);
        assertRange(25, f[6], DB::FileSegment::Range(28, 29), DB::FileSegment::State::EMPTY);
        assertRange(26, f[8], DB::FileSegment::Range(32, 39), DB::FileSegment::State::EMPTY);
    }

    {
        auto holder = cache.getOrSet(key, 2, 3); /// Get [2, 4]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assertRange(27, segments[0], DB::FileSegment::Range(2, 4), DB::FileSegment::State::DOWNLOADED);
    }

    /// Current cache:    [___]       [_][___][_]   [__]
    ///                   ^   ^       ^  ^   ^  ^   ^  ^
    ///                   2   4       23 24  26 27  30 31

    {
        auto holder = cache.getOrSet(key, 25, 5); /// Get [25, 29]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 3);

        assertRange(28, segments[0], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);
        assertRange(29, segments[1], DB::FileSegment::Range(27, 27), DB::FileSegment::State::DOWNLOADED);
        assertRange(30, segments[2], DB::FileSegment::Range(28, 29), DB::FileSegment::State::DOWNLOADING);

        ASSERT_TRUE(segments[2]->isDownloader());

        bool lets_start_download = false;
        std::mutex mutex;
        std::condition_variable cv;

        std::thread other_1([&]
        {
            auto holder_2 = cache.getOrSet(key, 25, 5); /// Get [25, 29] once again.
            auto segments_2 = fromHolder(holder_2);
            ASSERT_EQ(segments.size(), 3);

            assertRange(31, segments_2[0], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);
            assertRange(32, segments_2[1], DB::FileSegment::Range(27, 27), DB::FileSegment::State::DOWNLOADED);
            assertRange(33, segments_2[2], DB::FileSegment::Range(28, 29), DB::FileSegment::State::DOWNLOADING);

            ASSERT_TRUE(!segments[2]->isDownloader());
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

        download(segments[2]);
        segments[2]->complete(DB::FileSegment::State::DOWNLOADED);
        ASSERT_TRUE(segments[2]->state() == DB::FileSegment::State::DOWNLOADED);

        other_1.join();
    }

    /// Current cache:    [___]       [___][_][__][__]
    ///                   ^   ^       ^   ^  ^^  ^^  ^
    ///                   2   4       24  26 27  2930 31

    {
        /// Now let's check the similar case but getting ERROR state after segment->wait(), when
        /// state is changed not manually via segment->complete(state) but from destructor of holder
        /// and notify_all() is also called from destructor of holder.

        auto holder = cache.getOrSet(key, 3, 23); /// Get [3, 25]
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 3);

        assertRange(34, segments[0], DB::FileSegment::Range(2, 4), DB::FileSegment::State::DOWNLOADED);
        assertRange(35, segments[1], DB::FileSegment::Range(5, 23), DB::FileSegment::State::DOWNLOADING);
        assertRange(36, segments[2], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);

        ASSERT_TRUE(segments[1]->isDownloader());

        bool lets_start_download = false;
        std::mutex mutex;
        std::condition_variable cv;

        std::thread other_1([&]
        {
            auto holder_2 = cache.getOrSet(key, 3, 23); /// Get [3, 25] once again
            auto segments_2 = fromHolder(holder);
            ASSERT_EQ(segments_2.size(), 3);

            assertRange(37, segments_2[0], DB::FileSegment::Range(2, 4), DB::FileSegment::State::DOWNLOADED);
            assertRange(38, segments_2[1], DB::FileSegment::Range(5, 23), DB::FileSegment::State::DOWNLOADING);
            assertRange(39, segments_2[2], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);

            ASSERT_TRUE(!segments_2[1]->isDownloader());
            ASSERT_TRUE(segments_2[1]->state() == DB::FileSegment::State::DOWNLOADING);

            {
                std::lock_guard lock(mutex);
                lets_start_download = true;
            }
            cv.notify_one();

            segments_2[1]->wait();
            ASSERT_TRUE(segments_2[1]->state() == DB::FileSegment::State::ERROR);
        });

        {
            std::unique_lock lock(mutex);
            cv.wait(lock, [&]{ return lets_start_download; });
        }

        holder.~FileSegmentsHolder();
        other_1.join();
        ASSERT_TRUE(segments[1]->state() == DB::FileSegment::State::ERROR);
    }

    /// Current cache:    [___]         [___][_][__]
    ///                   ^   ^         ^   ^^  ^  ^
    ///                   2   4        24  2627 28 29

    {
        /// Test LRUCache::restore().

        auto cache2 = DB::LRUFileCache(cache_base_path, 30, 5);
        ASSERT_EQ(cache2.getStat().size, 4);

        auto holder1 = cache2.getOrSet(key, 2, 3); /// Get [2, 4]
        auto segments1 = fromHolder(holder1);
        assertRange(40, segments1[0], DB::FileSegment::Range(2, 4), DB::FileSegment::State::DOWNLOADED);

        auto holder2 = cache2.getOrSet(key, 24, 6); /// Get [24, 29]
        auto segments2 = fromHolder(holder2);
        assertRange(41, segments2[0], DB::FileSegment::Range(24, 26), DB::FileSegment::State::DOWNLOADED);
        assertRange(42, segments2[1], DB::FileSegment::Range(27, 27), DB::FileSegment::State::DOWNLOADED);
        assertRange(43, segments2[2], DB::FileSegment::Range(28, 29), DB::FileSegment::State::DOWNLOADED);
    }
}
