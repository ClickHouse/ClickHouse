#include "gtest_file_cache.h"

namespace fs = std::filesystem;

fs::path caches_dir = fs::current_path() / "file_cache_test";
String cache_base_path = caches_dir / "cache1" / "";

void assertRange(
    [[maybe_unused]] size_t assert_n,
    DB::FileSegmentPtr file_segment,
    const DB::FileSegment::Range & expected_range,
    DB::FileSegment::State expected_state)
{
    auto range = file_segment->range();

    std::cerr << fmt::format(
        "\nAssert #{} : {} == {} (state: {} == {})\n",
        assert_n,
        range.toString(),
        expected_range.toString(),
        toString(file_segment->state()),
        toString(expected_state));

    ASSERT_EQ(range.left, expected_range.left);
    ASSERT_EQ(range.right, expected_range.right);
    ASSERT_EQ(file_segment->state(), expected_state);
};

void printRanges(std::vector<DB::FileSegmentPtr> & segments)
{
    std::cerr << "\nHaving file segments: ";
    for (const auto & segment : segments)
        std::cerr << '\n'
                  << segment->range().toString() << " (state: " + DB::FileSegment::stateToString(segment->state()) + ")"
                  << "\n";
}

std::vector<DB::FileSegmentPtr> fromHolder(const DB::FileSegmentsHolder & holder)
{
    return std::vector<DB::FileSegmentPtr>(holder.file_segments.begin(), holder.file_segments.end());
}

String keyToStr(const DB::IFileCache::Key & key)
{
    return getHexUIntLowercase(key);
}

String getFileSegmentPath(const String & base_path, const DB::IFileCache::Key & key, size_t offset)
{
    auto key_str = keyToStr(key);
    return fs::path(base_path) / key_str.substr(0, 3) / key_str / DB::toString(offset);
}

void download(DB::FileSegmentPtr file_segment)
{
    const auto & key = file_segment->key();
    size_t size = file_segment->range().size();

    auto key_str = keyToStr(key);
    auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) / key_str;
    if (!fs::exists(subdir))
        fs::create_directories(subdir);

    std::string data(size, '0');
    file_segment->write(data.data(), size, file_segment->getDownloadOffset());
}

void prepareAndDownload(DB::FileSegmentPtr file_segment)
{
    // std::cerr << "Reserving: " << file_segment->range().size() << " for: " << file_segment->range().toString() << "\n";
    ASSERT_TRUE(file_segment->reserve(file_segment->range().size()));
    download(file_segment);
}

void complete(const DB::FileSegmentsHolder & holder)
{
    for (const auto & file_segment : holder.file_segments)
    {
        ASSERT_TRUE(file_segment->getOrSetDownloader() == DB::FileSegment::getCallerId());
        prepareAndDownload(file_segment);
        file_segment->complete(DB::FileSegment::State::DOWNLOADED);
    }
}
