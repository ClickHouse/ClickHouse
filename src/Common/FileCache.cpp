#include "FileCache.h"

#include <Common/randomSeed.h>
#include <Common/SipHash.h>
#include <Common/hex.h>
#include <IO/ReadHelpers.h>
#include <pcg-random/pcg_random.hpp>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace
{
    String keyToStr(const FileCache::Key & key)
    {
        return getHexUIntLowercase(key);
    }
}

FileCache::FileCache(const String & cache_base_path_, size_t max_size_, size_t max_element_size_)
    : cache_base_path(cache_base_path_), max_size(max_size_), max_element_size(max_element_size_)
{
}

FileCache::Key FileCache::hash(const String & path)
{
    return sipHash128(path.data(), path.size());
}

String FileCache::path(const Key & key, size_t offset)
{
    auto key_str = keyToStr(key);
    return fs::path(cache_base_path) / key_str.substr(0, 3) / key_str / std::to_string(offset);
}

String FileCache::path(const Key & key)
{
    auto key_str = keyToStr(key);
    return fs::path(cache_base_path) / key_str.substr(0, 3) / key_str;
}

LRUFileCache::LRUFileCache(const String & cache_base_path_, size_t max_size_, size_t max_element_size_)
    : FileCache(cache_base_path_, max_size_, max_element_size_), log(&Poco::Logger::get("LRUFileCache"))
{
    if (fs::exists(cache_base_path))
        restore();
    else
        fs::create_directories(cache_base_path);
}

void LRUFileCache::useCell(
    const FileSegmentCell & cell, FileSegments & result, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
{
    assert(cell.file_segment->state() == FileSegment::State::DOWNLOADED
           || cell.file_segment->state() == FileSegment::State::DOWNLOADING);

    result.push_back(cell.file_segment);
    /// Move to the end of the queue. The iterator remains valid.
    queue.splice(queue.end(), queue, cell.queue_iterator);
}

LRUFileCache::FileSegmentCell * LRUFileCache::getCell(
    const Key & key, size_t offset, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
{
    auto it = files.find(key);
    if (it == files.end())
        return nullptr;

    auto & offsets = it->second;
    auto cell_it = offsets.find(offset);
    if (cell_it == offsets.end())
        return nullptr;

    return &cell_it->second;
}

void LRUFileCache::removeCell(
    const Key & key, size_t offset, const LRUQueueIterator & queue_iterator, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
{
    LOG_TEST(log, "Remove. Key: {}, offset: {}", keyToStr(key), offset);

    queue.erase(queue_iterator);
    auto & offsets = files[key];
    offsets.erase(offset);
    if (offsets.empty())
        files.erase(key);
}

FileSegments LRUFileCache::getImpl(
    const Key & key, const FileSegment::Range & range, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
{
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    auto it = files.find(key);
    if (it == files.end())
        return {};

    const auto & file_segments = it->second;
    if (file_segments.empty())
    {
        files.erase(it);
        return {};
    }

    FileSegments result;
    auto segment_it = file_segments.lower_bound(range.left);
    if (segment_it == file_segments.end())
    {
        /// N - last cached segment for given file key, segment{N}.offset < range.left:
        ///   segment{N}                       segment{N}
        /// [________                         [_______]
        ///     [__________]         OR                  [________]
        ///     ^                                        ^
        ///     range.left                               range.left

        const auto & cell = (--file_segments.end())->second;
        if (cell.file_segment->range().right < range.left)
            return {};

        useCell(cell, result, cache_lock);
    }
    else /// segment_it <-- segmment{k}
    {
        if (segment_it != file_segments.begin())
        {
            const auto & prev_cell = std::prev(segment_it)->second;
            const auto & prev_cell_range = prev_cell.file_segment->range();

            if (range.left <= prev_cell_range.right)
            {
                ///   segment{k-1}  segment{k}
                ///   [________]   [_____
                ///       [___________
                ///       ^
                ///       range.left

                useCell(prev_cell, result, cache_lock);
            }
        }

        ///  segment{k} ...       segment{k-1}  segment{k}                      segment{k}
        ///  [______              [______]     [____                        [________
        ///  [_________     OR              [________      OR    [______]   ^
        ///  ^                              ^                           ^   segment{k}.offset
        ///  range.left                     range.left                  range.right

        while (segment_it != file_segments.end())
        {
            const auto & cell = segment_it->second;
            if (range.right < cell.file_segment->range().left)
                break;
            useCell(cell, result, cache_lock);
            ++segment_it;
        }
    }

    /// TODO: remove this extra debug logging.
    String ranges;
    for (const auto & s : result)
        ranges += s->range().toString() + " ";
    LOG_TEST(log, "Cache get. Key: {}, range: {}, file_segments number: {}, ranges: {}",
             keyToStr(key), range.toString(), result.size(), ranges);

    return result;
}

FileSegmentsHolder LRUFileCache::getOrSet(const Key & key, size_t offset, size_t size)
{
    FileSegment::Range range(offset, offset + size - 1);

    std::lock_guard cache_lock(mutex);

    /// Get all segments which intersect with the given range.
    auto file_segments = getImpl(key, range, cache_lock);

    if (file_segments.empty())
    {
        /// If there are no such file segments, try to reserve space for
        /// range [offset, offset + size) and put it in cache.

        auto * cell = setImpl(key, range.left, range.size(),  cache_lock);
        if (cell)
            file_segments = {cell->file_segment};
        else
            file_segments = {FileSegment::createEmpty(offset, size, key, this)};
    }
    else
    {
        /// There are segments [segment1, ..., segmentN]
        /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
        /// intersect with given range.

        /// It can have holes:
        /// [____________________]         -- requested range
        ///     [____]  [_]   [_________]  -- intersecting cache [segment1, ..., segmentN]

        /// As long as there is space in cache, try to reserve space and
        /// create a cache cell for range correspong to each hole.

        auto it = file_segments.begin();
        auto segment_range = (*it)->range();

        size_t current_pos;
        if (segment_range.left < range.left)
        {
            ///    [_______     -- requested range
            /// [_______
            /// ^
            /// segment1

            current_pos = segment_range.right + 1;
            ++it;
        }
        else
            current_pos = range.left;

        while (current_pos <= range.right && it != file_segments.end())
        {
            segment_range = (*it)->range();

            if (current_pos == segment_range.left)
            {
                current_pos = segment_range.right + 1;
                ++it;
                continue;
            }

            assert(current_pos < segment_range.left);

            auto hole_size = segment_range.left - current_pos;
            auto * cell = setImpl(key, current_pos, hole_size, cache_lock);
            if (cell)
                file_segments.insert(it, cell->file_segment);
            else
                file_segments.insert(it, FileSegment::createEmpty(current_pos, hole_size, key, this));

            current_pos = segment_range.right + 1;
            ++it;
        }

        if (current_pos <= range.right)
        {
            ///   ________]     -- requested range
            ///   _____]
            ///        ^
            /// segmentN

            auto hole_size = range.right - current_pos + 1;
            auto * cell = setImpl(key, current_pos, hole_size, cache_lock);
            if (cell)
                file_segments.push_back(cell->file_segment);
            else
                file_segments.push_back(FileSegment::createEmpty(current_pos, hole_size, key, this));
        }
    }

    return FileSegmentsHolder(std::move(file_segments));
}

LRUFileCache::FileSegmentCell * LRUFileCache::setImpl(
    const Key & key, size_t offset, size_t size, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
{
    if (!size)
        return nullptr; /// Empty files are not cached.

    LOG_TEST(log, "Set. Key: {}, offset: {}, size: {}", keyToStr(key), offset, size);

    if (files[key].contains(offset))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cache already exists for key: `{}`, offset: {}, size: {}", keyToStr(key), offset, size);

    bool reserved = tryReserve(size, cache_lock);
    if (!reserved)
        return nullptr;

    FileSegmentCell cell(
        std::make_shared<FileSegment>(offset, size, key, this),
        queue.insert(queue.end(), std::make_pair(key, offset)));

    auto & offsets = files[key];

    if (offsets.empty())
    {
        auto key_path = path(key);
        if (!fs::exists(key_path))
            fs::create_directories(key_path);
    }

    auto [it, inserted] = offsets.insert({offset, std::move(cell)});
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Failed to insert into cache key: `{}`, offset: {}, size: {}", keyToStr(key), offset, size);

    return &(it->second);
}

bool LRUFileCache::tryReserve(size_t size, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
{
    auto queue_size = queue.size() + 1;
    auto removed_size = 0;

    auto is_overflow = [&]
    {
        return (current_size + size - removed_size > max_size)
            || (max_element_size != 0 && queue_size > max_element_size);
    };

    std::vector<FileSegment *> to_evict;

    auto key_it = queue.begin();
    while (is_overflow() && key_it != queue.end())
    {
        const auto [key, offset] = *key_it++;

        auto * cell = getCell(key, offset, cache_lock);
        if (!cell)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cache became inconsistent. Key: {}, offset: {}", keyToStr(key), offset);

        size_t cell_size = cell->size();

        /// It is guaranteed that cell is not removed from cache as long as
        /// pointer to corresponding file segment is hold by any other thread.

        if (cell->releasable())
        {
            switch (cell->file_segment->state())
            {
                case FileSegment::State::DOWNLOADED:
                {
                    /// Cell will actually be removed only if
                    /// we managed to reserve enough space.

                    to_evict.emplace_back(cell->file_segment.get());
                    break;
                }
                default:
                {
                    removeCell(key, offset, cell->queue_iterator, cache_lock);
                    break;
                }
            }

            removed_size += cell_size;
            --queue_size;
        }
    }

    if (is_overflow())
        return false;

    for (auto & file_segment : to_evict)
        remove(*file_segment, cache_lock);

    current_size += size - removed_size;
    if (current_size > (1ull << 63))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache became inconsistent. There must be a bug");

    return true;
}

void LRUFileCache::remove(const FileSegment & file_segment, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
{
    const auto & key = file_segment.key();
    auto offset = file_segment.range().left;

    const auto * cell = getCell(key, offset, cache_lock);
    if (!cell)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Attempt to remove cell which is not in cache. Key: {}, offset: {}, segment state: {}",
            keyToStr(key), offset, file_segment.state());

    removeImpl(key, offset, cell->queue_iterator, cache_lock);
}

void LRUFileCache::removeImpl(
    const Key & key, size_t offset, const LRUQueueIterator & queue_iterator,
    [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
{
    removeCell(key, offset, queue_iterator, cache_lock);

    auto cache_file_path = path(key, offset);
    if (fs::exists(cache_file_path))
    {
        try
        {
            fs::remove(cache_file_path);

            /// If we just removed the last file segment -- also remove key directory.
            if (files.find(key) == files.end())
            {
                auto key_path = path(key);
                fs::remove(key_path);
            }
        }
        catch (...)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Removal of cached file failed. Key: {}, offset: {}, path: {}, error: {}",
                            keyToStr(key), offset, cache_file_path, getCurrentExceptionMessage(false));
        }
    }
}

void LRUFileCache::restore()
{
    std::lock_guard cache_lock(mutex);

    Key key;
    UInt64 offset;
    size_t size;
    std::vector<FileSegmentCell *> cells;

    /// cache_base_path / key_prefix / key / offset

    fs::directory_iterator key_prefix_it{cache_base_path};
    for (; key_prefix_it != fs::directory_iterator(); ++key_prefix_it)
    {
        fs::directory_iterator key_it{key_prefix_it->path()};
        for (; key_it != fs::directory_iterator(); ++key_it)
        {
            key = hash(key_it->path().filename());

            fs::directory_iterator offset_it{key_it->path()};
            for (; offset_it != fs::directory_iterator(); ++offset_it)
            {
                bool parsed = tryParse<UInt64>(offset, offset_it->path().filename());
                if (!parsed)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected file in cache: cannot parse offset. Path: {}", key_it->path().string());

                size = offset_it->file_size();

                auto * cell = setImpl(key, offset, size, cache_lock);
                if (cell)
                {
                    cell->file_segment->download_state = FileSegment::State::DOWNLOADED;
                    cell->file_segment->downloader = 0;

                    cells.push_back(cell);
                }
                else
                {
                    LOG_WARNING(log,
                                "Cache capacity changed (max size: {}, available: {}), cached file `{}` does not fit in cache anymore (size: {})",
                                max_size, available(), key_it->path().string(), size);
                    fs::remove(path(key, offset));
                }
            }
        }
    }

    /// Shuffle cells to have random order in LRUQueue as at startup all cells have the same priority.
    pcg64 generator(randomSeed());
    std::shuffle(cells.begin(), cells.end(), generator);
    for (const auto & cell : cells)
        queue.splice(queue.end(), queue, cell->queue_iterator);
}

void LRUFileCache::remove(const Key & key)
{
    std::lock_guard cache_lock(mutex);

    auto it = files.find(key);
    if (it == files.end())
        return;

    const auto & offsets = it->second;

    for (const auto & [offset, cell] : offsets)
        removeImpl(key, offset, cell.queue_iterator, cache_lock);

    files.erase(it);
}

LRUFileCache::Stat LRUFileCache::getStat()
{
    std::lock_guard cache_lock(mutex);

    Stat stat
    {
        .size = queue.size(),
        .available = available(),
        .downloaded_size = 0,
        .downloading_size = 0,
    };

    for (const auto & [key, offset] : queue)
    {
        const auto * cell = getCell(key, offset, cache_lock);
        if (!cell)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cache became inconsistent. Key: {}, offset: {}", keyToStr(key), offset);

        switch (cell->file_segment->state())
        {
            case FileSegment::State::DOWNLOADED:
            {
                ++stat.downloaded_size;
                break;
            }
            case FileSegment::State::DOWNLOADING:
            {
                ++stat.downloading_size;
                break;
            }
            default:
                break;
        }
    }

    return stat;
}

void FileSegment::complete(State state)
{
    if (state != State::DOWNLOADED && state != State::ERROR)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can complete segment only with DOWNLOADED or ERROR state");

    {
        std::lock_guard segment_lock(mutex);

        if (download_state == State::EMPTY)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot complete segment from EMPTY state");

        std::lock_guard cache_lock(cache->mutex);

        download_state = state;
        downloader = 0;

        if (state == State::ERROR)
            cache->remove(*this, cache_lock);
    }

    cv.notify_all();
}

FileSegment::State FileSegment::wait()
{
    std::unique_lock segment_lock(mutex);

    switch (download_state)
    {
        case State::DOWNLOADING:
        {
            cv.wait(segment_lock, [this]
            {
                return download_state == State::DOWNLOADED || download_state == State::ERROR;
            });
            break;
        }
        case State::DOWNLOADED:[[fallthrough]];
        case State::ERROR:
        {
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to wait for segment with incorrect");
        }
    }

    return download_state;
}

void FileSegment::release()
{
    {
        std::lock_guard segment_lock(mutex);

        /// Empty segments are owned only by caller, not present in cache.
        if (download_state == State::EMPTY)
            return;

        if (download_state != State::DOWNLOADED)
        {
            /// Segment is removed from cache here by downloader's FileSegmentsHolder only if
            /// downloader did not call segment->complete(State::ERROR), otherwise it is removed by downloader.

            std::lock_guard cache_lock(cache->mutex);

            download_state = State::ERROR;
            cache->remove(*this, cache_lock);
        }
    }

    cv.notify_all();
}

}
