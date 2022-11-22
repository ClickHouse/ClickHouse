#include "FileCache.h"

#include <Common/randomSeed.h>
#include <Common/SipHash.h>
#include <Common/hex.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <pcg-random/pcg_random.hpp>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int REMOTE_FS_OBJECT_CACHE_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace
{
    String keyToStr(const IFileCache::Key & key)
    {
        return getHexUIntLowercase(key);
    }
}

IFileCache::IFileCache(
    const String & cache_base_path_,
    size_t max_size_,
    size_t max_element_size_,
    size_t max_file_segment_size_)
    : cache_base_path(cache_base_path_)
    , max_size(max_size_)
    , max_element_size(max_element_size_)
    , max_file_segment_size(max_file_segment_size_)
{
}

IFileCache::Key IFileCache::hash(const String & path)
{
    return sipHash128(path.data(), path.size());
}

String IFileCache::getPathInLocalCache(const Key & key, size_t offset)
{
    auto key_str = keyToStr(key);
    return fs::path(cache_base_path) / key_str.substr(0, 3) / key_str / std::to_string(offset);
}

String IFileCache::getPathInLocalCache(const Key & key)
{
    auto key_str = keyToStr(key);
    return fs::path(cache_base_path) / key_str.substr(0, 3) / key_str;
}

bool IFileCache::shouldBypassCache()
{
    return !CurrentThread::isInitialized()
        || !CurrentThread::get().getQueryContext()
        || CurrentThread::getQueryId().size == 0;
}

void IFileCache::assertInitialized() const
{
    if (!is_initialized)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Cache not initialized");
}

LRUFileCache::LRUFileCache(const String & cache_base_path_, size_t max_size_, size_t max_element_size_, size_t max_file_segment_size_)
    : IFileCache(cache_base_path_, max_size_, max_element_size_, max_file_segment_size_)
    , log(&Poco::Logger::get("LRUFileCache"))
{
}

void LRUFileCache::initialize()
{
    if (fs::exists(cache_base_path))
        loadCacheInfoIntoMemory();
    else
        fs::create_directories(cache_base_path);

    is_initialized = true;
}

void LRUFileCache::useCell(
    const FileSegmentCell & cell, FileSegments & result, std::lock_guard<std::mutex> & /* cache_lock */)
{
    auto file_segment = cell.file_segment;

    if (file_segment->isDownloaded()
        && fs::file_size(getPathInLocalCache(file_segment->key(), file_segment->offset())) == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot have zero size downloaded file segments. Current file segment: {}",
                        file_segment->range().toString());

    result.push_back(cell.file_segment);

    /**
     * A cell receives a queue iterator on first successful space reservation attempt
     * (space is reserved incrementally on each read buffer nextImpl() call).
     */
    if (cell.queue_iterator)
    {
        /// Move to the end of the queue. The iterator remains valid.
        queue.splice(queue.end(), queue, *cell.queue_iterator);
    }
}

LRUFileCache::FileSegmentCell * LRUFileCache::getCell(
    const Key & key, size_t offset, std::lock_guard<std::mutex> & /* cache_lock */)
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

FileSegments LRUFileCache::getImpl(
    const Key & key, const FileSegment::Range & range, std::lock_guard<std::mutex> & cache_lock)
{
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    auto it = files.find(key);
    if (it == files.end())
        return {};

    const auto & file_segments = it->second;
    if (file_segments.empty())
    {
        auto key_path = getPathInLocalCache(key);

        files.erase(key);

        if (fs::exists(key_path))
            fs::remove(key_path);

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

        const auto & cell = file_segments.rbegin()->second;
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

    return result;
}

FileSegments LRUFileCache::splitRangeIntoEmptyCells(
    const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock)
{
    assert(size > 0);

    auto current_pos = offset;
    auto end_pos_non_included = offset + size;

    size_t current_cell_size;
    size_t remaining_size = size;

    FileSegments file_segments;
    while (current_pos < end_pos_non_included)
    {
        current_cell_size = std::min(remaining_size, max_file_segment_size);
        remaining_size -= current_cell_size;

        auto * cell = addCell(key, current_pos, current_cell_size, FileSegment::State::EMPTY, cache_lock);
        if (cell)
            file_segments.push_back(cell->file_segment);

        current_pos += current_cell_size;
    }

    assert(file_segments.empty() || offset + size - 1 == file_segments.back()->range().right);
    return file_segments;
}

FileSegmentsHolder LRUFileCache::getOrSet(const Key & key, size_t offset, size_t size)
{
    assertInitialized();

    FileSegment::Range range(offset, offset + size - 1);

    std::lock_guard cache_lock(mutex);

    /// Get all segments which intersect with the given range.
    auto file_segments = getImpl(key, range, cache_lock);

    if (file_segments.empty())
    {
        file_segments = splitRangeIntoEmptyCells(key, offset, size, cache_lock);
    }
    else
    {
        /// There are segments [segment1, ..., segmentN]
        /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
        /// intersect with given range.

        /// It can have holes:
        /// [____________________]         -- requested range
        ///     [____]  [_]   [_________]  -- intersecting cache [segment1, ..., segmentN]
        ///
        /// For each such hole create a cell with file segment state EMPTY.

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
            file_segments.splice(it, splitRangeIntoEmptyCells(key, current_pos, hole_size, cache_lock));

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
            file_segments.splice(file_segments.end(), splitRangeIntoEmptyCells(key, current_pos, hole_size, cache_lock));
        }
    }

    assert(!file_segments.empty());
    return FileSegmentsHolder(std::move(file_segments));
}

LRUFileCache::FileSegmentCell * LRUFileCache::addCell(
    const Key & key, size_t offset, size_t size, FileSegment::State state,
    std::lock_guard<std::mutex> & /* cache_lock */)
{
    /// Create a file segment cell and put it in `files` map by [key][offset].

    if (!size)
        return nullptr; /// Empty files are not cached.

    if (files[key].contains(offset))
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
            "Cache already exists for key: `{}`, offset: {}, size: {}", keyToStr(key), offset, size);

    auto file_segment = std::make_shared<FileSegment>(offset, size, key, this, state);
    FileSegmentCell cell(std::move(file_segment), queue);

    auto & offsets = files[key];

    if (offsets.empty())
    {
        auto key_path = getPathInLocalCache(key);
        if (!fs::exists(key_path))
            fs::create_directories(key_path);
    }

    auto [it, inserted] = offsets.insert({offset, std::move(cell)});
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Failed to insert into cache key: `{}`, offset: {}, size: {}", keyToStr(key), offset, size);

    return &(it->second);
}

bool LRUFileCache::tryReserve(
    const Key & key_, size_t offset_, size_t size, std::lock_guard<std::mutex> & cache_lock)
{
    auto removed_size = 0;
    size_t queue_size = queue.size();
    assert(queue_size <= max_element_size);

    /// Since space reservation is incremental, cache cell already exists if it's state is EMPTY.
    /// And it cache cell does not exist on startup -- as we first check for space and then add a cell.
    auto * cell_for_reserve = getCell(key_, offset_, cache_lock);

    /// A cell acquires a LRUQueue iterator on first successful space reservation attempt.
    /// cell_for_reserve can be nullptr here when we call tryReserve() from loadCacheInfoIntoMemory().
    if (!cell_for_reserve || !cell_for_reserve->queue_iterator)
        queue_size += 1;

    auto is_overflow = [&]
    {
        return (current_size + size - removed_size > max_size)
            || (max_element_size != 0 && queue_size > max_element_size);
    };

    std::vector<FileSegmentCell *> to_evict;

    auto key_it = queue.begin();
    while (is_overflow() && key_it != queue.end())
    {
        const auto [key, offset] = *key_it;
        ++key_it;

        auto * cell = getCell(key, offset, cache_lock);
        if (!cell)
            throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Cache became inconsistent. Key: {}, offset: {}", keyToStr(key), offset);

        size_t cell_size = cell->size();

        /// It is guaranteed that cell is not removed from cache as long as
        /// pointer to corresponding file segment is hold by any other thread.

        if (cell->releasable())
        {
            auto & file_segment = cell->file_segment;

            std::lock_guard segment_lock(file_segment->mutex);

            switch (file_segment->download_state)
            {
                case FileSegment::State::DOWNLOADED:
                {
                    /// Cell will actually be removed only if
                    /// we managed to reserve enough space.

                    to_evict.push_back(cell);
                    break;
                }
                default:
                {
                    remove(key, offset, cache_lock, segment_lock);
                    break;
                }
            }

            removed_size += cell_size;
            --queue_size;
        }
    }

    if (is_overflow())
        return false;

    if (cell_for_reserve && !cell_for_reserve->queue_iterator)
        cell_for_reserve->queue_iterator = queue.insert(queue.end(), std::make_pair(key_, offset_));

    for (auto & cell : to_evict)
    {
        auto file_segment = cell->file_segment;
        if (file_segment)
        {
            std::lock_guard<std::mutex> segment_lock(file_segment->mutex);
            remove(file_segment->key(), file_segment->offset(), cache_lock, segment_lock);
        }
    }

    current_size += size - removed_size;
    if (current_size > (1ull << 63))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache became inconsistent. There must be a bug");

    return true;
}

void LRUFileCache::remove(const Key & key)
{
    assertInitialized();

    std::lock_guard cache_lock(mutex);

    auto it = files.find(key);
    if (it == files.end())
        return;

    auto & offsets = it->second;

    std::vector<FileSegmentCell *> to_remove;
    to_remove.reserve(offsets.size());

    for (auto & [offset, cell] : offsets)
        to_remove.push_back(&cell);

    for (auto & cell : to_remove)
    {
        if (!cell->releasable())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove file from cache because someone reads from it. File segment info: {}", cell->file_segment->getInfoForLog());

        auto file_segment = cell->file_segment;
        if (file_segment)
        {
            std::lock_guard<std::mutex> segment_lock(file_segment->mutex);
            remove(file_segment->key(), file_segment->offset(), cache_lock, segment_lock);
        }
    }

    auto key_path = getPathInLocalCache(key);

    files.erase(key);

    if (fs::exists(key_path))
        fs::remove(key_path);
}

void LRUFileCache::remove(
    Key key, size_t offset,
    std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & /* segment_lock */)
{
    LOG_TEST(log, "Remove. Key: {}, offset: {}", keyToStr(key), offset);

    auto * cell = getCell(key, offset, cache_lock);
    if (!cell)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "No cache cell for key: {}, offset: {}", keyToStr(key), offset);

    if (cell->queue_iterator)
        queue.erase(*cell->queue_iterator);

    auto & offsets = files[key];
    offsets.erase(offset);

    auto cache_file_path = getPathInLocalCache(key, offset);
    if (fs::exists(cache_file_path))
    {
        try
        {
            fs::remove(cache_file_path);

            if (is_initialized && offsets.empty())
            {
                auto key_path = getPathInLocalCache(key);

                files.erase(key);

                if (fs::exists(key_path))
                    fs::remove(key_path);
            }
        }
        catch (...)
        {
            throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                            "Removal of cached file failed. Key: {}, offset: {}, path: {}, error: {}",
                            keyToStr(key), offset, cache_file_path, getCurrentExceptionMessage(false));
        }
    }
}

void LRUFileCache::loadCacheInfoIntoMemory()
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
            key = unhexUInt<UInt128>(key_it->path().filename().string().data());

            fs::directory_iterator offset_it{key_it->path()};
            for (; offset_it != fs::directory_iterator(); ++offset_it)
            {
                bool parsed = tryParse<UInt64>(offset, offset_it->path().filename());
                if (!parsed)
                {
                    LOG_WARNING(log, "Unexpected file: ", offset_it->path().string());
                    continue; /// Or just remove? Some unexpected file.
                }

                size = offset_it->file_size();
                if (!size)
                {
                    fs::remove(offset_it->path());
                    continue;
                }

                if (tryReserve(key, offset, size, cache_lock))
                {
                    auto * cell = addCell(key, offset, size, FileSegment::State::DOWNLOADED, cache_lock);
                    if (cell)
                        cells.push_back(cell);
                }
                else
                {
                    LOG_WARNING(log,
                                "Cache capacity changed (max size: {}, available: {}), cached file `{}` does not fit in cache anymore (size: {})",
                                max_size, availableSize(), key_it->path().string(), size);
                    fs::remove(offset_it->path());
                }
            }
        }
    }

    /// Shuffle cells to have random order in LRUQueue as at startup all cells have the same priority.
    pcg64 generator(randomSeed());
    std::shuffle(cells.begin(), cells.end(), generator);
    for (const auto & cell : cells)
    {
        /// Cell cache size changed and, for example, 1st file segment fits into cache
        /// and 2nd file segment will fit only if first was evicted, then first will be removed and
        /// cell is nullptr here.
        if (cell)
            queue.splice(queue.end(), queue, *cell->queue_iterator);
    }
}

LRUFileCache::Stat LRUFileCache::getStat()
{
    std::lock_guard cache_lock(mutex);

    Stat stat
    {
        .size = queue.size(),
        .available = availableSize(),
        .downloaded_size = 0,
        .downloading_size = 0,
    };

    for (const auto & [key, offset] : queue)
    {
        const auto * cell = getCell(key, offset, cache_lock);
        if (!cell)
            throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                "Cache became inconsistent. Key: {}, offset: {}", keyToStr(key), offset);

        switch (cell->file_segment->download_state)
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

void LRUFileCache::reduceSizeToDownloaded(
    const Key & key, size_t offset,
    std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & /* segment_lock */)
{
    /**
     * In case file was partially downloaded and it's download cannot be continued
     * because of no space left in cache, we need to be able to cut cell's size to downloaded_size.
     */

    auto * cell = getCell(key, offset, cache_lock);

    if (!cell)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "No cell found for key: {}, offset: {}", keyToStr(key), offset);

    const auto & file_segment = cell->file_segment;

    size_t downloaded_size = file_segment->downloaded_size;
    if (downloaded_size == file_segment->range().size())
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                        "Nothing to reduce, file segment fully downloaded, key: {}, offset: {}", keyToStr(key), offset);

    cell->file_segment = std::make_shared<FileSegment>(offset, downloaded_size, key, this, FileSegment::State::DOWNLOADED);
}

bool LRUFileCache::isLastFileSegmentHolder(
    const Key & key, size_t offset,
    std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & /* segment_lock */)
{
    auto * cell = getCell(key, offset, cache_lock);

    if (!cell)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No cell found for key: {}, offset: {}", keyToStr(key), offset);

    /// The caller of this method is last file segment holder if use count is 2 (the second pointer is cache itself)
    return cell->file_segment.use_count() == 2;
}

LRUFileCache::FileSegmentCell::FileSegmentCell(FileSegmentPtr file_segment_, LRUQueue & queue_)
    : file_segment(file_segment_)
{
    /**
     * Cell can be created with either DOWNLOADED or EMPTY file segment's state.
     * File segment acquires DOWNLOADING state and creates LRUQueue iterator on first
     * successful getOrSetDownaloder call.
     */

    switch (file_segment->download_state)
    {
        case FileSegment::State::DOWNLOADED:
        {
            queue_iterator = queue_.insert(queue_.end(), getKeyAndOffset());
            break;
        }
        case FileSegment::State::EMPTY:
        {
            break;
        }
        default:
            throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR,
                            "Can create cell with either DOWNLOADED or EMPTY state, got: {}",
                            FileSegment::stateToString(file_segment->download_state));
    }
}

String LRUFileCache::dumpStructure(const Key & key_)
{
    std::lock_guard cache_lock(mutex);

    WriteBufferFromOwnString result;
    for (auto it = queue.begin(); it != queue.end(); ++it)
    {
        auto [key, offset] = *it;
        if (key == key_)
        {
            auto * cell = getCell(key, offset, cache_lock);
            result << (it != queue.begin() ? ", " : "") << cell->file_segment->range().toString();
            result << "(state: " << cell->file_segment->download_state << ")";
        }
    }
    return result.str();
}

}
