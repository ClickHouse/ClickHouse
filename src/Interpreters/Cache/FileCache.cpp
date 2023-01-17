#include "FileCache.h"

#include <Common/randomSeed.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <pcg-random/pcg_random.hpp>
#include <Common/hex.h>
#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FileCache::FileCache(
    const String & cache_base_path_,
    const FileCacheSettings & cache_settings_)
    : cache_base_path(cache_base_path_)
    , max_size(cache_settings_.max_size)
    , max_element_size(cache_settings_.max_elements)
    , max_file_segment_size(cache_settings_.max_file_segment_size)
    , allow_persistent_files(cache_settings_.do_not_evict_index_and_mark_files)
    , enable_bypass_cache_with_threshold(cache_settings_.enable_bypass_cache_with_threashold)
    , bypass_cache_threshold(enable_bypass_cache_with_threshold ? cache_settings_.bypass_cache_threashold : 0)
    , log(&Poco::Logger::get("FileCache"))
    , main_priority(std::make_unique<LRUFileCachePriority>())
    , stash(std::make_unique<HitsCountStash>(cache_settings_.max_elements, cache_settings_.enable_cache_hits_threshold, std::make_unique<LRUFileCachePriority>()))
    , query_limit(cache_settings_.enable_filesystem_query_cache_limit ? std::make_unique<QueryLimit>() : nullptr)
{
}

FileCache::Key FileCache::createKeyForPath(const String & path)
{
    return Key(path);
}

String FileCache::getPathInLocalCache(const Key & key, size_t offset, FileSegmentKind segment_kind) const
{
    String file_suffix;
    switch (segment_kind)
    {
        case FileSegmentKind::Persistent:
            file_suffix = "_persistent";
            break;
        case FileSegmentKind::Temporary:
            file_suffix = "_temporary";
            break;
        case FileSegmentKind::Regular:
            file_suffix = "";
            break;
    }

    auto key_str = key.toString();
    return fs::path(cache_base_path)
        / key_str.substr(0, 3)
        / key_str
        / (std::to_string(offset) + file_suffix);
}

String FileCache::getPathInLocalCache(const Key & key) const
{
    auto key_str = key.toString();
    return fs::path(cache_base_path) / key_str.substr(0, 3) / key_str;
}

void FileCache::assertInitialized() const
{
    if (is_initialized)
        return;

    std::unique_lock lock(init_mutex);
    if (is_initialized)
        return;

    if (init_exception)
        std::rethrow_exception(init_exception);
    if (!is_initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache not initialized");
}

void FileCache::initialize()
{
    std::lock_guard lock(init_mutex);

    if (is_initialized)
        return;

    try
    {
        loadMetadata();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        init_exception = std::current_exception();
        throw;
    }

    is_initialized = true;
}

static bool isQueryInitialized()
{
    return CurrentThread::isInitialized()
        && CurrentThread::get().getQueryContext()
        && !CurrentThread::getQueryId().empty();
}

bool FileCache::readThrowCacheAllowed()
{
    return !isQueryInitialized();
}

FileSegments FileCache::getImpl(
    const Key & key, const FileSegment::Range & range, const KeyTransaction & key_transaction) const
{
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    if (bypass_cache_threshold && range.size() > bypass_cache_threshold)
    {
        auto file_segment = std::make_shared<FileSegment>(
            range.left, range.size(), key, nullptr, nullptr,
            FileSegment::State::SKIP_CACHE, CreateFileSegmentSettings{});
        return { file_segment };
    }

    const auto & file_segments = key_transaction.getOffsets();
    if (file_segments.empty())
        return {};

    FileSegments result;
    auto add_to_result = [&](const FileSegmentCell & cell)
    {
        if (cell.file_segment->isDownloaded())
        {
            if (cell.file_segment->getDownloadedSize() == 0)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot have zero size downloaded file segments. {}",
                    cell.file_segment->getInfoForLog());
            }

    #ifndef NDEBUG
            /**
            * Check that in-memory state of the cache is consistent with the state on disk.
            * Check only in debug build, because such checks can be done often and can be quite
            * expensive compared to overall query execution time.
            */

            fs::path path = cell.file_segment->getPathInLocalCache();
            if (!fs::exists(path))
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "File path does not exist, but file has DOWNLOADED state. {}",
                    cell.file_segment->getInfoForLog());
            }

            if (fs::file_size(path) == 0)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot have zero size downloaded file segments. {}",
                    cell.file_segment->getInfoForLog());
            }
    #endif
        }
        auto state = cell.file_segment->state();
        if (state == FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
            std::terminate();

        result.push_back(cell.file_segment);
    };

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

        add_to_result(cell);
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
                add_to_result(prev_cell);
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

            add_to_result(cell);
            ++segment_it;
        }
    }

    return result;
}

FileSegments FileCache::splitRangeIntoCells(
    const Key & key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    const CreateFileSegmentSettings & settings,
    KeyTransaction & key_transaction)
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

        auto cell_it = addCell(key, current_pos, current_cell_size, state, settings, key_transaction, nullptr);
        file_segments.push_back(cell_it->second.file_segment);

        current_pos += current_cell_size;
    }

    assert(file_segments.empty() || offset + size - 1 == file_segments.back()->range().right);
    return file_segments;
}

void FileCache::fillHolesWithEmptyFileSegments(
    FileSegments & file_segments,
    const Key & key,
    const FileSegment::Range & range,
    bool fill_with_detached_file_segments,
    const CreateFileSegmentSettings & settings,
    KeyTransaction & key_transaction)
{
    /// There are segments [segment1, ..., segmentN]
    /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
    /// intersect with given range.

    /// It can have holes:
    /// [____________________]         -- requested range
    ///     [____]  [_]   [_________]  -- intersecting cache [segment1, ..., segmentN]
    ///
    /// For each such hole create a cell with file segment state EMPTY.

    assert(!file_segments.empty());

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

        if (fill_with_detached_file_segments)
        {
            auto file_segment = std::make_shared<FileSegment>(
                current_pos, hole_size, key,
                key_transaction.getCreator(),
                this, FileSegment::State::SKIP_CACHE, settings);

            file_segments.insert(it, file_segment);
        }
        else
        {
            file_segments.splice(
                it, splitRangeIntoCells(
                    key, current_pos, hole_size, FileSegment::State::EMPTY, settings, key_transaction));
        }

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

        if (fill_with_detached_file_segments)
        {
            auto file_segment = std::make_shared<FileSegment>(
                current_pos, hole_size, key,
                key_transaction.getCreator(),
                this, FileSegment::State::SKIP_CACHE, settings);

            file_segments.insert(file_segments.end(), file_segment);
        }
        else
        {
            file_segments.splice(
                file_segments.end(),
                splitRangeIntoCells(key, current_pos, hole_size, FileSegment::State::EMPTY, settings, key_transaction));
        }
    }
}

KeyTransactionPtr FileCache::createKeyTransaction(const Key & key, KeyNotFoundPolicy key_not_found_policy)
{
    std::lock_guard lock(files_mutex);

    /// TODO: Add cleanup thread instead of doing this in createKeyTransaction?
    remove_files_metadata.removeIfExists(key);
    remove_files_metadata.iterate([this](const Key & remove_metadata_key)
    {
        [[maybe_unused]] const bool erased = files.erase(remove_metadata_key);
        chassert(erased);

        const fs::path prefix_path = fs::path(getPathInLocalCache(remove_metadata_key)).parent_path();
        if (fs::exists(prefix_path) && fs::is_empty(prefix_path))
            fs::remove(prefix_path);
    });

    auto it = files.find(key);
    if (it == files.end())
    {
        switch (key_not_found_policy)
        {
            case KeyNotFoundPolicy::THROW:
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "No such key `{}` in cache", key.toString());
            }
            case KeyNotFoundPolicy::RETURN_NULL:
            {
                return nullptr;
            }
            case KeyNotFoundPolicy::CREATE_EMPTY:
            {
                it = files.emplace(key, std::make_shared<CacheCells>()).first;
                break;
            }
        }
    }

    return std::make_unique<KeyTransaction>(key, it->second, this);
}

FileSegmentsHolderPtr FileCache::set(const Key & key, size_t offset, size_t size, const CreateFileSegmentSettings & settings)
{
    assertInitialized();

    auto key_transaction = createKeyTransaction(key, KeyNotFoundPolicy::CREATE_EMPTY);
    FileSegment::Range range(offset, offset + size - 1);

    auto file_segments = getImpl(key, range, *key_transaction);
    if (!file_segments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Having intersection with already existing cache");

    if (settings.unbounded)
    {
        /// If the file is unbounded, we can create a single cell for it.
        auto cell_it = addCell(key, offset, size, FileSegment::State::EMPTY, settings, *key_transaction, nullptr);
        file_segments = {cell_it->second.file_segment};
    }
    else
        file_segments = splitRangeIntoCells(key, offset, size, FileSegment::State::EMPTY, settings, *key_transaction);

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::getOrSet(
    const Key & key,
    size_t offset,
    size_t size,
    const CreateFileSegmentSettings & settings)
{
    assertInitialized();

    FileSegment::Range range(offset, offset + size - 1);

    auto key_transaction = createKeyTransaction(key, KeyNotFoundPolicy::CREATE_EMPTY);

    /// Get all segments which intersect with the given range.
    auto file_segments = getImpl(key, range, *key_transaction);
    if (file_segments.empty())
    {
        file_segments = splitRangeIntoCells(
            key, offset, size, FileSegment::State::EMPTY, settings, *key_transaction);
    }
    else
    {
        fillHolesWithEmptyFileSegments(
            file_segments, key, range, /* fill_with_detached */false, settings, *key_transaction);
    }

    chassert(!file_segments.empty());
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::get(const Key & key, size_t offset, size_t size)
{
    assertInitialized();

    auto key_transaction = createKeyTransaction(key, KeyNotFoundPolicy::RETURN_NULL);
    if (key_transaction)
    {
        FileSegment::Range range(offset, offset + size - 1);

        /// Get all segments which intersect with the given range.
        auto file_segments = getImpl(key, range, *key_transaction);
        if (!file_segments.empty())
        {
            fillHolesWithEmptyFileSegments(
                file_segments, key, range, /* fill_with_detached */true,
                CreateFileSegmentSettings{}, *key_transaction);

            return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
        }
    }

    auto file_segment = std::make_shared<FileSegment>(
        offset, size, key,
        nullptr, this, FileSegment::State::SKIP_CACHE, CreateFileSegmentSettings{});

    return std::make_unique<FileSegmentsHolder>(FileSegments{file_segment});
}

FileCache::CacheCells::iterator FileCache::addCell(
    const Key & key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    const CreateFileSegmentSettings & settings,
    KeyTransaction & key_transaction,
    CachePriorityQueueGuard::Lock * queue_lock)
{
    /// Create a file segment cell and put it in `files` map by [key][offset].

    chassert(size > 0); /// Empty cells are not allowed.

    auto it = key_transaction.getOffsets().find(offset);
    if (it != key_transaction.getOffsets().end())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cache cell already exists for key: `{}`, offset: {}, size: {}.",
            key.toString(), offset, size);
    }

    FileSegment::State result_state;

    /// `stash` - a queue of "stashed" key-offset pairs. Implements counting of
    /// cache entries and allows caching only if cache hit threadhold is reached.
    if (stash && stash->cache_hits_threshold && state == FileSegment::State::EMPTY)
    {
        // auto stash_lock = stash.lock();
        // KeyAndOffset stash_key(key, offset);

        // auto record_it = stash.records.find(stash_key);
        // if (record_it == stash.records.end())
        // {
        //     auto & stash_queue = *stash.queue;
        //     auto & stash_records = stash.records;

        //     stash_records.insert({stash_key, stash_queue.add(key, offset, 0, key_transaction.getCreator(), lock)});

        //     if (stash_queue.getElementsNum(lock) > stash.max_stash_queue_size)
        //         stash_records.erase(stash_queue.pop(lock));

        //     result_state = FileSegment::State::SKIP_CACHE;
        // }
        // else
        // {
        //     result_state = record_it->second->use() >= stash.cache_hits_threshold
        //         ? FileSegment::State::EMPTY
        //         : FileSegment::State::SKIP_CACHE;
        // }
    }
    else
    {
        result_state = state;
    }

    auto file_segment = std::make_shared<FileSegment>(
        offset, size, key, key_transaction.getCreator(), this, result_state, settings);

    FileSegmentCell cell(std::move(file_segment), key_transaction, *main_priority, queue_lock);

    auto [cell_it, inserted] = key_transaction.getOffsets().emplace(offset, std::move(cell));
    assert(inserted);

    return cell_it;
}

bool FileCache::tryReserve(const Key & key, size_t offset, size_t size)
{
    assertInitialized();
    auto queue_lock = main_priority->lock();
    auto key_transaction = createKeyTransaction(key, KeyNotFoundPolicy::THROW);
    return tryReserveUnlocked(key, offset, size, key_transaction, queue_lock);
}

bool FileCache::tryReserveUnlocked(
    const Key & key,
    size_t offset,
    size_t size,
    KeyTransactionPtr key_transaction,
    const CachePriorityQueueGuard::Lock & queue_lock)
{
    auto query_context = query_limit ? query_limit->tryGetQueryContext(queue_lock) : nullptr;

    bool reserved;
    if (query_context)
    {
        const bool query_limit_exceeded = query_context->getCacheSize() + size > query_context->getMaxCacheSize();
        if (!query_limit_exceeded)
            reserved = tryReserveInCache(key, offset, size, query_context, key_transaction, queue_lock);
        else if (query_context->isSkipDownloadIfExceed())
            reserved = false;
        else
            reserved = tryReserveInQueryCache(key, offset, size, query_context, key_transaction, queue_lock);
    }
    else
    {
        reserved = tryReserveInCache(key, offset, size, nullptr, key_transaction, queue_lock);
    }

    if (reserved && !key_transaction->getOffsets().created_base_directory)
    {
        fs::create_directories(getPathInLocalCache(key));
        key_transaction->getOffsets().created_base_directory = true;
    }
    return reserved;
}

bool FileCache::tryReserveInQueryCache(
    const Key & key,
    size_t offset,
    size_t size,
    QueryLimit::QueryContextPtr query_context,
    KeyTransactionPtr,
    const CachePriorityQueueGuard::Lock & queue_lock)
{
    LOG_TEST(log, "Reserving query cache space {} for {}:{}", size, key.toString(), offset);

    auto & query_priority = query_context->getPriority();
    if (query_priority.getElementsNum(queue_lock)) {}
//    struct Segment
//    {
//        Key key;
//        size_t offset;
//        size_t size;
//
//        Segment(Key key_, size_t offset_, size_t size_)
//            : key(key_), offset(offset_), size(size_) {}
//    };
//
//    std::vector<Segment> ghost;

    // size_t queue_size = main_priority->getElementsNum(lock);
    // size_t removed_size = 0;

    // auto is_overflow = [&]
    // {
    //     return (max_size != 0 && main_priority->getCacheSize() + size - removed_size > max_size)
    //         || (max_element_size != 0 && queue_size > max_element_size)
    //         || (query_context->getCacheSize() + size - removed_size > query_context->getMaxCacheSize());
    // };

    // query_priority.iterate([&](const QueueEntry & entry) -> IterationResult
    // {
    //     if (!is_overflow())
    //         return IterationResult::BREAK;

    //     // auto * cell = key_transaction.getOffsets().tryGet(iter->offset());

    //     return IterationResult::CONTINUE;
    // }, lock);
//
//        auto * cell = key_transaction.getOffsets().tryGet(iter->offset());
//
//        if (!cell)
//        {
//            /// The cache corresponding to this record may be swapped out by
//            /// other queries, so it has become invalid.
//            removed_size += iter->size();
//            ghost.push_back(Segment(iter->key(), iter->offset(), iter->size()));
//            /// next()
//            iter->removeAndGetNext();
//        }
//        else
//        {
//            size_t cell_size = cell->size();
//            assert(iter->size() == cell_size);
//
//            if (cell->releasable())
//            {
//                auto & file_segment = cell->file_segment;
//
//                if (file_segment->isPersistent() && allow_persistent_files)
//                    continue;
//
//                switch (file_segment->state())
//                {
//                    case FileSegment::State::DOWNLOADED:
//                    {
//                        to_evict.push_back(cell);
//                        break;
//                    }
//                    default:
//                    {
//                        trash.push_back(cell);
//                        break;
//                    }
//                }
//                removed_size += cell_size;
//                --queue_size;
//            }
//
//            iter->next();
//        }
//    }
//
//    // auto remove_file_segment = [&](FileSegmentPtr file_segment, size_t file_segment_size)
//    // {
//    //     /// FIXME: key transaction is incorrect
//    //     query_context->remove(file_segment->key(), file_segment->offset(), file_segment_size, key_transaction);
//    //     remove(file_segment);
//    // };
//
//    assert(trash.empty());
//    //for (auto & cell : trash)
//    //{
//    //    if (auto file_segment = cell->file_segment)
//    //        // remove_file_segment(file_segment, cell->size());
//    //}
//
//    for (auto & entry : ghost)
//        /// FIXME: key transaction is incorrect
//        query_context->remove(entry.key, entry.offset, entry.size, key_transaction);
//
//    if (is_overflow())
//        return false;
//
//    if (cell_for_reserve)
//    {
//        auto queue_iterator = cell_for_reserve->queue_iterator;
//        if (queue_iterator)
//            queue_iterator->incrementSize(size);
//        else
//            cell_for_reserve->queue_iterator = main_priority->add(key, offset, size, key_transaction.getCreator());
//    }
//
//    // for (auto & cell : to_evict)
//    // {
//    //     if (auto file_segment = cell->file_segment)
//    //         remove_file_segment(file_segment, cell->size());
//    // }
//
//    query_context->reserve(key, offset, size, key_transaction);
//
//    if (reserved && !key_transaction.getOffsets().created_base_directory)
//    {
//        fs::create_directories(getPathInLocalCache(key));
//        key_transaction.getOffsets().created_base_directory = true;
//    }
    return true;
}

void FileCache::iterateAndCollectKeyLocks(
    IFileCachePriority & priority,
    IterateAndCollectLocksFunc && func,
    KeyTransactionsMap & locked_map,
    const CachePriorityQueueGuard::Lock & queue_lock)
{
    priority.iterate([&, func = std::move(func)](const IFileCachePriority::Entry & entry)
    {
        KeyTransactionPtr current;

        auto locked_it = locked_map.find(entry.key);
        const bool locked = locked_it != locked_map.end();
        if (locked)
            current = locked_it->second;
        else
            current = entry.createKeyTransaction();

        auto res = func(entry, *current);
        if (res.lock_key && !locked)
            locked_map.emplace(entry.key, current);

        return res.iteration_result;
    }, queue_lock);
}

bool FileCache::tryReserveInCache(
    const Key & key,
    size_t offset,
    size_t size,
    QueryLimit::QueryContextPtr query_context,
    KeyTransactionPtr key_transaction,
    const CachePriorityQueueGuard::Lock & queue_lock)
{
    LOG_TEST(log, "Reserving space {} for {}:{}", size, key.toString(), offset);

    size_t queue_size = main_priority->getElementsNum(queue_lock);
    chassert(queue_size <= max_element_size);

    auto * cell_for_reserve = key_transaction->getOffsets().tryGet(offset);

    /// A cell acquires a LRUQueue iterator on first successful space reservation attempt.
    if (!cell_for_reserve || !cell_for_reserve->queue_iterator)
        queue_size += 1;

    size_t removed_size = 0;
    auto is_overflow = [&]
    {
        /// max_size == 0 means unlimited cache size,
        /// max_element_size means unlimited number of cache elements.
        return (max_size != 0 && main_priority->getCacheSize(queue_lock) + size - removed_size > max_size)
            || (max_element_size != 0 && queue_size > max_element_size);
    };

    KeyTransactionsMap locked;
    locked[key] = key_transaction;

    using QueueEntry = IFileCachePriority::Entry;
    using IterationResult = IFileCachePriority::IterationResult;

    iterateAndCollectKeyLocks(
        *main_priority,
        [&](const QueueEntry & entry, KeyTransaction & locked_key) -> IterateAndLockResult
    {
        if (!is_overflow())
            return { IterationResult::BREAK, false };

        auto * cell = locked_key.getOffsets().get(entry.offset);

        chassert(entry.size == cell->size());
        const size_t cell_size = cell->size();
        bool remove_current_it = false;

        /// It is guaranteed that cell is not removed from cache as long as
        /// pointer to corresponding file segment is hold by any other thread.

        bool save_key_transaction = false;
        if (cell->releasable())
        {
            auto file_segment = cell->file_segment;

            chassert(entry.offset == file_segment->offset());
            if (file_segment->isPersistent() && allow_persistent_files)
            {
                return { IterationResult::CONTINUE, false };
            }

            switch (file_segment->state())
            {
                case FileSegment::State::DOWNLOADED:
                {
                    /// Cell will actually be removed only if we managed to reserve enough space.

                    locked_key.delete_offsets.push_back(file_segment->offset());
                    save_key_transaction = true;
                    break;
                }
                default:
                {
                    remove_current_it = true;
                    cell->queue_iterator = {};
                    locked_key.remove(file_segment, queue_lock);
                    break;
                }
            }

            removed_size += cell_size;
            --queue_size;
        }

        if (remove_current_it)
            return { IterationResult::REMOVE_AND_CONTINUE, save_key_transaction };

        return { IterationResult::CONTINUE, save_key_transaction };
    }, locked, queue_lock);

    if (is_overflow())
        return false;

    if (cell_for_reserve)
    {
        /// queue_iteratir is std::nullopt here if no space has been reserved yet, a cache cell
        /// acquires queue iterator on first successful space reservation attempt.
        /// If queue iterator already exists, we need to update the size after each space reservation.
        auto queue_iterator = cell_for_reserve->queue_iterator;
        if (queue_iterator)
            queue_iterator->incrementSize(size, queue_lock);
        else
        {
            /// Space reservation is incremental, so cache cell is created first (with state empty),
            /// and queue_iterator is assigned on first space reservation attempt.
            cell_for_reserve->queue_iterator = main_priority->add(key, offset, size, key_transaction->getCreator(), queue_lock);
        }
    }

    for (auto & [_, transaction] : locked)
    {
        for (const auto & offset_to_delete : transaction->delete_offsets)
        {
            auto * cell = transaction->getOffsets().get(offset_to_delete);
            transaction->remove(cell->file_segment, queue_lock);
        }
    }

    if (main_priority->getCacheSize(queue_lock) > (1ull << 63))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache became inconsistent. There must be a bug");

    if (query_context)
        query_context->reserve(key, offset, size, *key_transaction, queue_lock);

    return true;
}

void FileCache::removeIfExists(const Key & key)
{
    assertInitialized();

    auto queue_lock = main_priority->lock();
    auto key_transaction = createKeyTransaction(key, KeyNotFoundPolicy::RETURN_NULL);
    if (!key_transaction)
        return;

    auto & offsets = key_transaction->getOffsets();
    if (!offsets.empty())
    {
        std::vector<FileSegmentCell *> remove_cells;
        remove_cells.reserve(offsets.size());
        for (auto & [offset, cell] : offsets)
            remove_cells.push_back(&cell);

        for (auto & cell : remove_cells)
        {
            /// In ordinary case we remove data from cache when it's not used by anyone.
            /// But if we have multiple replicated zero-copy tables on the same server
            /// it became possible to start removing something from cache when it is used
            /// by other "zero-copy" tables. That is why it's not an error.
            if (!cell->releasable())
                continue;

            key_transaction->remove(cell->file_segment, queue_lock);
        }
    }
}

void FileCache::removeAllReleasable()
{
    assertInitialized();

    using QueueEntry = IFileCachePriority::Entry;
    using IterationResult = IFileCachePriority::IterationResult;

    /// Try remove all cached files by cache_base_path.
    /// Only releasable file segments are evicted.
    /// `remove_persistent_files` defines whether non-evictable by some criteria files
    /// (they do not comply with the cache eviction policy) should also be removed.

    auto queue_lock = main_priority->lock();
    main_priority->iterate([&](const QueueEntry & entry) -> IterationResult
    {
        auto key_transaction = entry.createKeyTransaction();
        auto * cell = key_transaction->getOffsets().get(entry.offset);

        if (cell->releasable())
        {
            cell->queue_iterator = {};
            key_transaction->remove(cell->file_segment, queue_lock);
            return IterationResult::REMOVE_AND_CONTINUE;
        }
        return IterationResult::CONTINUE;
    }, queue_lock);

    if (stash)
    {
        /// Remove all access information.
        auto lock = stash->queue->lock();
        stash->records.clear();
        stash->queue->removeAll(lock);
    }
}

KeyTransaction::KeyTransaction(const Key & key_, FileCache::CacheCellsPtr offsets_, const FileCache * cache_)
    : key(key_)
    , cache(cache_)
    , guard(offsets_->guard)
    , lock(guard->lock())
    , offsets(offsets_)
    , log(&Poco::Logger::get("KeyTransaction"))
{
}

KeyTransaction::~KeyTransaction()
{
    cleanupKeyDirectory();
}

void KeyTransaction::remove(FileSegmentPtr file_segment, const CachePriorityQueueGuard::Lock & queue_lock)
{
    /// We must hold pointer to file segment while removing it.
    chassert(file_segment->key() == key);
    remove(file_segment->offset(), file_segment->lock(), queue_lock);
}

bool KeyTransaction::isLastHolder(size_t offset)
{
    const auto * cell = getOffsets().get(offset);
    return cell->file_segment.use_count() == 2;
}

void KeyTransaction::remove(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    const CachePriorityQueueGuard::Lock & queue_lock)
{
    LOG_DEBUG(
        log, "Remove from cache. Key: {}, offset: {}",
        key.toString(), offset);

    auto * cell = offsets->get(offset);

    if (cell->queue_iterator)
        cell->queue_iterator->remove(queue_lock);

    const auto cache_file_path = cell->file_segment->getPathInLocalCache();
    cell->file_segment->detach(segment_lock, *this);

    offsets->erase(offset);

    if (fs::exists(cache_file_path))
    {
        try
        {
            fs::remove(cache_file_path);
        }
        catch (...)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Removal of cached file failed. Key: {}, offset: {}, path: {}, error: {}",
                key.toString(), offset, cache_file_path, getCurrentExceptionMessage(false));
        }
    }
}

void KeyTransaction::cleanupKeyDirectory() const
{
    /// We cannot remove key directory, because if cache is not initialized,
    /// it means we are currently iterating it.
    if (!cache->is_initialized)
        return;

    /// Someone might still need this directory.
    if (!offsets->empty())
        return;

    /// Now `offsets` empty and the key lock is still locked.
    /// So it is guaranteed that no one will add something.

    fs::path key_path = cache->getPathInLocalCache(key);
    fs::path prefix_path = key_path.parent_path();

    if (fs::exists(key_path))
    {
        offsets->created_base_directory = false;
        fs::remove_all(key_path);
    }
    cache->remove_files_metadata.add(key);
}

void FileCache::loadMetadata()
{
    auto queue_lock = main_priority->lock();

    UInt64 offset = 0;
    size_t size = 0;
    std::vector<std::pair<IFileCachePriority::Iterator, std::weak_ptr<FileSegment>>> queue_entries;

    /// cache_base_path / key_prefix / key / offset
    if (!files.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cache initialization is partially made. "
            "This can be a result of a failed first attempt to initialize cache. "
            "Please, check log for error messages");
    }

    fs::directory_iterator key_prefix_it{cache_base_path};
    for (; key_prefix_it != fs::directory_iterator(); ++key_prefix_it)
    {
        if (!key_prefix_it->is_directory())
        {
            if (key_prefix_it->path().filename() != "status")
            {
                LOG_DEBUG(
                    log, "Unexpected file {} (not a directory), will skip it",
                    key_prefix_it->path().string());
            }
            continue;
        }

        fs::directory_iterator key_it{key_prefix_it->path()};
        for (; key_it != fs::directory_iterator(); ++key_it)
        {
            if (!key_it->is_directory())
            {
                LOG_DEBUG(
                    log,
                    "Unexpected file: {} (not a directory). Expected a directory",
                    key_it->path().string());
                continue;
            }

            auto key = Key(unhexUInt<UInt128>(key_it->path().filename().string().data()));

            fs::directory_iterator offset_it{key_it->path()};
            for (; offset_it != fs::directory_iterator(); ++offset_it)
            {
                auto offset_with_suffix = offset_it->path().filename().string();
                auto delim_pos = offset_with_suffix.find('_');
                bool parsed;
                FileSegmentKind segment_kind = FileSegmentKind::Regular;

                if (delim_pos == std::string::npos)
                    parsed = tryParse<UInt64>(offset, offset_with_suffix);
                else
                {
                    parsed = tryParse<UInt64>(offset, offset_with_suffix.substr(0, delim_pos));
                    if (offset_with_suffix.substr(delim_pos+1) == "persistent")
                        segment_kind = FileSegmentKind::Persistent;
                    if (offset_with_suffix.substr(delim_pos+1) == "temporary")
                    {
                        fs::remove(offset_it->path());
                        continue;
                    }
                }

                if (!parsed)
                {
                    LOG_WARNING(log, "Unexpected file: {}", offset_it->path().string());
                    continue; /// Or just remove? Some unexpected file.
                }

                size = offset_it->file_size();
                if (!size)
                {
                    fs::remove(offset_it->path());
                    continue;
                }

                auto key_transaction = createKeyTransaction(key, KeyNotFoundPolicy::CREATE_EMPTY);
                key_transaction->getOffsets().created_base_directory = true;

                if (tryReserveUnlocked(key, offset, size, key_transaction, queue_lock))
                {
                    auto cell_it = addCell(
                        key, offset, size, FileSegment::State::DOWNLOADED,
                        CreateFileSegmentSettings(segment_kind), *key_transaction, &queue_lock);

                    queue_entries.emplace_back(cell_it->second.queue_iterator, cell_it->second.file_segment);
                }
                else
                {
                    LOG_WARNING(
                        log,
                        "Cache capacity changed (max size: {}, used: {}), "
                        "cached file `{}` does not fit in cache anymore (size: {})",
                        max_size, main_priority->getCacheSize(queue_lock), key_it->path().string(), size);

                    fs::remove(offset_it->path());
                }
            }
        }
    }

    /// Shuffle cells to have random order in LRUQueue as at startup all cells have the same priority.
    pcg64 generator(randomSeed());
    std::shuffle(queue_entries.begin(), queue_entries.end(), generator);
    for (const auto & [it, file_segment] : queue_entries)
    {
        /// Cell cache size changed and, for example, 1st file segment fits into cache
        /// and 2nd file segment will fit only if first was evicted, then first will be removed and
        /// cell is nullptr here.
        if (file_segment.expired())
            continue;

        it->use(queue_lock);
    }
}

void KeyTransaction::reduceSizeToDownloaded(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    const CachePriorityQueueGuard::Lock & queue_lock)
{
    /**
     * In case file was partially downloaded and it's download cannot be continued
     * because of no space left in cache, we need to be able to cut cell's size to downloaded_size.
     */

    auto * cell = offsets->get(offset);
    const auto & file_segment = cell->file_segment;

    size_t downloaded_size = file_segment->downloaded_size;
    size_t full_size = file_segment->range().size();

    if (downloaded_size == full_size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Nothing to reduce, file segment fully downloaded: {}",
            file_segment->getInfoForLogUnlocked(segment_lock));
    }

    [[maybe_unused]] const auto & entry = **cell->queue_iterator;
    assert(file_segment->downloaded_size <= file_segment->reserved_size);
    assert(entry.size == file_segment->reserved_size);
    assert(entry.size >= file_segment->downloaded_size);

    if (file_segment->reserved_size > file_segment->downloaded_size)
    {
        int64_t extra_size = static_cast<ssize_t>(cell->file_segment->reserved_size) - static_cast<ssize_t>(file_segment->downloaded_size);
        cell->queue_iterator->incrementSize(-extra_size, queue_lock);
    }

    CreateFileSegmentSettings create_settings(file_segment->getKind());
    cell->file_segment = std::make_shared<FileSegment>(
        offset, downloaded_size, key, getCreator(), file_segment->cache,
        FileSegment::State::DOWNLOADED, create_settings);

    assert(file_segment->reserved_size == downloaded_size);
    assert(cell->size() == entry.size);
}

FileSegmentsHolderPtr FileCache::getSnapshot()
{
    assertInitialized();

    std::lock_guard lock(files_mutex);

    FileSegments file_segments;
    for (const auto & [key, metadata] : files)
    {
        for (const auto & [_, cell] : *metadata)
            file_segments.push_back(FileSegment::getSnapshot(cell.file_segment));
    }
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::getSnapshot(const Key & key)
{
    FileSegments file_segments;
    auto key_transaction = createKeyTransaction(key, KeyNotFoundPolicy::THROW);
    for (const auto & [_, cell] : key_transaction->getOffsets())
        file_segments.push_back(FileSegment::getSnapshot(cell.file_segment));
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::dumpQueue()
{
    assertInitialized();
    using QueueEntry = IFileCachePriority::Entry;
    using IterationResult = IFileCachePriority::IterationResult;

    FileSegments file_segments;
    main_priority->iterate([&](const QueueEntry & entry)
    {
        auto tx = entry.createKeyTransaction();
        auto * cell = tx->getOffsets().get(entry.offset);
        file_segments.push_back(FileSegment::getSnapshot(cell->file_segment));
        return IterationResult::CONTINUE;
    }, main_priority->lock());

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

std::vector<String> FileCache::tryGetCachePaths(const Key & key)
{
    assertInitialized();

    auto key_transaction = createKeyTransaction(key, KeyNotFoundPolicy::RETURN_NULL);
    if (!key_transaction)
        return {};

    std::vector<String> cache_paths;

    for (const auto & [offset, cell] : key_transaction->getOffsets())
    {
        if (cell.file_segment->state() == FileSegment::State::DOWNLOADED)
            cache_paths.push_back(getPathInLocalCache(key, offset, cell.file_segment->getKind()));
    }
    return cache_paths;
}

size_t FileCache::getUsedCacheSize() const
{
    auto lock = main_priority->lock();
    return main_priority->getCacheSize(lock);
}

size_t FileCache::getFileSegmentsNum() const
{
    auto lock = main_priority->lock();
    return main_priority->getElementsNum(lock);
}

FileCache::FileSegmentCell::FileSegmentCell(
    FileSegmentPtr file_segment_,
    KeyTransaction & key_transaction,
    IFileCachePriority & priority_queue,
    CachePriorityQueueGuard::Lock * queue_lock)
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
            if (!queue_lock)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Adding file segment with state DOWNLOADED requires locked queue lock");
            }
            queue_iterator = priority_queue.add(
                file_segment->key(), file_segment->offset(), file_segment->range().size(),
                key_transaction.getCreator(), *queue_lock);

            /// TODO: add destructor
            break;
        }
        case FileSegment::State::SKIP_CACHE:
        case FileSegment::State::EMPTY:
        case FileSegment::State::DOWNLOADING:
        {
            break;
        }
        default:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can create cell with either EMPTY, DOWNLOADED, DOWNLOADING state, got: {}",
                FileSegment::stateToString(file_segment->download_state));
    }
}

const FileCache::FileSegmentCell * FileCache::CacheCells::get(size_t offset) const
{
    auto it = find(offset);
    if (it == end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return &(it->second);
}

FileCache::FileSegmentCell * FileCache::CacheCells::get(size_t offset)
{
    auto it = find(offset);
    if (it == end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return &(it->second);
}

const FileCache::FileSegmentCell * FileCache::CacheCells::tryGet(size_t offset) const
{
    auto it = find(offset);
    if (it == end())
        return nullptr;
    return &(it->second);
}

FileCache::FileSegmentCell * FileCache::CacheCells::tryGet(size_t offset)
{
    auto it = find(offset);
    if (it == end())
        return nullptr;
    return &(it->second);
}

std::string FileCache::CacheCells::toString() const
{
    std::string result;
    for (auto it = begin(); it != end(); ++it)
    {
        if (it != begin())
            result += ", ";
        result += std::to_string(it->first);
    }
    return result;
}

void FileCache::assertCacheCorrectness()
{
    for (const auto & [key, metadata] : files)
    {
        for (const auto & [_, cell] : *metadata)
        {
            const auto & file_segment = cell.file_segment;
            file_segment->assertCorrectness();

            if (file_segment->reserved_size != 0)
            {
                assert(cell.queue_iterator);
                // assert(main_priority->contains(file_segment->key(), file_segment->offset()));
            }
        }
    }

    using QueueEntry = IFileCachePriority::Entry;
    using IterationResult = IFileCachePriority::IterationResult;

    auto lock = main_priority->lock();
    [[maybe_unused]] size_t total_size = 0;

    main_priority->iterate([&](const QueueEntry & entry) -> IterationResult
    {
        auto key_transaction = entry.createKeyTransaction();
        auto * cell = key_transaction->getOffsets().get(entry.offset);

        if (cell->size() != entry.size)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected {} == {} size ({})",
                cell->size(), entry.size, cell->file_segment->getInfoForLog());
        }

        total_size += entry.size;
        return IterationResult::CONTINUE;
    }, lock);

    assert(total_size == main_priority->getCacheSize(lock));
    assert(main_priority->getCacheSize(lock) <= max_size);
    assert(main_priority->getElementsNum(lock) <= max_element_size);
}

FileCache::QueryContextHolder::QueryContextHolder(
    const String & query_id_,
    FileCache * cache_,
    FileCache::QueryLimit::QueryContextPtr context_)
    : query_id(query_id_)
    , cache(cache_)
    , context(context_)
{
}

FileCache::QueryContextHolder::~QueryContextHolder()
{
    /// If only the query_map and the current holder hold the context_query,
    /// the query has been completed and the query_context is released.
    if (context && context.use_count() == 2)
        cache->query_limit->removeQueryContext(query_id, cache->main_priority->lock());
}

FileCache::QueryLimit::QueryContext::QueryContext(
    size_t max_cache_size_,
    bool skip_download_if_exceeds_query_cache_)
    : max_cache_size(max_cache_size_)
    , skip_download_if_exceeds_query_cache(skip_download_if_exceeds_query_cache_)
{
}

FileCache::QueryLimit::QueryContextPtr
FileCache::QueryLimit::tryGetQueryContext(const CachePriorityQueueGuard::Lock &)
{
    if (!isQueryInitialized())
        return nullptr;

    auto query_iter = query_map.find(std::string(CurrentThread::getQueryId()));
    return (query_iter == query_map.end()) ? nullptr : query_iter->second;
}

void FileCache::QueryLimit::removeQueryContext(const std::string & query_id, const CachePriorityQueueGuard::Lock &)
{
    auto query_iter = query_map.find(query_id);
    if (query_iter == query_map.end())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Attempt to release query context that does not exist (query_id: {})",
            query_id);
    }
    query_map.erase(query_iter);
}

FileCache::QueryLimit::QueryContextPtr FileCache::QueryLimit::getOrSetQueryContext(
    const std::string & query_id,
    const ReadSettings & settings,
    const CachePriorityQueueGuard::Lock &)
{
    if (query_id.empty())
        return nullptr;

    auto [it, inserted] = query_map.emplace(query_id, nullptr);
    if (inserted)
    {
        it->second = std::make_shared<QueryContext>(
            settings.max_query_cache_size, settings.skip_download_if_exceeds_query_cache);
    }

    return it->second;
}

FileCache::QueryContextHolderPtr FileCache::getQueryContextHolder(
    const String & query_id, const ReadSettings & settings)
{
    if (!query_limit || settings.max_query_cache_size == 0)
        return {};

    auto context = query_limit->getOrSetQueryContext(query_id, settings, main_priority->lock());
    return std::make_unique<QueryContextHolder>(query_id, this, std::move(context));
}

void FileCache::QueryLimit::QueryContext::remove(
    const Key & key,
    size_t offset,
    size_t size,
    const CachePriorityQueueGuard::Lock & queue_lock)
{
    std::lock_guard lock(mutex);
    if (cache_size < size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Deleted cache size exceeds existing cache size");

    if (!skip_download_if_exceeds_query_cache)
    {
        auto record = records.find({key, offset});
        if (record != records.end())
        {
            record->second->remove(queue_lock);
            records.erase({key, offset});
        }
    }
    cache_size -= size;
}

void FileCache::QueryLimit::QueryContext::reserve(
    const Key & key,
    size_t offset,
    size_t size,
    const KeyTransaction & key_transaction,
    const CachePriorityQueueGuard::Lock & queue_lock)
{
    std::lock_guard lock(mutex);
    if (cache_size + size > max_cache_size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Reserved cache size exceeds the remaining cache size (key: {}, offset: {})",
            key.toString(), offset);
    }

    if (!skip_download_if_exceeds_query_cache)
    {
        auto record = records.find({key, offset});
        if (record == records.end())
        {
             auto queue_iter = priority->add(key, offset, 0, key_transaction.getCreator(), queue_lock);
             record = records.insert({{key, offset}, queue_iter}).first;
        }
        record->second->incrementSize(size, queue_lock);
    }
    cache_size += size;
}

void FileCache::QueryLimit::QueryContext::use(const Key & key, size_t offset, const CachePriorityQueueGuard::Lock & queue_lock)
{
    if (skip_download_if_exceeds_query_cache)
        return;

    std::lock_guard lock(mutex);
    auto record = records.find({key, offset});
    if (record != records.end())
        record->second->use(queue_lock);
}

KeyTransactionPtr KeyTransactionCreator::create()
{
    return std::make_unique<KeyTransaction>(key, offsets, cache);
}

}
