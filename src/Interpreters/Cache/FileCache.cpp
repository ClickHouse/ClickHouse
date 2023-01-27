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
    , max_file_segment_size(cache_settings_.max_file_segment_size)
    , allow_persistent_files(cache_settings_.do_not_evict_index_and_mark_files)
    , bypass_cache_threshold(cache_settings_.enable_bypass_cache_with_threashold ? cache_settings_.bypass_cache_threashold : 0)
    , log(&Poco::Logger::get("FileCache"))
    , cleanup_keys_metadata_queue(std::make_shared<KeysQueue>())
{
    main_priority = std::make_unique<LRUFileCachePriority>(cache_settings_.max_size, cache_settings_.max_elements);

    if (cache_settings_.cache_hits_threshold)
        stash = std::make_unique<HitsCountStash>(cache_settings_.cache_hits_threshold, cache_settings_.max_elements);

    if (cache_settings_.enable_filesystem_query_cache_limit)
        query_limit = std::make_unique<QueryLimit>();
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
    const Key & key, const FileSegment::Range & range, const KeyTransaction & key_transaction)
{
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    if (bypass_cache_threshold && range.size() > bypass_cache_threshold)
    {
        auto file_segment = std::make_shared<FileSegment>(
            range.left, range.size(), key, nullptr, this,
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
    auto lock = metadata_guard.lock();

    cleanup_keys_metadata_queue->clear([this](const Key & cleanup_key)
    {
        [[maybe_unused]] const bool erased = metadata.erase(cleanup_key);
        chassert(erased);

        try
        {
            const fs::path prefix_path = fs::path(getPathInLocalCache(cleanup_key)).parent_path();
            if (fs::exists(prefix_path) && fs::is_empty(prefix_path))
                fs::remove_all(prefix_path);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    });

    auto it = metadata.find(key);
    if (it == metadata.end())
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
                it = metadata.emplace(key, std::make_shared<KeyMetadata>()).first;
                break;
            }
        }
    }
    else if (!it->second || !it->second->guard)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trash in metadata");

    return std::make_unique<KeyTransaction>(key, it->second, cleanup_keys_metadata_queue, this);
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

FileCache::KeyMetadata::iterator FileCache::addCell(
    const Key & key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    const CreateFileSegmentSettings & settings,
    KeyTransaction & key_transaction,
    CacheGuard::LockPtr * lock)
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
    if (stash && state == FileSegment::State::EMPTY)
    {
        if (!lock)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Using stash requires cache_lock");

        KeyAndOffset stash_key(key, offset);

        auto record_it = stash->records.find(stash_key);
        if (record_it == stash->records.end())
        {
            auto stash_queue = LockedCachePriority(*lock, *stash->queue);
            auto & stash_records = stash->records;

            stash_records.emplace(stash_key, stash_queue.add(key, offset, 0, key_transaction.getCreator()));

            if (stash_queue.getElementsCount() > stash->queue->getElementsLimit())
                stash_queue.pop();

            result_state = FileSegment::State::SKIP_CACHE;
        }
        else
        {
            result_state = LockedCachePriorityIterator(*lock, record_it->second).use() >= stash->hits_threshold
                ? FileSegment::State::EMPTY
                : FileSegment::State::SKIP_CACHE;
        }
    }
    else
    {
        result_state = state;
    }

    auto file_segment = std::make_shared<FileSegment>(
        offset, size, key, key_transaction.getCreator(), this, result_state, settings);

    std::optional<LockedCachePriority> locked_queue(lock ? LockedCachePriority(*lock, *main_priority) : std::optional<LockedCachePriority>{});

    FileSegmentCell cell(std::move(file_segment), key_transaction, locked_queue ? &*locked_queue : nullptr);

    auto [cell_it, inserted] = key_transaction.getOffsets().emplace(offset, std::move(cell));
    assert(inserted);

    return cell_it;
}

bool FileCache::tryReserve(const Key & key, size_t offset, size_t size)
{
    assertInitialized();
    auto lock = cache_guard.lock();
    auto key_transaction = createKeyTransaction(key, KeyNotFoundPolicy::THROW);
    return tryReserveUnlocked(key, offset, size, key_transaction, lock);
}

bool FileCache::tryReserveUnlocked(
    const Key & key,
    size_t offset,
    size_t size,
    KeyTransactionPtr key_transaction,
    CacheGuard::LockPtr lock)
{
    auto query_context = query_limit ? query_limit->tryGetQueryContext(lock) : nullptr;
    bool reserved;

    if (query_context)
    {
        const bool query_limit_exceeded = query_context->getSize() + size > query_context->getSizeLimit();
        reserved = (!query_limit_exceeded || query_context->recacheOnQueryLimitExceeded())
            && tryReserveImpl(query_context->getPriority(), key, offset, size, key_transaction, query_context.get(), lock);
    }
    else
    {
        reserved = tryReserveImpl(*main_priority, key, offset, size, key_transaction, nullptr, lock);
    }

    if (reserved && !key_transaction->getOffsets().created_base_directory)
    {
        fs::create_directories(getPathInLocalCache(key));
        key_transaction->getOffsets().created_base_directory = true;
    }

    return reserved;
}

void FileCache::iterateAndCollectKeyLocks(
    LockedCachePriority & priority,
    IterateAndCollectLocksFunc && func,
    KeyTransactionsMap & locked_map)
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
    });
}

bool FileCache::tryReserveImpl(
    IFileCachePriority & priority_queue,
    const Key & key,
    size_t offset,
    size_t size,
    KeyTransactionPtr key_transaction,
    QueryLimit::LockedQueryContext * query_context,
    CacheGuard::LockPtr priority_lock)
{
    /// Iterate cells in the priority of `priority_queue`.
    /// If some entry is in `priority_queue` it must be guaranteed to have a
    /// corresponding cache cell in key_transaction->offsets() and in
    /// query_context->records (if query_context != nullptr).
    /// When we evict some entry, then it must be removed from both:
    /// main_priority and query_context::priority (if query_context != nullptr).
    /// If we successfulkly reserved space, entry must be added to both:
    /// cells and query_context::records (if query_context != nullptr);

    LOG_TEST(log, "Reserving space {} for {}:{}", size, key.toString(), offset);

    LockedCachePriority locked_priority_queue(priority_lock, priority_queue);
    LockedCachePriority locked_main_priority(priority_lock, *main_priority);

    size_t queue_size = locked_priority_queue.getElementsCount();
    chassert(queue_size <= locked_priority_queue.getElementsLimit());

    /// A cell acquires a LRUQueue iterator on first successful space reservation attempt.
    auto * cell_for_reserve = key_transaction->getOffsets().tryGet(offset);
    if (!cell_for_reserve || !cell_for_reserve->queue_iterator)
        queue_size += 1;

    size_t removed_size = 0;
    auto is_overflow = [&]
    {
        /// max_size == 0 means unlimited cache size,
        /// max_element_size means unlimited number of cache elements.
        return (main_priority->getSizeLimit() != 0 && locked_main_priority.getSize() + size - removed_size > main_priority->getSizeLimit())
            || (main_priority->getElementsLimit() != 0 && queue_size > main_priority->getElementsLimit())
            || (query_context && query_context->getSize() + size - removed_size > query_context->getSizeLimit());
    };

    KeyTransactionsMap locked;
    locked[key] = key_transaction;

    using QueueEntry = IFileCachePriority::Entry;
    using IterationResult = IFileCachePriority::IterationResult;

    iterateAndCollectKeyLocks(
        locked_priority_queue,
        [&](const QueueEntry & entry, KeyTransaction & locked_key) -> IterateAndLockResult
    {
        if (!is_overflow())
            return { IterationResult::BREAK, false };

        auto * cell = locked_key.getOffsets().get(entry.offset);

        chassert(cell->queue_iterator);
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
                    locked_key.remove(file_segment, priority_lock);
                    break;
                }
            }

            removed_size += cell_size;
            --queue_size;
        }

        if (remove_current_it)
            return { IterationResult::REMOVE_AND_CONTINUE, save_key_transaction };

        return { IterationResult::CONTINUE, save_key_transaction };
    }, locked);

    if (is_overflow())
        return false;

    for (auto & [_, transaction] : locked)
    {
        for (const auto & offset_to_delete : transaction->delete_offsets)
        {
            auto * cell = transaction->getOffsets().get(offset_to_delete);
            transaction->remove(cell->file_segment, priority_lock);
            if (query_context)
                query_context->remove(key, offset);
        }
    }

    if (cell_for_reserve)
    {
        /// queue_iteratir is std::nullopt here if no space has been reserved yet, a cache cell
        /// acquires queue iterator on first successful space reservation attempt.
        /// If queue iterator already exists, we need to update the size after each space reservation.
        if (cell_for_reserve->queue_iterator)
            LockedCachePriorityIterator(priority_lock, cell_for_reserve->queue_iterator).incrementSize(size);
        else
        {
            /// Space reservation is incremental, so cache cell is created first (with state empty),
            /// and queue_iterator is assigned on first space reservation attempt.
            cell_for_reserve->queue_iterator = locked_main_priority.add(key, offset, size, key_transaction->getCreator());
        }
    }

    if (query_context)
    {
        auto queue_iterator = query_context->tryGet(key, offset);
        if (queue_iterator)
            LockedCachePriorityIterator(priority_lock, queue_iterator).incrementSize(size);
        else
            query_context->add(key, offset, LockedCachePriority(priority_lock, query_context->getPriority()).add(key, offset, size, key_transaction->getCreator()));
    }

    if (locked_main_priority.getSize() > (1ull << 63))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache became inconsistent. There must be a bug");

    return true;
}

void FileCache::removeKeyIfExists(const Key & key)
{
    assertInitialized();

    auto lock = cache_guard.lock();
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

            key_transaction->remove(cell->file_segment, lock);
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

    auto lock = cache_guard.lock();
    LockedCachePriority(lock, *main_priority).iterate([&](const QueueEntry & entry) -> IterationResult
    {
        auto key_transaction = entry.createKeyTransaction();
        auto * cell = key_transaction->getOffsets().get(entry.offset);

        if (cell->releasable())
        {
            cell->queue_iterator = {};
            key_transaction->remove(cell->file_segment, lock);
            return IterationResult::REMOVE_AND_CONTINUE;
        }
        return IterationResult::CONTINUE;
    });

    if (stash)
    {
        /// Remove all access information.
        stash->records.clear();
        LockedCachePriority(lock, *stash->queue).removeAll();
    }
}

KeyTransaction::KeyTransaction(
    const Key & key_,
    FileCache::KeyMetadataPtr offsets_,
    KeysQueuePtr cleanup_keys_metadata_queue_,
    const FileCache * cache_)
    : key(key_)
    , cache(cache_)
    , guard(offsets_->guard)
    , lock(guard->lock())
    , offsets(offsets_)
    , cleanup_keys_metadata_queue(cleanup_keys_metadata_queue_)
    , log(&Poco::Logger::get("KeyTransaction"))
{
}

KeyTransaction::~KeyTransaction()
{
    cleanupKeyDirectory();
}

void KeyTransaction::remove(FileSegmentPtr file_segment, CacheGuard::LockPtr cache_lock)
{
    /// We must hold pointer to file segment while removing it.
    chassert(file_segment->key() == key);
    remove(file_segment->offset(), file_segment->lock(), cache_lock);
}

bool KeyTransaction::isLastHolder(size_t offset)
{
    const auto * cell = getOffsets().get(offset);
    return cell->file_segment.use_count() == 2;
}

void KeyTransaction::remove(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    CacheGuard::LockPtr cache_lock)
{
    LOG_DEBUG(
        log, "Remove from cache. Key: {}, offset: {}",
        key.toString(), offset);

    auto * cell = offsets->get(offset);

    if (cell->queue_iterator)
        LockedCachePriorityIterator(cache_lock, cell->queue_iterator).remove();

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
    if (!cache->isInitialized())
        return;

    /// Someone might still need this directory.
    if (!offsets->empty())
        return;

    /// Now `offsets` empty and the key lock is still locked.
    /// So it is guaranteed that no one will add something.

    fs::path key_path = cache->getPathInLocalCache(key);
    if (fs::exists(key_path))
    {
        offsets->created_base_directory = false;
        fs::remove_all(key_path);
    }
    cleanup_keys_metadata_queue->add(key);
}

void FileCache::loadMetadata()
{
    auto lock = cache_guard.lock();
    LockedCachePriority priority_queue(lock, *main_priority);

    UInt64 offset = 0;
    size_t size = 0;
    std::vector<std::pair<IFileCachePriority::Iterator, std::weak_ptr<FileSegment>>> queue_entries;

    /// cache_base_path / key_prefix / key / offset
    if (!metadata.empty())
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

                if (tryReserveUnlocked(key, offset, size, key_transaction, lock))
                {
                    auto cell_it = addCell(
                        key, offset, size, FileSegment::State::DOWNLOADED,
                        CreateFileSegmentSettings(segment_kind), *key_transaction, &lock);

                    queue_entries.emplace_back(cell_it->second.queue_iterator, cell_it->second.file_segment);
                }
                else
                {
                    LOG_WARNING(
                        log,
                        "Cache capacity changed (max size: {}, used: {}), "
                        "cached file `{}` does not fit in cache anymore (size: {})",
                        priority_queue.getSizeLimit(), priority_queue.getSize(), key_it->path().string(), size);

                    fs::remove(offset_it->path());
                }
            }
        }
    }

    /// Shuffle cells to have random order in LRUQueue as at startup all cells have the same priority.
    pcg64 generator(randomSeed());
    std::shuffle(queue_entries.begin(), queue_entries.end(), generator);
    for (auto & [it, file_segment] : queue_entries)
    {
        /// Cell cache size changed and, for example, 1st file segment fits into cache
        /// and 2nd file segment will fit only if first was evicted, then first will be removed and
        /// cell is nullptr here.
        if (file_segment.expired())
            continue;

        LockedCachePriorityIterator(lock, it).use();
    }
}

void KeyTransaction::reduceSizeToDownloaded(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    CacheGuard::LockPtr cache_lock)
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

    [[maybe_unused]] const auto & entry = *LockedCachePriorityIterator(cache_lock, cell->queue_iterator);
    assert(file_segment->downloaded_size <= file_segment->reserved_size);
    assert(entry.size == file_segment->reserved_size);
    assert(entry.size >= file_segment->downloaded_size);

    if (file_segment->reserved_size > file_segment->downloaded_size)
    {
        int64_t extra_size = static_cast<ssize_t>(cell->file_segment->reserved_size) - static_cast<ssize_t>(file_segment->downloaded_size);
        LockedCachePriorityIterator(cache_lock, cell->queue_iterator).incrementSize(-extra_size);
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

    auto lock = metadata_guard.lock();

    FileSegments file_segments;
    for (const auto & [key, key_metadata] : metadata)
    {
        for (const auto & [_, cell] : *key_metadata)
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
    LockedCachePriority(cache_guard.lock(), *main_priority).iterate([&](const QueueEntry & entry)
    {
        auto tx = entry.createKeyTransaction();
        auto * cell = tx->getOffsets().get(entry.offset);
        file_segments.push_back(FileSegment::getSnapshot(cell->file_segment));
        return IterationResult::CONTINUE;
    });

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
    return LockedCachePriority(cache_guard.lock(), *main_priority).getSize();
}

size_t FileCache::getFileSegmentsNum() const
{
    return LockedCachePriority(cache_guard.lock(), *main_priority).getElementsCount();
}

FileCache::FileSegmentCell::FileSegmentCell(
    FileSegmentPtr file_segment_,
    KeyTransaction & key_transaction,
    LockedCachePriority * locked_queue)
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
            if (!locked_queue)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Adding file segment with state DOWNLOADED requires locked queue lock");
            }
            queue_iterator = locked_queue->add(
                file_segment->key(), file_segment->offset(), file_segment->range().size(), key_transaction.getCreator());

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

const FileCache::FileSegmentCell * FileCache::KeyMetadata::get(size_t offset) const
{
    auto it = find(offset);
    if (it == end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return &(it->second);
}

FileCache::FileSegmentCell * FileCache::KeyMetadata::get(size_t offset)
{
    auto it = find(offset);
    if (it == end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return &(it->second);
}

const FileCache::FileSegmentCell * FileCache::KeyMetadata::tryGet(size_t offset) const
{
    auto it = find(offset);
    if (it == end())
        return nullptr;
    return &(it->second);
}

FileCache::FileSegmentCell * FileCache::KeyMetadata::tryGet(size_t offset)
{
    auto it = find(offset);
    if (it == end())
        return nullptr;
    return &(it->second);
}

std::string FileCache::KeyMetadata::toString() const
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
    for (const auto & [key, key_metadata] : metadata)
    {
        for (const auto & [_, cell] : *key_metadata)
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

    auto lock = cache_guard.lock();
    LockedCachePriority queue(lock, *main_priority);
    [[maybe_unused]] size_t total_size = 0;

    queue.iterate([&](const QueueEntry & entry) -> IterationResult
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
    });

    chassert(queue.getSize() == total_size);
    chassert(queue.getSize() <= queue.getSizeLimit());
    chassert(queue.getElementsCount() <= queue.getElementsLimit());
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
    {
        auto lock = cache->cache_guard.lock();
        cache->query_limit->removeQueryContext(query_id, lock);
    }
}

FileCache::QueryLimit::LockedQueryContextPtr
FileCache::QueryLimit::tryGetQueryContext(CacheGuard::LockPtr lock)
{
    if (!isQueryInitialized())
        return nullptr;

    auto query_iter = query_map.find(std::string(CurrentThread::getQueryId()));
    return (query_iter == query_map.end()) ? nullptr : std::make_unique<LockedQueryContext>(query_iter->second, lock);
}

void FileCache::QueryLimit::removeQueryContext(const std::string & query_id, CacheGuard::LockPtr)
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
    CacheGuard::LockPtr)
{
    if (query_id.empty())
        return nullptr;

    auto [it, inserted] = query_map.emplace(query_id, nullptr);
    if (inserted)
    {
        it->second = std::make_shared<QueryContext>(
            settings.filesystem_cache_max_download_size, !settings.skip_download_if_exceeds_query_cache);
    }

    return it->second;
}

FileCache::QueryContextHolderPtr FileCache::getQueryContextHolder(
    const String & query_id, const ReadSettings & settings)
{
    if (!query_limit || settings.filesystem_cache_max_download_size == 0)
        return {};

    auto lock = cache_guard.lock();
    auto context = query_limit->getOrSetQueryContext(query_id, settings, lock);
    return std::make_unique<QueryContextHolder>(query_id, this, std::move(context));
}

void FileCache::QueryLimit::LockedQueryContext::add(const Key & key, size_t offset, IFileCachePriority::Iterator iterator)
{
    auto [_, inserted] = context->records.emplace(KeyAndOffset{key, offset}, iterator);
    if (!inserted)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add offset {} to query context under key {}, it already exists",
            offset, key.toString());
    }
}

void FileCache::QueryLimit::LockedQueryContext::remove(const Key & key, size_t offset)
{
    auto record = context->records.find({key, offset});
    if (record == context->records.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no {}:{} in query context", key.toString(), offset);

    LockedCachePriorityIterator(lock, record->second).remove();
    context->records.erase({key, offset});
}

IFileCachePriority::Iterator FileCache::QueryLimit::LockedQueryContext::tryGet(const Key & key, size_t offset)
{
    auto it = context->records.find({key, offset});
    if (it == context->records.end())
        return nullptr;
    return it->second;

}

KeyTransactionPtr KeyTransactionCreator::create()
{
    return std::make_unique<KeyTransaction>(key, offsets, cleanup_keys_metadata_queue, cache);
}

}
