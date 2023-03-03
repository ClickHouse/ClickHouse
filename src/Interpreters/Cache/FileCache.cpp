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
        query_limit = std::make_unique<FileCacheQueryLimit>();
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

FileSegments FileCache::getImpl(
    const Key & key, const FileSegment::Range & range, const LockedKey & locked_key)
{
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    if (bypass_cache_threshold && range.size() > bypass_cache_threshold)
    {
        auto file_segment = std::make_shared<FileSegment>(
            range.left, range.size(), key, std::weak_ptr<KeyMetadata>(), this,
            FileSegment::State::DETACHED, CreateFileSegmentSettings{});
        return { file_segment };
    }

    const auto & file_segments = *locked_key.getKeyMetadata();
    if (file_segments.empty())
        return {};

    FileSegments result;
    auto add_to_result = [&](const FileSegmentMetadata & file_segment_metadata)
    {
        if (file_segment_metadata.file_segment->isDownloaded())
        {
            if (file_segment_metadata.file_segment->getDownloadedSize(true) == 0)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot have zero size downloaded file segments. {}",
                    file_segment_metadata.file_segment->getInfoForLog());
            }

    #ifndef NDEBUG
            /**
            * Check that in-memory state of the cache is consistent with the state on disk.
            * Check only in debug build, because such checks can be done often and can be quite
            * expensive compared to overall query execution time.
            */

            fs::path path = file_segment_metadata.file_segment->getPathInLocalCache();
            if (!fs::exists(path))
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "File path does not exist, but file has DOWNLOADED state. {}",
                    file_segment_metadata.file_segment->getInfoForLog());
            }

            if (fs::file_size(path) == 0)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot have zero size downloaded file segments. {}",
                    file_segment_metadata.file_segment->getInfoForLog());
            }
    #endif
        }
        auto state = file_segment_metadata.file_segment->state();
        if (state == FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
            std::terminate();

        result.push_back(file_segment_metadata.file_segment);
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

        const auto & file_segment_metadata = file_segments.rbegin()->second;
        if (file_segment_metadata.file_segment->range().right < range.left)
            return {};

        add_to_result(file_segment_metadata);
    }
    else /// segment_it <-- segmment{k}
    {
        if (segment_it != file_segments.begin())
        {
            const auto & prev_file_segment_metadata = std::prev(segment_it)->second;
            const auto & prev_range = prev_file_segment_metadata.file_segment->range();

            if (range.left <= prev_range.right)
            {
                ///   segment{k-1}  segment{k}
                ///   [________]   [_____
                ///       [___________
                ///       ^
                ///       range.left
                add_to_result(prev_file_segment_metadata);
            }
        }

        ///  segment{k} ...       segment{k-1}  segment{k}                      segment{k}
        ///  [______              [______]     [____                        [________
        ///  [_________     OR              [________      OR    [______]   ^
        ///  ^                              ^                           ^   segment{k}.offset
        ///  range.left                     range.left                  range.right

        while (segment_it != file_segments.end())
        {
            const auto & file_segment_metadata = segment_it->second;
            if (range.right < file_segment_metadata.file_segment->range().left)
                break;

            add_to_result(file_segment_metadata);
            ++segment_it;
        }
    }

    return result;
}

FileSegments FileCache::splitRangeInfoFileSegments(
    const Key & key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    const CreateFileSegmentSettings & settings,
    LockedKey & locked_key)
{
    assert(size > 0);

    auto current_pos = offset;
    auto end_pos_non_included = offset + size;

    size_t current_file_segment_size;
    size_t remaining_size = size;

    FileSegments file_segments;
    while (current_pos < end_pos_non_included)
    {
        current_file_segment_size = std::min(remaining_size, max_file_segment_size);
        remaining_size -= current_file_segment_size;

        auto file_segment_metadata_it = addFileSegment(key, current_pos, current_file_segment_size, state, settings, locked_key, nullptr);
        file_segments.push_back(file_segment_metadata_it->second.file_segment);

        current_pos += current_file_segment_size;
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
    LockedKey & locked_key)
{
    /// There are segments [segment1, ..., segmentN]
    /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
    /// intersect with given range.

    /// It can have holes:
    /// [____________________]         -- requested range
    ///     [____]  [_]   [_________]  -- intersecting cache [segment1, ..., segmentN]
    ///
    /// For each such hole create a file_segment_metadata with file segment state EMPTY.

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
                current_pos, hole_size, key, std::weak_ptr<KeyMetadata>(),
                this, FileSegment::State::DETACHED, settings);

            file_segments.insert(it, file_segment);
        }
        else
        {
            file_segments.splice(
                it, splitRangeInfoFileSegments(
                    key, current_pos, hole_size, FileSegment::State::EMPTY, settings, locked_key));
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
                current_pos, hole_size, key, std::weak_ptr<KeyMetadata>(),
                this, FileSegment::State::DETACHED, settings);

            file_segments.insert(file_segments.end(), file_segment);
        }
        else
        {
            file_segments.splice(
                file_segments.end(),
                splitRangeInfoFileSegments(key, current_pos, hole_size, FileSegment::State::EMPTY, settings, locked_key));
        }
    }
}

FileSegmentsHolderPtr FileCache::set(const Key & key, size_t offset, size_t size, const CreateFileSegmentSettings & settings)
{
    assertInitialized();

    auto locked_key = createLockedKey(key, KeyNotFoundPolicy::CREATE_EMPTY);
    FileSegment::Range range(offset, offset + size - 1);

    auto file_segments = getImpl(key, range, *locked_key);
    if (!file_segments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Having intersection with already existing cache");

    if (settings.unbounded)
    {
        /// If the file is unbounded, we can create a single file_segment_metadata for it.
        auto file_segment_metadata_it = addFileSegment(key, offset, size, FileSegment::State::EMPTY, settings, *locked_key, nullptr);
        file_segments = {file_segment_metadata_it->second.file_segment};
    }
    else
        file_segments = splitRangeInfoFileSegments(key, offset, size, FileSegment::State::EMPTY, settings, *locked_key);

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

    auto locked_key = createLockedKey(key, KeyNotFoundPolicy::CREATE_EMPTY);

    /// Get all segments which intersect with the given range.
    auto file_segments = getImpl(key, range, *locked_key);
    if (file_segments.empty())
    {
        file_segments = splitRangeInfoFileSegments(
            key, offset, size, FileSegment::State::EMPTY, settings, *locked_key);
    }
    else
    {
        fillHolesWithEmptyFileSegments(
            file_segments, key, range, /* fill_with_detached */false, settings, *locked_key);
    }

    chassert(!file_segments.empty());
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::get(const Key & key, size_t offset, size_t size)
{
    assertInitialized();

    auto locked_key = createLockedKey(key, KeyNotFoundPolicy::RETURN_NULL);
    if (locked_key)
    {
        FileSegment::Range range(offset, offset + size - 1);

        /// Get all segments which intersect with the given range.
        auto file_segments = getImpl(key, range, *locked_key);
        if (!file_segments.empty())
        {
            fillHolesWithEmptyFileSegments(
                file_segments, key, range, /* fill_with_detached */true,
                CreateFileSegmentSettings{}, *locked_key);

            return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
        }
    }

    auto file_segment = std::make_shared<FileSegment>(
        offset, size, key,
        std::weak_ptr<KeyMetadata>(), this, FileSegment::State::DETACHED, CreateFileSegmentSettings{});

    return std::make_unique<FileSegmentsHolder>(FileSegments{file_segment});
}

KeyMetadata::iterator FileCache::addFileSegment(
    const Key & key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    const CreateFileSegmentSettings & settings,
    LockedKey & locked_key,
    const CacheGuard::Lock * lock)
{
    /// Create a file_segment_metadata and put it in `files` map by [key][offset].

    chassert(size > 0); /// Empty file segments in cache are not allowed.

    auto key_metadata = locked_key.getKeyMetadata();
    auto it = key_metadata->find(offset);
    if (it != key_metadata->end())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cache entry already exists for key: `{}`, offset: {}, size: {}.",
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

            stash_records.emplace(stash_key, stash_queue.add(key, offset, 0, key_metadata));

            if (stash_queue.getElementsCount() > stash->queue->getElementsLimit())
                stash_queue.pop();

            result_state = FileSegment::State::DETACHED;
        }
        else
        {
            result_state = LockedCachePriorityIterator(*lock, record_it->second).use() >= stash->hits_threshold
                ? FileSegment::State::EMPTY
                : FileSegment::State::DETACHED;
        }
    }
    else
    {
        result_state = state;
    }

    auto file_segment = std::make_shared<FileSegment>(offset, size, key, key_metadata, this, result_state, settings);

    std::optional<LockedCachePriority> locked_queue(lock ? LockedCachePriority(*lock, *main_priority) : std::optional<LockedCachePriority>{});

    FileSegmentMetadata file_segment_metadata(std::move(file_segment), locked_key, locked_queue ? &*locked_queue : nullptr);

    auto [file_segment_metadata_it, inserted] = key_metadata->emplace(offset, std::move(file_segment_metadata));
    assert(inserted);

    return file_segment_metadata_it;
}

bool FileCache::tryReserve(const Key & key, size_t offset, size_t size, KeyMetadataPtr key_metadata)
{
    assertInitialized();
    auto lock = cache_guard.lock();
    auto locked_key = createLockedKey(key, key_metadata);
    return tryReserveUnlocked(key, offset, size, locked_key, lock);
}

bool FileCache::tryReserveUnlocked(
    const Key & key,
    size_t offset,
    size_t size,
    LockedKeyPtr locked_key,
    const CacheGuard::Lock & lock)
{
    auto query_context = query_limit ? query_limit->tryGetQueryContext(lock) : nullptr;
    bool reserved;

    if (query_context)
    {
        const bool query_limit_exceeded = query_context->getSize() + size > query_context->getSizeLimit();
        reserved = (!query_limit_exceeded || query_context->recacheOnFileCacheQueryLimitExceeded())
            && tryReserveImpl(query_context->getPriority(), key, offset, size, locked_key, query_context.get(), lock);
    }
    else
    {
        reserved = tryReserveImpl(*main_priority, key, offset, size, locked_key, nullptr, lock);
    }

    if (reserved)
    {
        auto key_metadata = locked_key->getKeyMetadata();
        if (!key_metadata->created_base_directory)
        {
            fs::create_directories(getPathInLocalCache(key));
            key_metadata->created_base_directory = true;
        }
    }

    return reserved;
}

void FileCache::iterateAndCollectKeyLocks(
    LockedCachePriority & priority,
    IterateAndCollectLocksFunc && func,
    LockedKeysMap & locked_map) const
{
    priority.iterate([&, func = std::move(func)](const IFileCachePriority::Entry & entry)
    {
        LockedKeyPtr current;

        auto locked_it = locked_map.find(entry.key);
        const bool locked = locked_it != locked_map.end();
        if (locked)
            current = locked_it->second;
        else
            current = createLockedKey(entry.key, entry.getKeyMetadata());

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
    LockedKeyPtr locked_key,
    FileCacheQueryLimit::LockedQueryContext * query_context,
    const CacheGuard::Lock & priority_lock)
{
    /// Iterate cache entries in the priority of `priority_queue`.
    /// If some entry is in `priority_queue` it must be guaranteed to have a
    /// corresponding cache entry in locked_key->offsets() and in
    /// query_context->records (if query_context != nullptr).
    /// When we evict some entry, then it must be removed from both:
    /// main_priority and query_context::priority (if query_context != nullptr).
    /// If we successfulkly reserved space, entry must be added to both:
    /// cache entries and query_context::records (if query_context != nullptr);

    LOG_TEST(log, "Reserving space {} for {}:{}", size, key.toString(), offset);

    LockedCachePriority locked_priority_queue(priority_lock, priority_queue);
    LockedCachePriority locked_main_priority(priority_lock, *main_priority);

    size_t queue_size = locked_priority_queue.getElementsCount();
    chassert(queue_size <= locked_priority_queue.getElementsLimit());

    /// A file_segment_metadata acquires a LRUQueue iterator on first successful space reservation attempt.
    auto * file_segment_for_reserve = locked_key->getKeyMetadata()->tryGetByOffset(offset);
    if (!file_segment_for_reserve || !file_segment_for_reserve->queue_iterator)
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

    LockedKeysMap locked;
    locked[key] = locked_key;

    using QueueEntry = IFileCachePriority::Entry;
    using IterationResult = IFileCachePriority::IterationResult;

    iterateAndCollectKeyLocks(
        locked_priority_queue,
        [&](const QueueEntry & entry, LockedKey & current_locked_key) -> IterateAndLockResult
    {
        if (!is_overflow())
            return { IterationResult::BREAK, false };

        auto * file_segment_metadata = current_locked_key.getKeyMetadata()->getByOffset(entry.offset);

        chassert(file_segment_metadata->queue_iterator);
        chassert(entry.size == file_segment_metadata->size());

        const size_t file_segment_size = file_segment_metadata->size();
        bool remove_current_it = false;

        /// It is guaranteed that file_segment_metadata is not removed from cache as long as
        /// pointer to corresponding file segment is hold by any other thread.

        bool save_locked_key = false;
        if (file_segment_metadata->releasable())
        {
            auto file_segment = file_segment_metadata->file_segment;

            chassert(entry.offset == file_segment->offset());
            if (file_segment->isPersistent() && allow_persistent_files)
            {
                return { IterationResult::CONTINUE, false };
            }

            switch (file_segment->state())
            {
                case FileSegment::State::DOWNLOADED:
                {
                    /// file_segment_metadata will actually be removed only if we managed to reserve enough space.

                    current_locked_key.delete_offsets.push_back(file_segment->offset());
                    save_locked_key = true;
                    break;
                }
                default:
                {
                    remove_current_it = true;
                    file_segment_metadata->queue_iterator = {};
                    current_locked_key.remove(file_segment, priority_lock);
                    break;
                }
            }

            removed_size += file_segment_size;
            --queue_size;
        }

        if (remove_current_it)
            return { IterationResult::REMOVE_AND_CONTINUE, save_locked_key };

        return { IterationResult::CONTINUE, save_locked_key };
    }, locked);

    if (is_overflow())
        return false;

    for (auto it = locked.begin(); it != locked.end();)
    {
        auto & current_locked_key = it->second;
        for (const auto & offset_to_delete : current_locked_key->delete_offsets)
        {
            auto * file_segment_metadata = current_locked_key->getKeyMetadata()->getByOffset(offset_to_delete);
            current_locked_key->remove(file_segment_metadata->file_segment, priority_lock);
            if (query_context)
                query_context->remove(key, offset);
        }

        /// Do not hold the key lock longer than required.
        it = locked.erase(it);
    }

    if (file_segment_for_reserve)
    {
        /// queue_iteratir is std::nullopt here if no space has been reserved yet, a file_segment_metadata
        /// acquires queue iterator on first successful space reservation attempt.
        /// If queue iterator already exists, we need to update the size after each space reservation.
        if (file_segment_for_reserve->queue_iterator)
            LockedCachePriorityIterator(priority_lock, file_segment_for_reserve->queue_iterator).incrementSize(size);
        else
        {
            /// Space reservation is incremental, so file_segment_metadata is created first (with state empty),
            /// and queue_iterator is assigned on first space reservation attempt.
            file_segment_for_reserve->queue_iterator = locked_main_priority.add(key, offset, size, locked_key->getKeyMetadata());
        }
    }

    if (query_context)
    {
        auto queue_iterator = query_context->tryGet(key, offset);
        if (queue_iterator)
        {
            LockedCachePriorityIterator(priority_lock, queue_iterator).incrementSize(size);
        }
        else
        {
            auto it = LockedCachePriority(priority_lock, query_context->getPriority()).add(key, offset, size, locked_key->getKeyMetadata());
            query_context->add(key, offset, it);
        }
    }

    if (locked_main_priority.getSize() > (1ull << 63))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache became inconsistent. There must be a bug");

    return true;
}

void FileCache::removeKeyIfExists(const Key & key)
{
    assertInitialized();

    auto lock = cache_guard.lock();
    auto locked_key = createLockedKey(key, KeyNotFoundPolicy::RETURN_NULL);
    if (!locked_key)
        return;

    auto & offsets = *locked_key->getKeyMetadata();
    if (!offsets.empty())
    {
        std::vector<FileSegmentMetadata *> remove_file_segment_metadatas;
        remove_file_segment_metadatas.reserve(offsets.size());
        for (auto & [offset, file_segment_metadata] : offsets)
            remove_file_segment_metadatas.push_back(&file_segment_metadata);

        for (auto & file_segment_metadata : remove_file_segment_metadatas)
        {
            /// In ordinary case we remove data from cache when it's not used by anyone.
            /// But if we have multiple replicated zero-copy tables on the same server
            /// it became possible to start removing something from cache when it is used
            /// by other "zero-copy" tables. That is why it's not an error.
            if (!file_segment_metadata->releasable())
                continue;

            locked_key->remove(file_segment_metadata->file_segment, lock);
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
        auto locked_key = createLockedKey(entry.key, entry.getKeyMetadata());
        auto * file_segment_metadata = locked_key->getKeyMetadata()->getByOffset(entry.offset);

        if (file_segment_metadata->releasable())
        {
            file_segment_metadata->queue_iterator = {};
            locked_key->remove(file_segment_metadata->file_segment, lock);
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

                auto locked_key = createLockedKey(key, KeyNotFoundPolicy::CREATE_EMPTY);
                locked_key->getKeyMetadata()->created_base_directory = true;

                if (tryReserveUnlocked(key, offset, size, locked_key, lock))
                {
                    auto file_segment_metadata_it = addFileSegment(
                        key, offset, size, FileSegment::State::DOWNLOADED,
                        CreateFileSegmentSettings(segment_kind), *locked_key, &lock);

                    queue_entries.emplace_back(
                        file_segment_metadata_it->second.queue_iterator, file_segment_metadata_it->second.file_segment);
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

    /// Shuffle file_segment_metadatas to have random order in LRUQueue as at startup all file_segment_metadatas have the same priority.
    pcg64 generator(randomSeed());
    std::shuffle(queue_entries.begin(), queue_entries.end(), generator);
    for (auto & [it, file_segment] : queue_entries)
    {
        /// Cache size changed and, for example, 1st file segment fits into cache
        /// and 2nd file segment will fit only if first was evicted, then first will be removed and
        /// file_segment_metadata is nullptr here.
        if (file_segment.expired())
            continue;

        LockedCachePriorityIterator(lock, it).use();
    }
}

void FileCache::performDelayedRemovalOfDeletedKeysFromMetadata(const CacheMetadataGuard::Lock &)
{
    cleanup_keys_metadata_queue->clear([this](const Key & cleanup_key)
    {
        auto it = metadata.find(cleanup_key);
        if (it == metadata.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key {} in metadata", cleanup_key.toString());

        /// We must also lock the key.
        auto guard = it->second->guard;
        auto key_lock = guard->lock();

        /// Remove key from metadata.
        metadata.erase(it);

        try
        {
            /// Delete key directory if not yet deleted.
            const fs::path path = fs::path(getPathInLocalCache(cleanup_key));
            if (fs::exists(path))
                fs::remove_all(path);

            /// Delete three digit directory if empty.
            const fs::path prefix_path = path.parent_path();
            if (fs::exists(prefix_path) && fs::is_empty(prefix_path))
                fs::remove_all(prefix_path);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    });
    /// TODO: add assertCacheCorrectness().
}

LockedKeyPtr FileCache::createLockedKey(const Key & key, KeyNotFoundPolicy key_not_found_policy)
{
    auto lock = metadata.lock();
    performDelayedRemovalOfDeletedKeysFromMetadata(lock);

    auto it = metadata.find(key);
    if (it == metadata.end())
    {
        if (key_not_found_policy == KeyNotFoundPolicy::THROW)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key `{}` in cache", key.toString());
        else if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL)
            return nullptr;

        it = metadata.emplace(key, std::make_shared<KeyMetadata>()).first;
    }
    else if (!it->second || !it->second->guard)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trash in metadata");
    }

    auto key_metadata = it->second;
    auto key_guard = key_metadata->guard;
    auto key_lock = key_guard->lock();

    /// Removal of empty key from cache metadata is delayed.
    /// Therefore here we need to check if the key which we currently took from cache metadata is a valid non-removed key.
    /// Additional explanation:
    ///   KeyMetadata::removed can be changed only under according KeyGuard::Lock. keyMetadata::removed is needed
    ///   for delayed metadata cleanup, because usually we manage key metadata under key lock, e.g. delete the
    ///   key metadata objects as well. So we could delete all key objects and the see that we deleted everything
    ///   that we had for this key, so we consider that we need to remove this key from cache metadata. But in order
    ///   to do that we need CacheMetadataGuard::Lock, which cannot be taken while holding KeyGuard::Lock.
    ///   Therefore we need to retake locks in needed order to removed empty key from cache metadata. But it makes
    ///   sense to do that with delay here.
    if (key_metadata->removed)
    {
        /// If it is removed, then do perform delayed removal.
        metadata.erase(it);
        /// Get metadata one more time.
        /// Throw exception if KeyNotFoundPolicy::THROW, return nullptr if KeyNotFoundPolicy::RETURN_NULL.
        if (key_not_found_policy == KeyNotFoundPolicy::THROW)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key `{}` in cache", key.toString());
        else if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL)
            return nullptr;

        it = metadata.emplace(key, std::make_shared<KeyMetadata>()).first;
        return std::make_unique<LockedKey>(key, it->second, it->second->guard->lock(), cleanup_keys_metadata_queue, this);
    }

    return std::make_unique<LockedKey>(key, key_metadata, std::move(key_lock), cleanup_keys_metadata_queue, this);
}

LockedKeyPtr FileCache::createLockedKey(const Key & key, KeyMetadataPtr key_metadata) const
{
    auto key_lock = key_metadata->guard->lock();
    if (key_metadata->removed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot lock key: it was removed from cache");
    return std::make_unique<LockedKey>(key, key_metadata, std::move(key_lock), cleanup_keys_metadata_queue, this);
}

FileSegmentsHolderPtr FileCache::getSnapshot()
{
    assertInitialized();

    auto lock = metadata.lock();
    performDelayedRemovalOfDeletedKeysFromMetadata(lock);

    FileSegments file_segments;
    for (auto it = metadata.begin(); it != metadata.end();)
    {
        auto key_metadata = it->second;
        auto key_guard = key_metadata->guard;
        auto key_lock = key_guard->lock();

        /// Perform delayed removal of deleted key from cache metadata.
        if (key_metadata->removed)
        {
            it = metadata.erase(it);
            continue;
        }

        for (const auto & [_, file_segment_metadata] : *key_metadata)
            file_segments.push_back(FileSegment::getSnapshot(file_segment_metadata.file_segment));

        ++it;
    }
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::getSnapshot(const Key & key)
{
    FileSegments file_segments;
    auto locked_key = createLockedKey(key, KeyNotFoundPolicy::THROW);
    auto key_metadata = locked_key->getKeyMetadata();
    for (const auto & [_, file_segment_metadata] : *key_metadata)
        file_segments.push_back(FileSegment::getSnapshot(file_segment_metadata.file_segment));
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
        auto tx = createLockedKey(entry.key, entry.getKeyMetadata());
        auto * file_segment_metadata = tx->getKeyMetadata()->getByOffset(entry.offset);
        file_segments.push_back(FileSegment::getSnapshot(file_segment_metadata->file_segment));
        return IterationResult::CONTINUE;
    });

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

std::vector<String> FileCache::tryGetCachePaths(const Key & key)
{
    assertInitialized();

    auto locked_key = createLockedKey(key, KeyNotFoundPolicy::RETURN_NULL);
    if (!locked_key)
        return {};

    std::vector<String> cache_paths;

    for (const auto & [offset, file_segment_metadata] : *locked_key->getKeyMetadata())
    {
        if (file_segment_metadata.file_segment->state() == FileSegment::State::DOWNLOADED)
            cache_paths.push_back(getPathInLocalCache(key, offset, file_segment_metadata.file_segment->getKind()));
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

void FileCache::assertCacheCorrectness()
{
    for (const auto & [key, key_metadata] : metadata)
    {
        for (const auto & [_, file_segment_metadata] : *key_metadata)
        {
            const auto & file_segment = file_segment_metadata.file_segment;
            file_segment->assertCorrectness();

            if (file_segment->reserved_size != 0)
            {
                assert(file_segment_metadata.queue_iterator);
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
        auto locked_key = createLockedKey(entry.key, entry.getKeyMetadata());
        auto * file_segment_metadata = locked_key->getKeyMetadata()->getByOffset(entry.offset);

        if (file_segment_metadata->size() != entry.size)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Expected {} == {} size ({})",
                file_segment_metadata->size(), entry.size, file_segment_metadata->file_segment->getInfoForLog());
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
    FileCacheQueryLimit::QueryContextPtr context_)
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

FileCache::QueryContextHolderPtr FileCache::getQueryContextHolder(
    const String & query_id, const ReadSettings & settings)
{
    if (!query_limit || settings.filesystem_cache_max_download_size == 0)
        return {};

    auto lock = cache_guard.lock();
    auto context = query_limit->getOrSetQueryContext(query_id, settings, lock);
    return std::make_unique<QueryContextHolder>(query_id, this, std::move(context));
}

}
