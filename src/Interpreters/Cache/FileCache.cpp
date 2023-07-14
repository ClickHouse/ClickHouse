#include "FileCache.h"

#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Context.h>
#include <base/hex.h>
#include <pcg-random/pcg_random.hpp>
#include <Common/randomSeed.h>
#include <Common/ThreadPool.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event FilesystemCacheEvictedBytes;
    extern const Event FilesystemCacheEvictedFileSegments;
    extern const Event FilesystemCacheLockCacheMicroseconds;
    extern const Event FilesystemCacheReserveMicroseconds;
    extern const Event FilesystemCacheEvictMicroseconds;
    extern const Event FilesystemCacheGetOrSetMicroseconds;
    extern const Event FilesystemCacheGetMicroseconds;
}

namespace
{

size_t roundDownToMultiple(size_t num, size_t multiple)
{
    return (num / multiple) * multiple;
}

size_t roundUpToMultiple(size_t num, size_t multiple)
{
    return roundDownToMultiple(num + multiple - 1, multiple);
}
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FileCache::FileCache(const FileCacheSettings & settings)
    : max_file_segment_size(settings.max_file_segment_size)
    , bypass_cache_threshold(settings.enable_bypass_cache_with_threashold ? settings.bypass_cache_threashold : 0)
    , delayed_cleanup_interval_ms(settings.delayed_cleanup_interval_ms)
    , boundary_alignment(settings.boundary_alignment)
    , background_download_threads(settings.background_download_threads)
    , log(&Poco::Logger::get("FileCache"))
    , metadata(settings.base_path)
{
    main_priority = std::make_unique<LRUFileCachePriority>(settings.max_size, settings.max_elements);

    if (settings.cache_hits_threshold)
        stash = std::make_unique<HitsCountStash>(settings.cache_hits_threshold, settings.max_elements);

    if (settings.enable_filesystem_query_cache_limit)
        query_limit = std::make_unique<FileCacheQueryLimit>();
}

FileCache::Key FileCache::createKeyForPath(const String & path)
{
    return Key(path);
}

const String & FileCache::getBasePath() const
{
    return metadata.getBaseDirectory();
}

String FileCache::getPathInLocalCache(const Key & key, size_t offset, FileSegmentKind segment_kind) const
{
    return metadata.getPathForFileSegment(key, offset, segment_kind);
}

String FileCache::getPathInLocalCache(const Key & key) const
{
    return metadata.getPathForKey(key);
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
        if (fs::exists(getBasePath()))
        {
            loadMetadata();
        }
        else
        {
            fs::create_directories(getBasePath());
        }
    }
    catch (...)
    {
        init_exception = std::current_exception();
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    is_initialized = true;

    for (size_t i = 0; i < background_download_threads; ++i)
         download_threads.emplace_back([this] { metadata.downloadThreadFunc(); });

    cleanup_task = Context::getGlobalContextInstance()->getSchedulePool().createTask("FileCacheCleanup", [this]{ cleanupThreadFunc(); });
    cleanup_task->activate();
    cleanup_task->scheduleAfter(delayed_cleanup_interval_ms);
}

CacheGuard::Lock FileCache::lockCache() const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockCacheMicroseconds);
    return cache_guard.lock();
}

FileSegments FileCache::getImpl(const LockedKey & locked_key, const FileSegment::Range & range) const
{
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    if (bypass_cache_threshold && range.size() > bypass_cache_threshold)
    {
        auto file_segment = std::make_shared<FileSegment>(
            locked_key.getKey(), range.left, range.size(), FileSegment::State::DETACHED);
        return { file_segment };
    }

    const auto & file_segments = *locked_key.getKeyMetadata();
    if (file_segments.empty())
        return {};

    FileSegments result;
    auto add_to_result = [&](const FileSegmentMetadata & file_segment_metadata)
    {
        FileSegmentPtr file_segment;
        if (!file_segment_metadata.evicting())
        {
            file_segment = file_segment_metadata.file_segment;
            if (file_segment->isDownloaded())
            {
                if (file_segment->getDownloadedSize(true) == 0)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Cannot have zero size downloaded file segments. {}",
                        file_segment->getInfoForLog());
                }

#ifndef NDEBUG
                /**
                * Check that in-memory state of the cache is consistent with the state on disk.
                * Check only in debug build, because such checks can be done often and can be quite
                * expensive compared to overall query execution time.
                */

                fs::path path = file_segment->getPathInLocalCache();
                if (!fs::exists(path))
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "File path does not exist, but file has DOWNLOADED state. {}",
                        file_segment->getInfoForLog());
                }

                if (fs::file_size(path) == 0)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Cannot have zero size downloaded file segments. {}",
                        file_segment->getInfoForLog());
                }
#endif
            }
        }
        else
        {
            file_segment = std::make_shared<FileSegment>(
                locked_key.getKey(),
                file_segment_metadata.file_segment->offset(),
                file_segment_metadata.file_segment->range().size(),
                FileSegment::State::DETACHED);
        }

        result.push_back(file_segment);
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

        const auto & file_segment_metadata = *file_segments.rbegin()->second;
        if (file_segment_metadata.file_segment->range().right < range.left)
            return {};

        add_to_result(file_segment_metadata);
    }
    else /// segment_it <-- segmment{k}
    {
        if (segment_it != file_segments.begin())
        {
            const auto & prev_file_segment_metadata = *std::prev(segment_it)->second;
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
            const auto & file_segment_metadata = *segment_it->second;
            if (range.right < file_segment_metadata.file_segment->range().left)
                break;

            add_to_result(file_segment_metadata);
            ++segment_it;
        }
    }

    return result;
}

FileSegments FileCache::splitRangeIntoFileSegments(
    LockedKey & locked_key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    const CreateFileSegmentSettings & settings)
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

        auto file_segment_metadata_it = addFileSegment(
            locked_key, current_pos, current_file_segment_size, state, settings, nullptr);
        file_segments.push_back(file_segment_metadata_it->second->file_segment);

        current_pos += current_file_segment_size;
    }

    assert(file_segments.empty() || offset + size - 1 == file_segments.back()->range().right);
    return file_segments;
}

void FileCache::fillHolesWithEmptyFileSegments(
    LockedKey & locked_key,
    FileSegments & file_segments,
    const FileSegment::Range & range,
    bool fill_with_detached_file_segments,
    const CreateFileSegmentSettings & settings)
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
                locked_key.getKey(), current_pos, hole_size, FileSegment::State::DETACHED, settings);

            file_segments.insert(it, file_segment);
        }
        else
        {
            auto split = splitRangeIntoFileSegments(
                locked_key, current_pos, hole_size, FileSegment::State::EMPTY, settings);
            file_segments.splice(it, std::move(split));
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
                locked_key.getKey(), current_pos, hole_size, FileSegment::State::DETACHED, settings);

            file_segments.insert(file_segments.end(), file_segment);
        }
        else
        {
            auto split = splitRangeIntoFileSegments(
                locked_key, current_pos, hole_size, FileSegment::State::EMPTY, settings);
            file_segments.splice(file_segments.end(), std::move(split));
        }
    }
}

FileSegmentsHolderPtr FileCache::set(
    const Key & key,
    size_t offset,
    size_t size,
    const CreateFileSegmentSettings & settings)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY);
    FileSegment::Range range(offset, offset + size - 1);

    auto file_segments = getImpl(*locked_key, range);
    if (!file_segments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Having intersection with already existing cache");

    if (settings.unbounded)
    {
        /// If the file is unbounded, we can create a single file_segment_metadata for it.
        auto file_segment_metadata_it = addFileSegment(
            *locked_key, offset, size, FileSegment::State::EMPTY, settings, nullptr);
        file_segments = {file_segment_metadata_it->second->file_segment};
    }
    else
    {
        file_segments = splitRangeIntoFileSegments(
            *locked_key, offset, size, FileSegment::State::EMPTY, settings);
    }

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr
FileCache::getOrSet(
    const Key & key,
    size_t offset,
    size_t size,
    size_t file_size,
    const CreateFileSegmentSettings & settings)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);

    assertInitialized();

    const auto aligned_offset = roundDownToMultiple(offset, boundary_alignment);
    const auto aligned_end = std::min(roundUpToMultiple(offset + size, boundary_alignment), file_size);
    const auto aligned_size = aligned_end - aligned_offset;

    FileSegment::Range range(aligned_offset, aligned_offset + aligned_size - 1);

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY);

    /// Get all segments which intersect with the given range.
    auto file_segments = getImpl(*locked_key, range);
    if (file_segments.empty())
    {
        file_segments = splitRangeIntoFileSegments(*locked_key, range.left, range.size(), FileSegment::State::EMPTY, settings);
    }
    else
    {
        fillHolesWithEmptyFileSegments(
            *locked_key, file_segments, range, /* fill_with_detached */false, settings);
    }

    while (!file_segments.empty() && file_segments.front()->range().right < offset)
        file_segments.pop_front();

    while (!file_segments.empty() && file_segments.back()->range().left >= offset + size)
        file_segments.pop_back();

    chassert(!file_segments.empty());
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::get(const Key & key, size_t offset, size_t size)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheGetMicroseconds);

    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL);
    if (locked_key)
    {
        FileSegment::Range range(offset, offset + size - 1);

        /// Get all segments which intersect with the given range.
        auto file_segments = getImpl(*locked_key, range);
        if (!file_segments.empty())
        {
            fillHolesWithEmptyFileSegments(
                *locked_key, file_segments, range, /* fill_with_detached */true, CreateFileSegmentSettings{});

            return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
        }
    }

    return std::make_unique<FileSegmentsHolder>(FileSegments{
        std::make_shared<FileSegment>(key, offset, size, FileSegment::State::DETACHED)});
}

KeyMetadata::iterator FileCache::addFileSegment(
    LockedKey & locked_key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    const CreateFileSegmentSettings & settings,
    const CacheGuard::Lock * lock)
{
    /// Create a file_segment_metadata and put it in `files` map by [key][offset].

    chassert(size > 0); /// Empty file segments in cache are not allowed.

    const auto & key = locked_key.getKey();
    const FileSegment::Range range(offset, offset + size - 1);

    if (auto intersecting_range = locked_key.hasIntersectingRange(range))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Attempt to add intersecting file segment in cache ({} intersects {})",
            range.toString(), intersecting_range->toString());
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
            auto & stash_records = stash->records;

            stash_records.emplace(
                stash_key, stash->queue->add(locked_key.getKeyMetadata(), offset, 0, *lock));

            if (stash->queue->getElementsCount(*lock) > stash->queue->getElementsLimit())
                stash->queue->pop(*lock);

            result_state = FileSegment::State::DETACHED;
        }
        else
        {
            result_state = record_it->second->use(*lock) >= stash->hits_threshold
                ? FileSegment::State::EMPTY
                : FileSegment::State::DETACHED;
        }
    }
    else
    {
        result_state = state;
    }

    PriorityIterator cache_it;
    if (state == FileSegment::State::DOWNLOADED)
    {
        cache_it = main_priority->add(locked_key.getKeyMetadata(), offset, size, *lock);
    }

    try
    {
        auto file_segment = std::make_shared<FileSegment>(
            key, offset, size, result_state, settings, this, locked_key.getKeyMetadata(), cache_it);
        auto file_segment_metadata = std::make_shared<FileSegmentMetadata>(std::move(file_segment));

        auto [file_segment_metadata_it, inserted] = locked_key.getKeyMetadata()->emplace(offset, file_segment_metadata);
        if (!inserted)
        {
            if (cache_it)
                cache_it->remove(*lock);

            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Failed to insert {}:{}: entry already exists", key, offset);
        }

        return file_segment_metadata_it;
    }
    catch (...)
    {
        if (cache_it)
            cache_it->remove(*lock);
        throw;
    }
}

bool FileCache::tryReserve(FileSegment & file_segment, const size_t size)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheReserveMicroseconds);

    assertInitialized();
    auto cache_lock = lockCache();

    LOG_TEST(
        log, "Trying to reserve space ({} bytes) for {}:{}, current usage {}/{}",
        size, file_segment.key(), file_segment.offset(),
        main_priority->getSize(cache_lock), main_priority->getSizeLimit());

    /// In case of per query cache limit (by default disabled), we add/remove entries from both
    /// (main_priority and query_priority) priority queues, but iterate entries in order of query_priority,
    /// while checking the limits in both.
    Priority * query_priority = nullptr;

    auto query_context = query_limit ? query_limit->tryGetQueryContext(cache_lock) : nullptr;
    if (query_context)
    {
        query_priority = &query_context->getPriority();

        const bool query_limit_exceeded = query_priority->getSize(cache_lock) + size > query_priority->getSizeLimit();
        if (query_limit_exceeded && !query_context->recacheOnFileCacheQueryLimitExceeded())
        {
            LOG_TEST(log, "Query limit exceeded, space reservation failed, "
                     "recache_on_query_limit_exceeded is disabled (while reserving for {}:{})",
                     file_segment.key(), file_segment.offset());
            return false;
        }

        LOG_TEST(
            log, "Using query limit, current usage: {}/{} (while reserving for {}:{})",
            query_priority->getSize(cache_lock), query_priority->getSizeLimit(),
            file_segment.key(), file_segment.offset());
    }

    struct EvictionCandidates
    {
        explicit EvictionCandidates(KeyMetadataPtr key_metadata_) : key_metadata(std::move(key_metadata_)) {}

        void add(const FileSegmentMetadataPtr & candidate)
        {
            candidate->removal_candidate = true;
            candidates.push_back(candidate);
        }

        ~EvictionCandidates()
        {
            /// If failed to reserve space, we don't delete the candidates but drop the flag instead
            /// so the segments can be used again
            for (const auto & candidate : candidates)
                candidate->removal_candidate = false;
        }

        KeyMetadataPtr key_metadata;
        std::vector<FileSegmentMetadataPtr> candidates;
    };

    std::unordered_map<Key, EvictionCandidates> to_delete;
    size_t freeable_space = 0, freeable_count = 0;

    auto iterate_func = [&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
    {
        chassert(segment_metadata->file_segment->assertCorrectness());

        if (segment_metadata->releasable())
        {
            const auto & key = segment_metadata->file_segment->key();
            auto it = to_delete.find(key);
            if (it == to_delete.end())
                it = to_delete.emplace(key, locked_key.getKeyMetadata()).first;
            it->second.add(segment_metadata);

            freeable_space += segment_metadata->size();
            ++freeable_count;
        }
        return PriorityIterationResult::CONTINUE;
    };

    if (query_priority)
    {
        auto is_query_priority_overflow = [&]
        {
            const size_t new_size = query_priority->getSize(cache_lock) + size - freeable_space;
            return new_size > query_priority->getSizeLimit();
        };

        if (is_query_priority_overflow())
        {
            query_priority->iterate(
                [&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
                { return is_query_priority_overflow() ? iterate_func(locked_key, segment_metadata) : PriorityIterationResult::BREAK; },
                cache_lock);

            if (is_query_priority_overflow())
                return false;
        }

        LOG_TEST(
            log, "Query limits satisfied (while reserving for {}:{})",
            file_segment.key(), file_segment.offset());
    }

    auto is_main_priority_overflow = [main_priority_size_limit = main_priority->getSizeLimit(),
                                      main_priority_elements_limit = main_priority->getElementsLimit(),
                                      size,
                                      &freeable_space,
                                      &freeable_count,
                                      &file_segment,
                                      &cache_lock,
                                      my_main_priority = this->main_priority.get(),
                                      my_log = this->log]
    {
        const bool is_overflow =
            /// size_limit == 0 means unlimited cache size
            (main_priority_size_limit != 0 && (my_main_priority->getSize(cache_lock) + size - freeable_space > main_priority_size_limit))
            /// elements_limit == 0 means unlimited number of cache elements
            || (main_priority_elements_limit != 0 && freeable_count == 0
                && my_main_priority->getElementsCount(cache_lock) == main_priority_elements_limit);

        LOG_TEST(
            my_log, "Overflow: {}, size: {}, ready to remove: {} ({} in number), current cache size: {}/{}, elements: {}/{}, while reserving for {}:{}",
            is_overflow, size, freeable_space, freeable_count,
            my_main_priority->getSize(cache_lock), my_main_priority->getSizeLimit(),
            my_main_priority->getElementsCount(cache_lock), my_main_priority->getElementsLimit(),
            file_segment.key(), file_segment.offset());

        return is_overflow;
    };

    if (is_main_priority_overflow())
    {
        main_priority->iterate(
            [&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
            { return is_main_priority_overflow() ? iterate_func(locked_key, segment_metadata) : PriorityIterationResult::BREAK; },
            cache_lock);

        if (is_main_priority_overflow())
            return false;
    }

    if (!file_segment.getKeyMetadata()->createBaseDirectory())
        return false;

    if (!to_delete.empty())
    {
        LOG_DEBUG(
            log, "Will evict {} file segments (while reserving {} bytes for {}:{})",
            to_delete.size(), size, file_segment.key(), file_segment.offset());

        ProfileEventTimeIncrement<Microseconds> evict_watch(ProfileEvents::FilesystemCacheEvictMicroseconds);

        for (auto & [current_key, deletion_info] : to_delete)
        {
            auto locked_key = deletion_info.key_metadata->tryLock();
            if (!locked_key)
                continue; /// key could become invalid after we released the key lock above, just skip it.

            /// delete from vector in reverse order just for efficiency
            auto & candidates = deletion_info.candidates;
            while (!candidates.empty())
            {
                auto & candidate = candidates.back();
                chassert(candidate->releasable());

                const auto * segment = candidate->file_segment.get();
                auto queue_it = segment->getQueueIterator();
                chassert(queue_it);

                ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedFileSegments);
                ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedBytes, segment->range().size());

                locked_key->removeFileSegment(segment->offset(), segment->lock());
                queue_it->remove(cache_lock);

                if (query_context)
                    query_context->remove(current_key, segment->offset(), cache_lock);

                candidates.pop_back();
            }
        }
    }

    /// A file_segment_metadata acquires a LRUQueue iterator on first successful space reservation attempt,
    /// e.g. queue_iteratir is std::nullopt here if no space has been reserved yet.
    auto queue_iterator = file_segment.getQueueIterator();
    chassert(!queue_iterator || file_segment.getReservedSize() > 0);

    if (queue_iterator)
    {
        queue_iterator->updateSize(size);
    }
    else
    {
        /// Space reservation is incremental, so file_segment_metadata is created first (with state empty),
        /// and getQueueIterator() is assigned on first space reservation attempt.
        queue_iterator = main_priority->add(file_segment.getKeyMetadata(), file_segment.offset(), size, cache_lock);
        file_segment.setQueueIterator(queue_iterator);
    }

    file_segment.reserved_size += size;
    chassert(file_segment.reserved_size == queue_iterator->getEntry().size);

    if (query_context)
    {
        auto query_queue_it = query_context->tryGet(file_segment.key(), file_segment.offset(), cache_lock);
        if (query_queue_it)
            query_queue_it->updateSize(size);
        else
            query_context->add(file_segment.getKeyMetadata(), file_segment.offset(), size, cache_lock);
    }

    if (main_priority->getSize(cache_lock) > (1ull << 63))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache became inconsistent. There must be a bug");

    return true;
}

void FileCache::removeKeyIfExists(const Key & key)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL);
    if (!locked_key)
        return;

    /// In ordinary case we remove data from cache when it's not used by anyone.
    /// But if we have multiple replicated zero-copy tables on the same server
    /// it became possible to start removing something from cache when it is used
    /// by other "zero-copy" tables. That is why it's not an error.
    locked_key->removeAllReleasable();
}

void FileCache::removePathIfExists(const String & path)
{
    removeKeyIfExists(createKeyForPath(path));
}

void FileCache::removeAllReleasable()
{
    assertInitialized();

    auto lock = lockCache();

    main_priority->iterate([&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
    {
        if (segment_metadata->releasable())
        {
            auto file_segment = segment_metadata->file_segment;
            locked_key.removeFileSegment(file_segment->offset(), file_segment->lock());
            return PriorityIterationResult::REMOVE_AND_CONTINUE;
        }
        return PriorityIterationResult::CONTINUE;
    }, lock);

    if (stash)
    {
        /// Remove all access information.
        stash->records.clear();
        stash->queue->removeAll(lock);
    }
}

void FileCache::loadMetadata()
{
    auto lock = lockCache();

    UInt64 offset = 0;
    size_t size = 0;
    std::vector<std::pair<PriorityIterator, std::weak_ptr<FileSegment>>> queue_entries;

    /// cache_base_path / key_prefix / key / offset
    if (!metadata.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cache initialization is partially made. "
            "This can be a result of a failed first attempt to initialize cache. "
            "Please, check log for error messages");
    }

    size_t total_size = 0;
    for (auto key_prefix_it = fs::directory_iterator{metadata.getBaseDirectory()};
         key_prefix_it != fs::directory_iterator();)
    {
        const fs::path key_prefix_directory = key_prefix_it->path();
        key_prefix_it++;

        if (!fs::is_directory(key_prefix_directory))
        {
            if (key_prefix_directory.filename() != "status")
            {
                LOG_WARNING(
                    log, "Unexpected file {} (not a directory), will skip it",
                    key_prefix_directory.string());
            }
            continue;
        }

        if (fs::is_empty(key_prefix_directory))
        {
            LOG_DEBUG(log, "Removing empty key prefix directory: {}", key_prefix_directory.string());
            fs::remove(key_prefix_directory);
            continue;
        }

        for (fs::directory_iterator key_it{key_prefix_directory}; key_it != fs::directory_iterator();)
        {
            const fs::path key_directory = key_it->path();
            ++key_it;

            if (!fs::is_directory(key_directory))
            {
                LOG_DEBUG(
                    log,
                    "Unexpected file: {} (not a directory). Expected a directory",
                    key_directory.string());
                continue;
            }

            if (fs::is_empty(key_directory))
            {
                LOG_DEBUG(log, "Removing empty key directory: {}", key_directory.string());
                fs::remove(key_directory);
                continue;
            }

            const auto key = Key(unhexUInt<UInt128>(key_directory.filename().string().data()));
            auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, /* is_initial_load */true);

            for (fs::directory_iterator offset_it{key_directory}; offset_it != fs::directory_iterator(); ++offset_it)
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
                    {
                        /// For compatibility. Persistent files are no longer supported.
                        fs::remove(offset_it->path());
                        continue;
                    }
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

                if ((main_priority->getSizeLimit() == 0 || main_priority->getSize(lock) + size <= main_priority->getSizeLimit())
                    && (main_priority->getElementsLimit() == 0 || main_priority->getElementsCount(lock) + 1 <= main_priority->getElementsLimit()))
                {
                    KeyMetadata::iterator file_segment_metadata_it;
                    try
                    {
                        file_segment_metadata_it = addFileSegment(
                            *locked_key, offset, size, FileSegment::State::DOWNLOADED, CreateFileSegmentSettings(segment_kind), &lock);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                        chassert(false);

                        fs::remove(offset_it->path());
                        continue;
                    }

                    const auto & file_segment_metadata = file_segment_metadata_it->second;
                    chassert(file_segment_metadata->file_segment->assertCorrectness());
                    total_size += size;

                    queue_entries.emplace_back(
                        file_segment_metadata->getQueueIterator(),
                        file_segment_metadata->file_segment);
                }
                else
                {
                    LOG_WARNING(
                        log,
                        "Cache capacity changed (max size: {}, used: {}), "
                        "cached file `{}` does not fit in cache anymore (size: {})",
                        main_priority->getSizeLimit(), main_priority->getSize(lock), key_directory.string(), size);

                    fs::remove(offset_it->path());
                }
            }
        }
    }

    chassert(total_size == main_priority->getSize(lock));
    chassert(total_size <= main_priority->getSizeLimit());

    /// Shuffle file_segment_metadatas to have random order in LRUQueue
    /// as at startup all file_segment_metadatas have the same priority.
    pcg64 generator(randomSeed());
    std::shuffle(queue_entries.begin(), queue_entries.end(), generator);
    for (auto & [it, file_segment] : queue_entries)
    {
        /// Cache size changed and, for example, 1st file segment fits into cache
        /// and 2nd file segment will fit only if first was evicted, then first will be removed and
        /// file_segment_metadata is nullptr here.
        if (file_segment.expired())
            continue;

        it->use(lock);
    }
}

FileCache::~FileCache()
{
    deactivateBackgroundOperations();
}

void FileCache::deactivateBackgroundOperations()
{
    if (cleanup_task)
        cleanup_task->deactivate();

    metadata.cancelDownload();
    for (auto & thread : download_threads)
        if (thread.joinable())
            thread.join();
}

void FileCache::cleanup()
{
    metadata.doCleanup();
}

void FileCache::cleanupThreadFunc()
{
    try
    {
        cleanup();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        chassert(false);
    }

    cleanup_task->scheduleAfter(delayed_cleanup_interval_ms);
}

FileSegmentsHolderPtr FileCache::getSnapshot()
{
    assertInitialized();
#ifndef NDEBUG
    assertCacheCorrectness();
#endif

    FileSegments file_segments;
    metadata.iterate([&](const LockedKey & locked_key)
    {
        for (const auto & [_, file_segment_metadata] : locked_key)
            file_segments.push_back(FileSegment::getSnapshot(file_segment_metadata->file_segment));
    });
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments), /* complete_on_dtor */false);
}

FileSegmentsHolderPtr FileCache::getSnapshot(const Key & key)
{
    FileSegments file_segments;
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW);
    for (const auto & [_, file_segment_metadata] : *locked_key->getKeyMetadata())
        file_segments.push_back(FileSegment::getSnapshot(file_segment_metadata->file_segment));
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::dumpQueue()
{
    assertInitialized();

    FileSegments file_segments;
    main_priority->iterate([&](LockedKey &, const FileSegmentMetadataPtr & segment_metadata)
    {
        file_segments.push_back(FileSegment::getSnapshot(segment_metadata->file_segment));
        return PriorityIterationResult::CONTINUE;
    }, lockCache());

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

std::vector<String> FileCache::tryGetCachePaths(const Key & key)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL);
    if (!locked_key)
        return {};

    std::vector<String> cache_paths;

    for (const auto & [offset, file_segment_metadata] : *locked_key->getKeyMetadata())
    {
        if (file_segment_metadata->file_segment->state() == FileSegment::State::DOWNLOADED)
            cache_paths.push_back(metadata.getPathForFileSegment(key, offset, file_segment_metadata->file_segment->getKind()));
    }
    return cache_paths;
}

size_t FileCache::getUsedCacheSize() const
{
    return main_priority->getSize(lockCache());
}

size_t FileCache::getFileSegmentsNum() const
{
    return main_priority->getElementsCount(lockCache());
}

void FileCache::assertCacheCorrectness()
{
    auto lock = lockCache();
    main_priority->iterate([&](LockedKey &, const FileSegmentMetadataPtr & segment_metadata)
    {
        const auto & file_segment = *segment_metadata->file_segment;
        UNUSED(file_segment);
        chassert(file_segment.assertCorrectness());
        return PriorityIterationResult::CONTINUE;
    }, lock);
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
        auto lock = cache->lockCache();
        cache->query_limit->removeQueryContext(query_id, lock);
    }
}

FileCache::QueryContextHolderPtr FileCache::getQueryContextHolder(
    const String & query_id, const ReadSettings & settings)
{
    if (!query_limit || settings.filesystem_cache_max_download_size == 0)
        return {};

    auto lock = lockCache();
    auto context = query_limit->getOrSetQueryContext(query_id, settings, lock);
    return std::make_unique<QueryContextHolder>(query_id, this, std::move(context));
}

}
