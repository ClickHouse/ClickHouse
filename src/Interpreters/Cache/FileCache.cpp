#include "FileCache.h"

#include <Common/randomSeed.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <pcg-random/pcg_random.hpp>
#include <base/hex.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FileCache::FileCache(const FileCacheSettings & settings)
    : max_file_segment_size(settings.max_file_segment_size)
    , allow_persistent_files(settings.do_not_evict_index_and_mark_files)
    , bypass_cache_threshold(settings.enable_bypass_cache_with_threashold ? settings.bypass_cache_threashold : 0)
    , delayed_cleanup_interval_ms(settings.delayed_cleanup_interval_ms)
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
    return metadata.getPathInLocalCache(key, offset, segment_kind);
}

String FileCache::getPathInLocalCache(const Key & key) const
{
    return metadata.getPathInLocalCache(key);
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

    cleanup_task = Context::getGlobalContextInstance()->getSchedulePool().createTask("FileCacheCleanup", [this]{ cleanupThreadFunc(); });
    cleanup_task->activate();
    cleanup_task->scheduleAfter(delayed_cleanup_interval_ms);
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
        if (file_segment_metadata.valid())
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

FileSegmentsHolderPtr FileCache::getOrSet(
    const Key & key,
    size_t offset,
    size_t size,
    const CreateFileSegmentSettings & settings)
{
    assertInitialized();

    FileSegment::Range range(offset, offset + size - 1);

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY);

    /// Get all segments which intersect with the given range.
    auto file_segments = getImpl(*locked_key, range);
    if (file_segments.empty())
    {
        file_segments = splitRangeIntoFileSegments(
            *locked_key, offset, size, FileSegment::State::EMPTY, settings);
    }
    else
    {
        fillHolesWithEmptyFileSegments(
            *locked_key, file_segments, range, /* fill_with_detached */false, settings);
    }

    chassert(!file_segments.empty());
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::get(const Key & key, size_t offset, size_t size)
{
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
    if (locked_key.tryGetByOffset(offset))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cache entry already exists for key: `{}`, offset: {}, size: {}.",
            key, offset, size);
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

bool FileCache::tryReserve(FileSegment & file_segment, size_t size)
{
    assertInitialized();
    auto cache_lock = cache_guard.lock();

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
            return false;
    }

    size_t queue_size = main_priority->getElementsCount(cache_lock);
    chassert(queue_size <= main_priority->getElementsLimit());

    /// A file_segment_metadata acquires a LRUQueue iterator on first successful space reservation attempt.
    auto queue_iterator = file_segment.getQueueIterator();
    if (queue_iterator)
        chassert(file_segment.getReservedSize() > 0);
    else
        queue_size += 1;

    size_t removed_size = 0;

    class EvictionCandidates final : public std::vector<FileSegmentMetadataPtr>
    {
    public:
        explicit EvictionCandidates(KeyMetadataPtr key_metadata_) : key_metadata(key_metadata_) {}

        KeyMetadata & getMetadata() { return *key_metadata; }

        void add(FileSegmentMetadataPtr candidate)
        {
            candidate->removal_candidate = true;
            push_back(candidate);
        }

        ~EvictionCandidates()
        {
            for (const auto & candidate : *this)
                candidate->removal_candidate = false;
        }

    private:
        KeyMetadataPtr key_metadata;
    };

    std::unordered_map<Key, EvictionCandidates> to_delete;

    auto iterate_func = [&](LockedKey & locked_key, FileSegmentMetadataPtr segment_metadata)
    {
        chassert(segment_metadata->file_segment->assertCorrectness());

        const bool is_persistent = allow_persistent_files && segment_metadata->file_segment->isPersistent();
        const bool releasable = segment_metadata->releasable() && !is_persistent;

        if (releasable)
        {
            removed_size += segment_metadata->size();
            --queue_size;

            auto segment = segment_metadata->file_segment;
            if (segment->state() == FileSegment::State::DOWNLOADED)
            {
                const auto & key = segment->key();
                auto it = to_delete.find(key);
                if (it == to_delete.end())
                    it = to_delete.emplace(key, locked_key.getKeyMetadata()).first;
                it->second.add(segment_metadata);
                return PriorityIterationResult::CONTINUE;
            }

            /// TODO: we can resize if partially downloaded instead.
            locked_key.removeFileSegment(segment->offset(), segment->lock());
            return PriorityIterationResult::REMOVE_AND_CONTINUE;
        }
        return PriorityIterationResult::CONTINUE;
    };

    if (query_priority)
    {
        auto is_query_priority_overflow = [&]
        {
            const size_t new_size = query_priority->getSize(cache_lock) + size - removed_size;
            return new_size > query_priority->getSizeLimit();
        };

        query_priority->iterate(
            [&](LockedKey & locked_key, FileSegmentMetadataPtr segment_metadata)
            { return is_query_priority_overflow() ? iterate_func(locked_key, segment_metadata) : PriorityIterationResult::BREAK; },
            cache_lock);

        if (is_query_priority_overflow())
            return false;
    }

    auto is_main_priority_overflow = [&]
    {
        /// max_size == 0 means unlimited cache size,
        /// max_element_size means unlimited number of cache elements.
        return (main_priority->getSizeLimit() != 0 && main_priority->getSize(cache_lock) + size - removed_size > main_priority->getSizeLimit())
            || (main_priority->getElementsLimit() != 0 && queue_size > main_priority->getElementsLimit());
    };

    main_priority->iterate(
        [&](LockedKey & locked_key, FileSegmentMetadataPtr segment_metadata)
        { return is_main_priority_overflow() ? iterate_func(locked_key, segment_metadata) : PriorityIterationResult::BREAK; },
        cache_lock);

    if (is_main_priority_overflow())
        return false;

    if (!file_segment.getKeyMetadata()->createBaseDirectory())
        return false;

    for (auto & [current_key, deletion_info] : to_delete)
    {
        auto locked_key = deletion_info.getMetadata().tryLock();
        if (!locked_key)
            continue; /// key could become invalid after we released the key lock above, just skip it.

        for (auto it = deletion_info.begin(); it != deletion_info.end();)
        {
            chassert((*it)->releasable());

            auto segment = (*it)->file_segment;
            locked_key->removeFileSegment(segment->offset(), segment->lock());
            segment->getQueueIterator()->remove(cache_lock);

            if (query_context)
                query_context->remove(current_key, segment->offset(), cache_lock);

            it = deletion_info.erase(it);
        }
    }

    /// queue_iteratir is std::nullopt here if no space has been reserved yet, a file_segment_metadata
    /// acquires queue iterator on first successful space reservation attempt.
    /// If queue iterator already exists, we need to update the size after each space reservation.
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

void FileCache::removeAllReleasable()
{
    assertInitialized();

    /// Only releasable file segments are evicted.
    /// `remove_persistent_files` defines whether non-evictable by some criteria files
    /// (they do not comply with the cache eviction policy) should also be removed.

    auto lock = cache_guard.lock();

    main_priority->iterate([&](LockedKey & locked_key, FileSegmentMetadataPtr segment_metadata)
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
    auto lock = cache_guard.lock();

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
                        segment_kind = FileSegmentKind::Persistent;
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
                    auto file_segment_metadata_it = addFileSegment(
                        *locked_key, offset, size, FileSegment::State::DOWNLOADED, CreateFileSegmentSettings(segment_kind), &lock);

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
}

void FileCache::cleanup()
{
    metadata.doCleanup();
}

void FileCache::cleanupThreadFunc()
{
#ifndef NDEBUG
    assertCacheCorrectness();
#endif

    try
    {
        cleanup();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
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
    main_priority->iterate([&](LockedKey &, FileSegmentMetadataPtr segment_metadata)
    {
        file_segments.push_back(FileSegment::getSnapshot(segment_metadata->file_segment));
        return PriorityIterationResult::CONTINUE;
    }, cache_guard.lock());

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
            cache_paths.push_back(metadata.getPathInLocalCache(key, offset, file_segment_metadata->file_segment->getKind()));
    }
    return cache_paths;
}

size_t FileCache::getUsedCacheSize() const
{
    return main_priority->getSize(cache_guard.lock());
}

size_t FileCache::getFileSegmentsNum() const
{
    return main_priority->getElementsCount(cache_guard.lock());
}

void FileCache::assertCacheCorrectness()
{
    auto lock = cache_guard.lock();
    main_priority->iterate([&](LockedKey &, FileSegmentMetadataPtr segment_metadata)
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
