#include "FileCache.h"

#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/SLRUFileCachePriority.h>
#include <Interpreters/Cache/EvictionCandidates.h>
#include <Interpreters/Context.h>
#include <base/hex.h>
#include <Common/ThreadPool.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event FilesystemCacheLoadMetadataMicroseconds;
    extern const Event FilesystemCacheLockCacheMicroseconds;
    extern const Event FilesystemCacheReserveMicroseconds;
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
    extern const int BAD_ARGUMENTS;
}

void FileCacheReserveStat::update(size_t size, FileSegmentKind kind, bool releasable)
{
    auto & local_stat = stat_by_kind[kind];
    if (releasable)
    {
        stat.releasable_size += size;
        ++stat.releasable_count;

        local_stat.releasable_size += size;
        ++local_stat.releasable_count;
    }
    else
    {
        stat.non_releasable_size += size;
        ++stat.non_releasable_count;

        local_stat.non_releasable_size += size;
        ++local_stat.non_releasable_count;
    }
}

FileCache::FileCache(const std::string & cache_name, const FileCacheSettings & settings)
    : max_file_segment_size(settings.max_file_segment_size)
    , bypass_cache_threshold(settings.enable_bypass_cache_with_threshold ? settings.bypass_cache_threshold : 0)
    , boundary_alignment(settings.boundary_alignment)
    , load_metadata_threads(settings.load_metadata_threads)
    , log(&Poco::Logger::get("FileCache(" + cache_name + ")"))
    , metadata(settings.base_path, settings.background_download_queue_size_limit, settings.background_download_threads)
{
    if (settings.cache_policy == "LRU")
        main_priority = std::make_unique<LRUFileCachePriority>(settings.max_size, settings.max_elements);
    else if (settings.cache_policy == "SLRU")
        main_priority = std::make_unique<SLRUFileCachePriority>(settings.max_size, settings.max_elements, settings.slru_size_ratio);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown cache policy: {}", settings.cache_policy);

    LOG_DEBUG(log, "Using {} cache policy", settings.cache_policy);

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

        status_file = make_unique<StatusFile>(fs::path(getBasePath()) / "status", StatusFile::write_full_info);
    }
    catch (...)
    {
        init_exception = std::current_exception();
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    metadata.startup();
    is_initialized = true;
}

CacheGuard::Lock FileCache::lockCache() const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockCacheMicroseconds);
    return cache_guard.lock();
}

FileSegments FileCache::getImpl(const LockedKey & locked_key, const FileSegment::Range & range, size_t file_segments_limit) const
{
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    if (bypass_cache_threshold && range.size() > bypass_cache_threshold)
    {
        auto file_segment = std::make_shared<FileSegment>(
            locked_key.getKey(), range.left, range.size(), FileSegment::State::DETACHED);
        return { file_segment };
    }

    if (locked_key.empty())
        return {};

    FileSegments result;
    auto add_to_result = [&](const FileSegmentMetadata & file_segment_metadata)
    {
        if (file_segments_limit && result.size() == file_segments_limit)
            return false;

        FileSegmentPtr file_segment;
        if (!file_segment_metadata.evicting())
        {
            file_segment = file_segment_metadata.file_segment;
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
        return true;
    };

    const auto & file_segments = locked_key;
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

        if (!add_to_result(file_segment_metadata))
            return result;
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
                if (!add_to_result(prev_file_segment_metadata))
                    return result;
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

            if (!add_to_result(file_segment_metadata))
                return result;

            ++segment_it;
        }
    }

    return result;
}

std::vector<FileSegment::Range> FileCache::splitRange(size_t offset, size_t size)
{
    assert(size > 0);
    std::vector<FileSegment::Range> ranges;

    size_t current_pos = offset;
    size_t end_pos_non_included = offset + size;
    size_t remaining_size = size;

    FileSegments file_segments;
    while (current_pos < end_pos_non_included)
    {
        auto current_file_segment_size = std::min(remaining_size, max_file_segment_size);
        ranges.emplace_back(current_pos, current_pos + current_file_segment_size - 1);

        remaining_size -= current_file_segment_size;
        current_pos += current_file_segment_size;
    }

    return ranges;
}

FileSegments FileCache::splitRangeIntoFileSegments(
    LockedKey & locked_key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    size_t file_segments_limit,
    const CreateFileSegmentSettings & create_settings)
{
    assert(size > 0);

    auto current_pos = offset;
    auto end_pos_non_included = offset + size;

    size_t current_file_segment_size;
    size_t remaining_size = size;

    FileSegments file_segments;
    while (current_pos < end_pos_non_included && (!file_segments_limit || file_segments.size() < file_segments_limit))
    {
        current_file_segment_size = std::min(remaining_size, max_file_segment_size);
        remaining_size -= current_file_segment_size;

        auto file_segment_metadata_it = addFileSegment(
            locked_key, current_pos, current_file_segment_size, state, create_settings, nullptr);
        file_segments.push_back(file_segment_metadata_it->second->file_segment);

        current_pos += current_file_segment_size;
    }

    return file_segments;
}

void FileCache::fillHolesWithEmptyFileSegments(
    LockedKey & locked_key,
    FileSegments & file_segments,
    const FileSegment::Range & range,
    size_t file_segments_limit,
    bool fill_with_detached_file_segments,
    const CreateFileSegmentSettings & create_settings)
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
    size_t processed_count = 0;
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
        ++processed_count;
    }
    else
        current_pos = range.left;

    auto is_limit_reached = [&]() -> bool
    {
        return file_segments_limit && processed_count >= file_segments_limit;
    };

    while (current_pos <= range.right && it != file_segments.end() && !is_limit_reached())
    {
        segment_range = (*it)->range();

        if (current_pos == segment_range.left)
        {
            current_pos = segment_range.right + 1;
            ++it;
            ++processed_count;
            continue;
        }

        assert(current_pos < segment_range.left);

        auto hole_size = segment_range.left - current_pos;

        if (fill_with_detached_file_segments)
        {
            auto file_segment = std::make_shared<FileSegment>(
                locked_key.getKey(), current_pos, hole_size, FileSegment::State::DETACHED, create_settings);

            file_segments.insert(it, file_segment);
            ++processed_count;
        }
        else
        {
            auto ranges = splitRange(current_pos, hole_size);
            FileSegments hole;
            for (const auto & r : ranges)
            {
                auto metadata_it = addFileSegment(locked_key, r.left, r.size(), FileSegment::State::EMPTY, create_settings, nullptr);
                hole.push_back(metadata_it->second->file_segment);
                ++processed_count;

                if (is_limit_reached())
                    break;
            }
            file_segments.splice(it, std::move(hole));
        }

        if (is_limit_reached())
            break;

        current_pos = segment_range.right + 1;
        ++it;
        ++processed_count;
    }

    auto erase_unprocessed = [&]()
    {
        chassert(file_segments.size() >= file_segments_limit);
        file_segments.erase(it, file_segments.end());
        chassert(file_segments.size() == file_segments_limit);
    };

    if (is_limit_reached())
    {
        erase_unprocessed();
        return;
    }

    chassert(!file_segments_limit || file_segments.size() < file_segments_limit);

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
                locked_key.getKey(), current_pos, hole_size, FileSegment::State::DETACHED, create_settings);

            file_segments.insert(file_segments.end(), file_segment);
        }
        else
        {
            auto ranges = splitRange(current_pos, hole_size);
            FileSegments hole;
            for (const auto & r : ranges)
            {
                auto metadata_it = addFileSegment(locked_key, r.left, r.size(), FileSegment::State::EMPTY, create_settings, nullptr);
                hole.push_back(metadata_it->second->file_segment);
                ++processed_count;

                if (is_limit_reached())
                    break;
            }
            file_segments.splice(it, std::move(hole));

            if (is_limit_reached())
                erase_unprocessed();
        }
    }
}

FileSegmentsHolderPtr FileCache::set(
    const Key & key,
    size_t offset,
    size_t size,
    const CreateFileSegmentSettings & create_settings)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY);
    FileSegment::Range range(offset, offset + size - 1);

    auto file_segments = getImpl(*locked_key, range, /* file_segments_limit */0);
    if (!file_segments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Having intersection with already existing cache");

    if (create_settings.unbounded)
    {
        /// If the file is unbounded, we can create a single file_segment_metadata for it.
        auto file_segment_metadata_it = addFileSegment(
            *locked_key, offset, size, FileSegment::State::EMPTY, create_settings, nullptr);
        file_segments = {file_segment_metadata_it->second->file_segment};
    }
    else
    {
        file_segments = splitRangeIntoFileSegments(
            *locked_key, offset, size, FileSegment::State::EMPTY, /* file_segments_limit */0, create_settings);
    }

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr
FileCache::getOrSet(
    const Key & key,
    size_t offset,
    size_t size,
    size_t file_size,
    const CreateFileSegmentSettings & create_settings,
    size_t file_segments_limit)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);

    assertInitialized();

    FileSegment::Range range(offset, offset + size - 1);

    const auto aligned_offset = roundDownToMultiple(range.left, boundary_alignment);
    auto aligned_end_offset = std::min(roundUpToMultiple(offset + size, boundary_alignment), file_size) - 1;

    chassert(aligned_offset <= range.left);
    chassert(aligned_end_offset >= range.right);

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY);
    /// Get all segments which intersect with the given range.
    auto file_segments = getImpl(*locked_key, range, file_segments_limit);

    if (file_segments_limit)
    {
        chassert(file_segments.size() <= file_segments_limit);
        if (file_segments.size() == file_segments_limit)
            range.right = aligned_end_offset = file_segments.back()->range().right;
    }

    /// Check case if we have uncovered prefix, e.g.
    ///
    ///   [_______________]
    ///   ^               ^
    ///   range.left      range.right
    ///         [___] [__________]        <-- current cache (example)
    ///   [    ]
    ///   ^----^
    ///   uncovered prefix.
    const bool has_uncovered_prefix = file_segments.empty() || range.left < file_segments.front()->range().left;

    if (aligned_offset < range.left && has_uncovered_prefix)
    {
        auto prefix_range = FileSegment::Range(aligned_offset, file_segments.empty() ? range.left - 1 : file_segments.front()->range().left - 1);
        auto prefix_file_segments = getImpl(*locked_key, prefix_range, /* file_segments_limit */0);

        if (prefix_file_segments.empty())
        {
            ///   [____________________][_______________]
            ///   ^                     ^               ^
            ///   aligned_offset        range.left      range.right
            ///                             [___] [__________]         <-- current cache (example)
            range.left = aligned_offset;
        }
        else
        {
            ///   [____________________][_______________]
            ///   ^                     ^               ^
            ///   aligned_offset        range.left          range.right
            ///   ____]     [____]           [___] [__________]        <-- current cache (example)
            ///                  ^
            ///                  prefix_file_segments.back().right

            chassert(prefix_file_segments.back()->range().right < range.left);
            chassert(prefix_file_segments.back()->range().right >= aligned_offset);

            range.left = prefix_file_segments.back()->range().right + 1;
        }
    }

    /// Check case if we have uncovered suffix.
    ///
    ///   [___________________]
    ///   ^                   ^
    ///   range.left          range.right
    ///      [___]   [___]                  <-- current cache (example)
    ///                   [___]
    ///                   ^---^
    ///                    uncovered_suffix
    const bool has_uncovered_suffix = file_segments.empty() || file_segments.back()->range().right < range.right;

    if (range.right < aligned_end_offset && has_uncovered_suffix)
    {
        auto suffix_range = FileSegment::Range(range.right, aligned_end_offset);
        /// We need to get 1 file segment, so file_segments_limit = 1 here.
        auto suffix_file_segments = getImpl(*locked_key, suffix_range, /* file_segments_limit */1);

        if (suffix_file_segments.empty())
        {
            ///   [__________________][                       ]
            ///   ^                  ^                        ^
            ///   range.left         range.right              aligned_end_offset
            ///      [___]   [___]                                    <-- current cache (example)

            range.right = aligned_end_offset;
        }
        else
        {
            ///   [__________________][                       ]
            ///   ^                  ^                        ^
            ///   range.left         range.right              aligned_end_offset
            ///      [___]   [___]          [_________]               <-- current cache (example)
            ///                             ^
            ///                             suffix_file_segments.front().left
            range.right = suffix_file_segments.front()->range().left - 1;
        }
    }

    if (file_segments.empty())
    {
        file_segments = splitRangeIntoFileSegments(*locked_key, range.left, range.size(), FileSegment::State::EMPTY, file_segments_limit, create_settings);
    }
    else
    {
        chassert(file_segments.front()->range().right >= range.left);
        chassert(file_segments.back()->range().left <= range.right);

        fillHolesWithEmptyFileSegments(
            *locked_key, file_segments, range, file_segments_limit, /* fill_with_detached */false, create_settings);

        if (!file_segments.front()->range().contains(offset))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} to include {} "
                            "(end offset: {}, aligned offset: {}, aligned end offset: {})",
                            file_segments.front()->range().toString(), offset, range.right, aligned_offset, aligned_end_offset);
        }
    }

    chassert(file_segments_limit ? file_segments.back()->range().left <= range.right : file_segments.back()->range().contains(range.right));
    chassert(!file_segments_limit || file_segments.size() <= file_segments_limit);

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::get(const Key & key, size_t offset, size_t size, size_t file_segments_limit)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheGetMicroseconds);

    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL);
    if (locked_key)
    {
        FileSegment::Range range(offset, offset + size - 1);

        /// Get all segments which intersect with the given range.
        auto file_segments = getImpl(*locked_key, range, file_segments_limit);
        if (!file_segments.empty())
        {
            if (file_segments_limit)
            {
                chassert(file_segments.size() <= file_segments_limit);
                if (file_segments.size() == file_segments_limit)
                    range.right = file_segments.back()->range().right;
            }

            fillHolesWithEmptyFileSegments(
                *locked_key, file_segments, range, file_segments_limit, /* fill_with_detached */true, CreateFileSegmentSettings{});

            chassert(!file_segments_limit || file_segments.size() <= file_segments_limit);
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
    const CreateFileSegmentSettings & create_settings,
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

            if (stash->queue->getElementsCount(*lock) > stash->queue->getElementsLimit(*lock))
                stash->queue->pop(*lock);

            result_state = FileSegment::State::DETACHED;
        }
        else
        {
            result_state = record_it->second->increasePriority(*lock) >= stash->hits_threshold
                ? FileSegment::State::EMPTY
                : FileSegment::State::DETACHED;
        }
    }
    else
    {
        result_state = state;
    }

    auto file_segment = std::make_shared<FileSegment>(key, offset, size, result_state, create_settings, metadata.isBackgroundDownloadEnabled(), this, locked_key.getKeyMetadata());
    auto file_segment_metadata = std::make_shared<FileSegmentMetadata>(std::move(file_segment));

    auto [file_segment_metadata_it, inserted] = locked_key.emplace(offset, file_segment_metadata);
    if (!inserted)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Failed to insert {}:{}: entry already exists", key, offset);
    }

    return file_segment_metadata_it;
}

bool FileCache::tryReserve(FileSegment & file_segment, const size_t size, FileCacheReserveStat & reserve_stat)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheReserveMicroseconds);

    assertInitialized();
    auto cache_lock = lockCache();

    LOG_TEST(
        log, "Trying to reserve space ({} bytes) for {}:{}, current usage {}/{}",
        size, file_segment.key(), file_segment.offset(),
        main_priority->getSize(cache_lock), main_priority->getSizeLimit(cache_lock));

    /// In case of per query cache limit (by default disabled), we add/remove entries from both
    /// (main_priority and query_priority) priority queues, but iterate entries in order of query_priority,
    /// while checking the limits in both.
    Priority * query_priority = nullptr;

    auto query_context = query_limit ? query_limit->tryGetQueryContext(cache_lock) : nullptr;
    if (query_context)
    {
        query_priority = &query_context->getPriority();

        const bool query_limit_exceeded = query_priority->getSize(cache_lock) + size > query_priority->getSizeLimit(cache_lock);
        if (query_limit_exceeded && !query_context->recacheOnFileCacheQueryLimitExceeded())
        {
            LOG_TEST(log, "Query limit exceeded, space reservation failed, "
                     "recache_on_query_limit_exceeded is disabled (while reserving for {}:{})",
                     file_segment.key(), file_segment.offset());
            return false;
        }

        LOG_TEST(
            log, "Using query limit, current usage: {}/{} (while reserving for {}:{})",
            query_priority->getSize(cache_lock), query_priority->getSizeLimit(cache_lock),
            file_segment.key(), file_segment.offset());
    }

    EvictionCandidates eviction_candidates;
    IFileCachePriority::FinalizeEvictionFunc finalize_eviction_func;

    if (query_priority)
    {
        if (!query_priority->collectCandidatesForEviction(size, reserve_stat, eviction_candidates, {}, finalize_eviction_func, cache_lock))
            return false;

        LOG_TEST(log, "Query limits satisfied (while reserving for {}:{})", file_segment.key(), file_segment.offset());
        /// If we have enough space in query_priority, we are not interested about stat there anymore.
        /// Clean the stat before iterating main_priority to avoid calculating any segment stat twice.
        reserve_stat.stat_by_kind.clear();
    }

    /// A file_segment_metadata acquires a priority iterator on first successful space reservation attempt,
    auto queue_iterator = file_segment.getQueueIterator();
    chassert(!queue_iterator || file_segment.getReservedSize() > 0);

    if (!main_priority->collectCandidatesForEviction(size, reserve_stat, eviction_candidates, queue_iterator, finalize_eviction_func, cache_lock))
        return false;

    if (!file_segment.getKeyMetadata()->createBaseDirectory())
        return false;

    eviction_candidates.evict(query_context.get(), cache_lock);

    if (finalize_eviction_func)
        finalize_eviction_func(cache_lock);

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
    chassert(file_segment.reserved_size == queue_iterator->getEntry()->size);

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

void FileCache::iterate(IterateFunc && func)
{
    return metadata.iterate([&](const LockedKey & locked_key)
    {
        for (const auto & file_segment_metadata : locked_key)
            func(FileSegment::getInfo(file_segment_metadata.second->file_segment));
    });
}

void FileCache::removeKey(const Key & key)
{
    assertInitialized();
    metadata.removeKey(key, /* if_exists */false, /* if_releasable */true);
}

void FileCache::removeKeyIfExists(const Key & key)
{
    assertInitialized();
    metadata.removeKey(key, /* if_exists */true, /* if_releasable */true);
}

void FileCache::removeFileSegment(const Key & key, size_t offset)
{
    assertInitialized();
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW);
    locked_key->removeFileSegment(offset);
}

void FileCache::removePathIfExists(const String & path)
{
    removeKeyIfExists(createKeyForPath(path));
}

void FileCache::removeAllReleasable()
{
    assertInitialized();

#ifdef ABORT_ON_LOGICAL_ERROR
    assertCacheCorrectness();
#endif

    metadata.removeAllKeys(/* if_releasable */true);

    if (stash)
    {
        /// Remove all access information.
        auto lock = lockCache();
        stash->clear();
    }
}

void FileCache::loadMetadata()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLoadMetadataMicroseconds);

    if (!metadata.isEmpty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cache initialization is partially made. "
            "This can be a result of a failed first attempt to initialize cache. "
            "Please, check log for error messages");
    }

    loadMetadataImpl();

    /// Shuffle file_segment_metadatas to have random order in LRUQueue
    /// as at startup all file_segment_metadatas have the same priority.
    main_priority->shuffle(lockCache());
}

void FileCache::loadMetadataImpl()
{
    auto get_keys_dir_to_process = [
        &, key_prefix_it = fs::directory_iterator{metadata.getBaseDirectory()}, get_key_mutex = std::mutex()]
        () mutable -> std::optional<fs::path>
    {
        std::lock_guard lk(get_key_mutex);
        while (true)
        {
            if (key_prefix_it == fs::directory_iterator())
                return std::nullopt;

            auto path = key_prefix_it->path();
            if (key_prefix_it->is_directory())
            {
                key_prefix_it++;
                return path;
            }

            if (key_prefix_it->path().filename() != "status")
            {
                LOG_WARNING(log, "Unexpected file {} (not a directory), will skip it", path.string());
            }
            key_prefix_it++;
        }
    };

    std::vector<ThreadFromGlobalPool> loading_threads;
    std::exception_ptr first_exception;
    std::mutex set_exception_mutex;
    std::atomic<bool> stop_loading = false;

    LOG_INFO(log, "Loading filesystem cache with {} threads", load_metadata_threads);

    for (size_t i = 0; i < load_metadata_threads; ++i)
    {
        try
        {
            loading_threads.emplace_back([&]
            {
                while (!stop_loading)
                {
                    try
                    {
                        auto path = get_keys_dir_to_process();
                        if (!path.has_value())
                            return;

                        loadMetadataForKeys(path.value());
                    }
                    catch (...)
                    {
                        {
                            std::lock_guard exception_lock(set_exception_mutex);
                            if (!first_exception)
                                first_exception = std::current_exception();
                        }
                        stop_loading = true;
                        return;
                    }
                }
            });
        }
        catch (...)
        {
            {
                std::lock_guard exception_lock(set_exception_mutex);
                if (!first_exception)
                    first_exception = std::current_exception();
            }
            stop_loading = true;
            break;
        }
    }

    for (auto & thread : loading_threads)
        if (thread.joinable())
            thread.join();

    if (first_exception)
        std::rethrow_exception(first_exception);

#ifdef ABORT_ON_LOGICAL_ERROR
    assertCacheCorrectness();
#endif
}

void FileCache::loadMetadataForKeys(const fs::path & keys_dir)
{
    fs::directory_iterator key_it{keys_dir};
    if (key_it == fs::directory_iterator{})
    {
        LOG_DEBUG(log, "Removing empty key prefix directory: {}", keys_dir.string());
        fs::remove(keys_dir);
        return;
    }

    UInt64 offset = 0, size = 0;
    for (; key_it != fs::directory_iterator(); key_it++)
    {
        const fs::path key_directory = key_it->path();

        if (!key_it->is_directory())
        {
            LOG_DEBUG(
                log,
                "Unexpected file: {} (not a directory). Expected a directory",
                key_directory.string());
            continue;
        }

        if (fs::directory_iterator{key_directory} == fs::directory_iterator{})
        {
            LOG_DEBUG(log, "Removing empty key directory: {}", key_directory.string());
            fs::remove(key_directory);
            continue;
        }

        const auto key = Key::fromKeyString(key_directory.filename().string());
        auto key_metadata = metadata.getKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, /* is_initial_load */true);

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

            bool limits_satisfied;
            IFileCachePriority::IteratorPtr cache_it;
            size_t size_limit = 0;

            {
                auto lock = lockCache();
                size_limit = main_priority->getSizeLimit(lock);

                limits_satisfied = main_priority->canFit(size, lock);
                if (limits_satisfied)
                    cache_it = main_priority->add(key_metadata, offset, size, lock, /* is_startup */true);

                /// TODO: we can get rid of this lockCache() if we first load everything in parallel
                /// without any mutual lock between loading threads, and only after do removeOverflow().
                /// This will be better because overflow here may
                /// happen only if cache configuration changed and max_size because less than it was.
            }

            if (limits_satisfied)
            {
                bool inserted = false;
                try
                {
                    auto file_segment = std::make_shared<FileSegment>(key, offset, size,
                                                                      FileSegment::State::DOWNLOADED,
                                                                      CreateFileSegmentSettings(segment_kind),
                                                                      false,
                                                                      this,
                                                                      key_metadata,
                                                                      cache_it);

                    inserted = key_metadata->emplaceUnlocked(offset, std::make_shared<FileSegmentMetadata>(std::move(file_segment))).second;
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                    chassert(false);
                }

                if (inserted)
                {
                    LOG_TEST(log, "Added file segment {}:{} (size: {}) with path: {}", key, offset, size, offset_it->path().string());
                }
                else
                {
                    cache_it->remove(lockCache());
                    fs::remove(offset_it->path());
                    chassert(false);
                }
            }
            else
            {
                LOG_WARNING(
                    log,
                    "Cache capacity changed (max size: {}), "
                    "cached file `{}` does not fit in cache anymore (size: {})",
                    size_limit, offset_it->path().string(), size);

                fs::remove(offset_it->path());
            }
        }

        if (key_metadata->sizeUnlocked() == 0)
        {
            metadata.removeKey(key, false, false);
        }
    }
}

FileCache::~FileCache()
{
    deactivateBackgroundOperations();
#ifdef ABORT_ON_LOGICAL_ERROR
    assertCacheCorrectness();
#endif
}

void FileCache::deactivateBackgroundOperations()
{
    shutdown.store(true);
    metadata.shutdown();
}

std::vector<FileSegment::Info> FileCache::getFileSegmentInfos()
{
    assertInitialized();
#ifndef NDEBUG
    assertCacheCorrectness();
#endif

    std::vector<FileSegment::Info> file_segments;
    metadata.iterate([&](const LockedKey & locked_key)
    {
        for (const auto & [_, file_segment_metadata] : locked_key)
            file_segments.push_back(FileSegment::getInfo(file_segment_metadata->file_segment));
    });
    return file_segments;
}

std::vector<FileSegment::Info> FileCache::getFileSegmentInfos(const Key & key)
{
    std::vector<FileSegment::Info> file_segments;
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW_LOGICAL);
    for (const auto & [_, file_segment_metadata] : *locked_key)
        file_segments.push_back(FileSegment::getInfo(file_segment_metadata->file_segment));
    return file_segments;
}

std::vector<FileSegment::Info> FileCache::dumpQueue()
{
    assertInitialized();
    return main_priority->dump(lockCache());
}

std::vector<String> FileCache::tryGetCachePaths(const Key & key)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL);
    if (!locked_key)
        return {};

    std::vector<String> cache_paths;

    for (const auto & [offset, file_segment_metadata] : *locked_key)
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
    metadata.iterate([&](LockedKey & locked_key)
    {
        for (const auto & [_, file_segment_metadata] : locked_key)
        {
            chassert(file_segment_metadata->file_segment->assertCorrectness());
        }
    });
}

void FileCache::applySettingsIfPossible(const FileCacheSettings & new_settings, FileCacheSettings & actual_settings)
{
    if (!is_initialized || shutdown || new_settings == actual_settings)
        return;

    std::lock_guard lock(apply_settings_mutex);

    if (new_settings.background_download_queue_size_limit != actual_settings.background_download_queue_size_limit
        && metadata.setBackgroundDownloadQueueSizeLimit(new_settings.background_download_queue_size_limit))
    {
        LOG_INFO(log, "Changed background_download_queue_size from {} to {}",
                 actual_settings.background_download_queue_size_limit,
                 new_settings.background_download_queue_size_limit);

        actual_settings.background_download_queue_size_limit = new_settings.background_download_queue_size_limit;
    }

    if (new_settings.background_download_threads != actual_settings.background_download_threads)
    {
        bool updated = false;
        try
        {
            updated = metadata.setBackgroundDownloadThreads(new_settings.background_download_threads);
        }
        catch (...)
        {
            actual_settings.background_download_threads = metadata.getBackgroundDownloadThreads();
            throw;
        }

        if (updated)
        {
            LOG_INFO(log, "Changed background_download_threads from {} to {}",
                    actual_settings.background_download_threads,
                    new_settings.background_download_threads);

            actual_settings.background_download_threads = new_settings.background_download_threads;
        }
    }


    if (new_settings.max_size != actual_settings.max_size
        || new_settings.max_elements != actual_settings.max_elements)
    {
        auto cache_lock = lockCache();

        bool updated = false;
        try
        {
            updated = main_priority->modifySizeLimits(
                new_settings.max_size, new_settings.max_elements, new_settings.slru_size_ratio, cache_lock);
        }
        catch (...)
        {
            actual_settings.max_size = main_priority->getSizeLimit(cache_lock);
            actual_settings.max_elements = main_priority->getElementsLimit(cache_lock);
            throw;
        }

        if (updated)
        {
            LOG_INFO(log, "Changed max_size from {} to {}, max_elements from {} to {}",
                    actual_settings.max_size, new_settings.max_size,
                    actual_settings.max_elements, new_settings.max_elements);

            actual_settings.max_size = main_priority->getSizeLimit(cache_lock);
            actual_settings.max_elements = main_priority->getElementsLimit(cache_lock);
        }
    }
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
    const String & query_id, const ReadSettings & read_settings)
{
    if (!query_limit || read_settings.filesystem_cache_max_download_size == 0)
        return {};

    auto lock = lockCache();
    auto context = query_limit->getOrSetQueryContext(query_id, read_settings, lock);
    return std::make_unique<QueryContextHolder>(query_id, this, std::move(context));
}

std::vector<FileSegment::Info> FileCache::sync()
{
    std::vector<FileSegment::Info> file_segments;
    metadata.iterate([&](LockedKey & locked_key)
    {
        auto broken = locked_key.sync();
        file_segments.insert(file_segments.end(), broken.begin(), broken.end());
    });
    return file_segments;
}

FileCache::HitsCountStash::HitsCountStash(size_t hits_threashold_, size_t queue_size_)
    : hits_threshold(hits_threashold_), queue_size(queue_size_), queue(std::make_unique<LRUFileCachePriority>(0, queue_size_))
{
    if (!queue_size_)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Queue size for hits queue must be non-zero");
}

void FileCache::HitsCountStash::clear()
{
    records.clear();
    queue = std::make_unique<LRUFileCachePriority>(0, queue_size);
}

}
