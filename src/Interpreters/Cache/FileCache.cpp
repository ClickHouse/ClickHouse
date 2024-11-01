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
#include <Common/callOnce.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Core/ServerUUID.h>

#include <exception>
#include <filesystem>
#include <mutex>


namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event FilesystemCacheLoadMetadataMicroseconds;
    extern const Event FilesystemCacheLockCacheMicroseconds;
    extern const Event FilesystemCacheReserveMicroseconds;
    extern const Event FilesystemCacheGetOrSetMicroseconds;
    extern const Event FilesystemCacheGetMicroseconds;
    extern const Event FilesystemCacheFailToReserveSpaceBecauseOfLockContention;
    extern const Event FilesystemCacheFreeSpaceKeepingThreadRun;
    extern const Event FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds;
    extern const Event FilesystemCacheFailToReserveSpaceBecauseOfCacheResize;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
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

    std::string getCommonUserID()
    {
        auto user_from_context = DB::Context::getGlobalContextInstance()->getFilesystemCacheUser();
        const auto user = user_from_context.empty() ? toString(ServerUUID::get()) : user_from_context;
        return user;
    }
}

void FileCacheReserveStat::update(size_t size, FileSegmentKind kind, bool releasable)
{
    auto & local_stat = stat_by_kind[kind];
    if (releasable)
    {
        total_stat.releasable_size += size;
        ++total_stat.releasable_count;

        local_stat.releasable_size += size;
        ++local_stat.releasable_count;
    }
    else
    {
        total_stat.non_releasable_size += size;
        ++total_stat.non_releasable_count;

        local_stat.non_releasable_size += size;
        ++local_stat.non_releasable_count;
    }
}

FileCache::FileCache(const std::string & cache_name, const FileCacheSettings & settings)
    : max_file_segment_size(settings.max_file_segment_size)
    , bypass_cache_threshold(settings.enable_bypass_cache_with_threshold ? settings.bypass_cache_threshold : 0)
    , boundary_alignment(settings.boundary_alignment)
    , load_metadata_threads(settings.load_metadata_threads)
    , load_metadata_asynchronously(settings.load_metadata_asynchronously)
    , write_cache_per_user_directory(settings.write_cache_per_user_id_directory)
    , keep_current_size_to_max_ratio(1 - settings.keep_free_space_size_ratio)
    , keep_current_elements_to_max_ratio(1 - settings.keep_free_space_elements_ratio)
    , keep_up_free_space_remove_batch(settings.keep_free_space_remove_batch)
    , log(getLogger("FileCache(" + cache_name + ")"))
    , metadata(settings.base_path, settings.background_download_queue_size_limit, settings.background_download_threads, write_cache_per_user_directory)
{
    if (settings.cache_policy == "LRU")
    {
        main_priority = std::make_unique<LRUFileCachePriority>(
            settings.max_size, settings.max_elements, nullptr, cache_name);
    }
    else if (settings.cache_policy == "SLRU")
    {
        main_priority = std::make_unique<SLRUFileCachePriority>(
            settings.max_size, settings.max_elements, settings.slru_size_ratio, nullptr, nullptr, cache_name);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown cache policy: {}", settings.cache_policy);

    LOG_DEBUG(log, "Using {} cache policy", settings.cache_policy);

    if (settings.cache_hits_threshold)
        stash = std::make_unique<HitsCountStash>(settings.cache_hits_threshold, settings.max_elements);

    if (settings.enable_filesystem_query_cache_limit)
        query_limit = std::make_unique<FileCacheQueryLimit>();
}

const FileCache::UserInfo & FileCache::getCommonUser()
{
    static UserInfo user(getCommonUserID(), 0);
    return user;
}

const FileCache::UserInfo & FileCache::getInternalUser()
{
    static UserInfo user("internal");
    return user;
}

bool FileCache::isInitialized() const
{
    return is_initialized;
}

void FileCache::throwInitExceptionIfNeeded()
{
    if (load_metadata_asynchronously)
        return;

    std::lock_guard lock(init_mutex);
    if (init_exception)
        std::rethrow_exception(init_exception);
}

const String & FileCache::getBasePath() const
{
    return metadata.getBaseDirectory();
}

String FileCache::getFileSegmentPath(const Key & key, size_t offset, FileSegmentKind segment_kind, const UserInfo & user) const
{
    return metadata.getFileSegmentPath(key, offset, segment_kind, user);
}

String FileCache::getKeyPath(const Key & key, const UserInfo & user) const
{
    return metadata.getKeyPath(key, user);
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
    // Prevent initialize() from running twice. This may be caused by two cache disks being created with the same path (see integration/test_filesystem_cache).
    callOnce(initialize_called, [&] {
        bool need_to_load_metadata = fs::exists(getBasePath());
        try
        {
            if (!need_to_load_metadata)
                fs::create_directories(getBasePath());
            status_file = make_unique<StatusFile>(fs::path(getBasePath()) / "status", StatusFile::write_full_info);
        }
        catch (...)
        {
            init_exception = std::current_exception();
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }

        if (load_metadata_asynchronously)
        {
            load_metadata_main_thread = ThreadFromGlobalPool([this, need_to_load_metadata] { initializeImpl(need_to_load_metadata); });
        }
        else
        {
            initializeImpl(need_to_load_metadata);
        }
    });
}

void FileCache::initializeImpl(bool load_metadata)
{
    std::lock_guard lock(init_mutex);

    if (is_initialized)
        return;

    try
    {
        if (load_metadata)
            loadMetadata();

        metadata.startup();
    }
    catch (...)
    {
        init_exception = std::current_exception();
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    if (keep_current_size_to_max_ratio != 1 || keep_current_elements_to_max_ratio != 1)
    {
        keep_up_free_space_ratio_task = Context::getGlobalContextInstance()->getSchedulePool().createTask(log->name(), [this] { freeSpaceRatioKeepingThreadFunc(); });
        keep_up_free_space_ratio_task->schedule();
    }

    is_initialized = true;
    LOG_TEST(log, "Initialized cache from {}", metadata.getBaseDirectory());
}

CachePriorityGuard::Lock FileCache::lockCache() const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockCacheMicroseconds);
    return cache_guard.lock();
}

CachePriorityGuard::Lock FileCache::tryLockCache(std::optional<std::chrono::milliseconds> acquire_timeout) const
{
    return acquire_timeout.has_value() ? cache_guard.tryLockFor(acquire_timeout.value()) : cache_guard.tryLock();
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
        if (!file_segment_metadata.isEvictingOrRemoved(locked_key))
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

std::vector<FileSegment::Range> FileCache::splitRange(size_t offset, size_t size, size_t aligned_size)
{
    chassert(size > 0);
    chassert(size <= aligned_size);

    /// Consider this example to understand why we need to account here for both `size` and `aligned_size`.
    /// [________________]__________________] <-- requested range
    ///                  ^                  ^
    ///                right offset         aligned_right_offset
    /// [_________]                           <-- last cached file segment, e.g. we have uncovered suffix of the requested range
    ///           ^
    ///           last_file_segment_right_offset
    /// [________________]
    ///        size
    /// [____________________________________]
    ///        aligned_size
    ///
    /// So it is possible that we split this hole range into sub-segments by `max_file_segment_size`
    /// and get something like this:
    ///
    /// [________________________]
    ///          ^               ^
    ///          |               last_file_segment_right_offset + max_file_segment_size
    ///          last_file_segment_right_offset
    /// e.g. there is no need to create sub-segment for range (last_file_segment_right_offset + max_file_segment_size, aligned_right_offset].
    /// Because its left offset would be bigger than right_offset.
    /// Therefore, we set end_pos_non_included as offset+size, but remaining_size as aligned_size.

    std::vector<FileSegment::Range> ranges;

    size_t current_pos = offset;
    size_t end_pos_non_included = offset + size;
    size_t remaining_size = aligned_size;

    FileSegments file_segments;
    const size_t max_size = max_file_segment_size.load();
    while (current_pos < end_pos_non_included)
    {
        auto current_file_segment_size = std::min(remaining_size, max_size);
        ranges.emplace_back(current_pos, current_pos + current_file_segment_size - 1);

        remaining_size -= current_file_segment_size;
        current_pos += current_file_segment_size;
    }

    return ranges;
}

FileSegments FileCache::createFileSegmentsFromRanges(
    LockedKey & locked_key,
    const std::vector<FileSegment::Range> & ranges,
    size_t & file_segments_count,
    size_t file_segments_limit,
    const CreateFileSegmentSettings & create_settings)
{
    FileSegments result;
    for (const auto & r : ranges)
    {
        if (file_segments_limit && file_segments_count >= file_segments_limit)
            break;
        auto metadata_it = addFileSegment(locked_key, r.left, r.size(), FileSegment::State::EMPTY, create_settings, nullptr);
        result.push_back(metadata_it->second->file_segment);
        ++file_segments_count;
    }
    return result;
}

void FileCache::fillHolesWithEmptyFileSegments(
    LockedKey & locked_key,
    FileSegments & file_segments,
    const FileSegment::Range & range,
    size_t non_aligned_right_offset,
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
            const auto ranges = splitRange(current_pos, hole_size, hole_size);
            auto hole_segments = createFileSegmentsFromRanges(locked_key, ranges, processed_count, file_segments_limit, create_settings);
            file_segments.splice(it, std::move(hole_segments));
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

    if (current_pos <= non_aligned_right_offset)
    {
        ///   ________]     -- requested range
        ///   _____]
        ///        ^
        /// segmentN

        auto hole_size = range.right - current_pos + 1;
        auto non_aligned_hole_size = non_aligned_right_offset - current_pos + 1;

        if (fill_with_detached_file_segments)
        {
            auto file_segment = std::make_shared<FileSegment>(
                locked_key.getKey(), current_pos, non_aligned_hole_size, FileSegment::State::DETACHED, create_settings);

            file_segments.insert(file_segments.end(), file_segment);
        }
        else
        {
            const auto ranges = splitRange(current_pos, non_aligned_hole_size, hole_size);
            auto hole_segments = createFileSegmentsFromRanges(locked_key, ranges, processed_count, file_segments_limit, create_settings);
            file_segments.splice(it, std::move(hole_segments));

            if (is_limit_reached())
                erase_unprocessed();
        }
    }
}

FileSegmentsHolderPtr FileCache::set(
    const Key & key,
    size_t offset,
    size_t size,
    const CreateFileSegmentSettings & create_settings,
    const UserInfo & user)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, user);
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
        const auto ranges = splitRange(offset, size, size);
        size_t file_segments_count = 0;
        file_segments = createFileSegmentsFromRanges(*locked_key, ranges, file_segments_count, /* file_segments_limit */0, create_settings);
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
    size_t file_segments_limit,
    const UserInfo & user)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);

    assertInitialized();

    FileSegment::Range initial_range(offset, offset + size - 1);
    /// result_range is initial range, which will be adjusted according to
    /// 1. aligned_offset, aligned_end_offset
    /// 2. max_file_segments_limit
    FileSegment::Range result_range = initial_range;

    const auto aligned_offset = roundDownToMultiple(initial_range.left, boundary_alignment);
    auto aligned_end_offset = std::min(roundUpToMultiple(initial_range.right + 1, boundary_alignment), file_size) - 1;

    chassert(aligned_offset <= initial_range.left);
    chassert(aligned_end_offset >= initial_range.right);

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, user);
    /// Get all segments which intersect with the given range.
    auto file_segments = getImpl(*locked_key, initial_range, file_segments_limit);

    if (file_segments_limit)
    {
        chassert(file_segments.size() <= file_segments_limit);
        if (file_segments.size() == file_segments_limit)
            result_range.right = aligned_end_offset = file_segments.back()->range().right;
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
    const bool has_uncovered_prefix = file_segments.empty() || result_range.left < file_segments.front()->range().left;

    if (aligned_offset < result_range.left && has_uncovered_prefix)
    {
        auto prefix_range = FileSegment::Range(aligned_offset, file_segments.empty() ? result_range.left - 1 : file_segments.front()->range().left - 1);
        auto prefix_file_segments = getImpl(*locked_key, prefix_range, /* file_segments_limit */0);

        if (prefix_file_segments.empty())
        {
            ///   [____________________][_______________]
            ///   ^                     ^               ^
            ///   aligned_offset        range.left      range.right
            ///                             [___] [__________]         <-- current cache (example)
            result_range.left = aligned_offset;
        }
        else
        {
            ///   [____________________][_______________]
            ///   ^                     ^               ^
            ///   aligned_offset        range.left          range.right
            ///   ____]     [____]           [___] [__________]        <-- current cache (example)
            ///                  ^
            ///                  prefix_file_segments.back().right

            chassert(prefix_file_segments.back()->range().right < result_range.left);
            chassert(prefix_file_segments.back()->range().right >= aligned_offset);

            result_range.left = prefix_file_segments.back()->range().right + 1;
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
    const bool has_uncovered_suffix = file_segments.empty() || file_segments.back()->range().right < result_range.right;

    if (result_range.right < aligned_end_offset && has_uncovered_suffix)
    {
        auto suffix_range = FileSegment::Range(result_range.right, aligned_end_offset);
        /// We need to get 1 file segment, so file_segments_limit = 1 here.
        auto suffix_file_segments = getImpl(*locked_key, suffix_range, /* file_segments_limit */1);

        if (suffix_file_segments.empty())
        {
            ///   [__________________][                       ]
            ///   ^                  ^                        ^
            ///   range.left         range.right              aligned_end_offset
            ///      [___]   [___]                                    <-- current cache (example)

            result_range.right = aligned_end_offset;
        }
        else
        {
            ///   [__________________][                       ]
            ///   ^                  ^                        ^
            ///   range.left         range.right              aligned_end_offset
            ///      [___]   [___]          [_________]               <-- current cache (example)
            ///                             ^
            ///                             suffix_file_segments.front().left
            result_range.right = suffix_file_segments.front()->range().left - 1;
        }
    }

    if (file_segments.empty())
    {
        auto ranges = splitRange(result_range.left, initial_range.size() + (initial_range.left - result_range.left), result_range.size());
        size_t file_segments_count = file_segments.size();
        file_segments.splice(file_segments.end(), createFileSegmentsFromRanges(*locked_key, ranges, file_segments_count, file_segments_limit, create_settings));
    }
    else
    {
        chassert(file_segments.front()->range().right >= result_range.left);
        chassert(file_segments.back()->range().left <= result_range.right);

        fillHolesWithEmptyFileSegments(
            *locked_key, file_segments, result_range, offset + size - 1, file_segments_limit, /* fill_with_detached */false, create_settings);

        if (!file_segments.front()->range().contains(result_range.left))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} to include {} "
                            "(end offset: {}, aligned offset: {}, aligned end offset: {})",
                            file_segments.front()->range().toString(), offset, result_range.right, aligned_offset, aligned_end_offset);
        }
    }

    chassert(file_segments_limit
             ? file_segments.back()->range().left <= result_range.right
             : file_segments.back()->range().contains(result_range.right),
             fmt::format("Unexpected state. Back: {}, result range: {}, limit: {}",
                         file_segments.back()->range().toString(), result_range.toString(), file_segments_limit));

    chassert(!file_segments_limit || file_segments.size() <= file_segments_limit);

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::get(
    const Key & key,
    size_t offset,
    size_t size,
    size_t file_segments_limit,
    const UserID & user_id)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheGetMicroseconds);

    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, UserInfo(user_id));
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
                *locked_key, file_segments, range, offset + size - 1, file_segments_limit, /* fill_with_detached */true, CreateFileSegmentSettings{});

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
    const CachePriorityGuard::Lock * lock)
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
                stash_key, stash->queue->add(locked_key.getKeyMetadata(), offset, 0, locked_key.getKeyMetadata()->user, *lock));

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

bool FileCache::tryReserve(
    FileSegment & file_segment,
    const size_t size,
    FileCacheReserveStat & reserve_stat,
    const UserInfo & user,
    size_t lock_wait_timeout_milliseconds,
    std::string & failure_reason)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheReserveMicroseconds);

    assertInitialized();

    /// A logical race on cache_is_being_resized is still possible,
    /// in this case we will try to lock cache with timeout, this is ok, timeout is small
    /// and as resizing of cache can take a long time then this small chance of a race is
    /// ok compared to the number of cases this check will help.
    if (cache_is_being_resized.load(std::memory_order_relaxed))
    {
        ProfileEvents::increment(ProfileEvents::FilesystemCacheFailToReserveSpaceBecauseOfCacheResize);
        failure_reason = "cache is being resized";
        return false;
    }

    auto cache_lock = tryLockCache(std::chrono::milliseconds(lock_wait_timeout_milliseconds));
    if (!cache_lock)
    {
        ProfileEvents::increment(ProfileEvents::FilesystemCacheFailToReserveSpaceBecauseOfLockContention);
        failure_reason = "cache contention";
        return false;
    }

    LOG_TEST(
        log, "Trying to reserve space ({} bytes) for {}:{}, current usage: {}",
        size, file_segment.key(), file_segment.offset(), main_priority->getStateInfoForLog(cache_lock));

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
            failure_reason = "query limit exceeded";
            return false;
        }

        LOG_TEST(
            log, "Using query limit, current usage: {}/{} (while reserving for {}:{})",
            query_priority->getSize(cache_lock), query_priority->getSizeLimit(cache_lock),
            file_segment.key(), file_segment.offset());
    }

    auto queue_iterator = file_segment.getQueueIterator();

    /// A file_segment_metadata acquires a priority iterator
    /// on first successful space reservation attempt,
    /// so queue_iterator == nullptr, if no space reservation took place yet.
    chassert(!queue_iterator || file_segment.getReservedSize() > 0);

    /// If it is the first space reservatiob attempt for a file segment
    /// we need to make space for 1 element in cache,
    /// otherwise space is already taken and we need 0 elements to free.
    size_t required_elements_num = queue_iterator ? 0 : 1;

    EvictionCandidates eviction_candidates;

    /// If user has configured fs cache limit per query,
    /// we take into account query limits here.
    if (query_priority)
    {
        if (!query_priority->collectCandidatesForEviction(
                size, required_elements_num, reserve_stat, eviction_candidates, {}, user.user_id, cache_lock))
        {
            failure_reason = "cannot evict enough space for query limit";
            return false;
        }

        LOG_TEST(log, "Query limits satisfied (while reserving for {}:{})",
                 file_segment.key(), file_segment.offset());

        /// If we have enough space in query_priority, we are not interested about stat there anymore.
        /// Clean the stat before iterating main_priority to avoid calculating any segment stat twice.
        reserve_stat.stat_by_kind.clear();
    }

    if (!main_priority->collectCandidatesForEviction(
            size, required_elements_num, reserve_stat, eviction_candidates, queue_iterator, user.user_id, cache_lock))
    {
        failure_reason = "cannot evict enough space";
        return false;
    }

    if (!file_segment.getKeyMetadata()->createBaseDirectory())
    {
        failure_reason = "not enough space on device";
        return false;
    }

    if (eviction_candidates.size() > 0)
    {
        cache_lock.unlock();
        try
        {
            /// Remove eviction candidates from filesystem.
            eviction_candidates.evict();
        }
        catch (...)
        {
            cache_lock.lock();
            /// Invalidate queue entries if some succeeded to be removed.
            eviction_candidates.finalize(query_context.get(), cache_lock);
            throw;
        }

        cache_lock.lock();

        /// Invalidate and remove queue entries and execute finalize func.
        eviction_candidates.finalize(query_context.get(), cache_lock);
    }
    else if (!main_priority->canFit(size, required_elements_num, cache_lock, queue_iterator))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot fit {} in cache, but collection of eviction candidates succeeded with no candidates. "
            "This is a bug. Queue entry type: {}. Cache info: {}",
            size, queue_iterator ? queue_iterator->getType() : FileCacheQueueEntryType::None,
            main_priority->getStateInfoForLog(cache_lock));
    }

    if (queue_iterator)
    {
        /// Increase size of queue entry.
        queue_iterator->incrementSize(size, cache_lock);
    }
    else
    {
        /// Create a new queue entry and assign currently reserved size to it.
        queue_iterator = main_priority->add(file_segment.getKeyMetadata(), file_segment.offset(), size, user, cache_lock);
        file_segment.setQueueIterator(queue_iterator);
    }

    main_priority->check(cache_lock);

    if (query_context)
    {
        auto query_queue_it = query_context->tryGet(file_segment.key(), file_segment.offset(), cache_lock);
        if (query_queue_it)
            query_queue_it->incrementSize(size, cache_lock);
        else
            query_context->add(file_segment.getKeyMetadata(), file_segment.offset(), size, user, cache_lock);
    }

    file_segment.reserved_size += size;
    chassert(file_segment.reserved_size == queue_iterator->getEntry()->size);

    if (main_priority->getSize(cache_lock) > (1ull << 63))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache became inconsistent. There must be a bug");

    return true;
}

void FileCache::freeSpaceRatioKeepingThreadFunc()
{
    static constexpr auto lock_failed_reschedule_ms = 1000;
    static constexpr auto space_ratio_satisfied_reschedule_ms = 5000;
    static constexpr auto general_reschedule_ms = 5000;

    if (shutdown)
        return;

    Stopwatch watch;

    auto lock = tryLockCache();

    /// To avoid deteriorating contention on cache,
    /// proceed only if cache is not heavily used.
    if (!lock)
    {
        keep_up_free_space_ratio_task->scheduleAfter(lock_failed_reschedule_ms);
        return;
    }

    const size_t size_limit = main_priority->getSizeLimit(lock);
    const size_t elements_limit = main_priority->getElementsLimit(lock);

    const size_t desired_size = std::lround(keep_current_size_to_max_ratio * size_limit);
    const size_t desired_elements_num = std::lround(keep_current_elements_to_max_ratio * elements_limit);

    if ((size_limit == 0 || main_priority->getSize(lock) <= desired_size)
        && (elements_limit == 0 || main_priority->getElementsCount(lock) <= desired_elements_num))
    {
        /// Nothing to free - all limits are satisfied.
        keep_up_free_space_ratio_task->scheduleAfter(space_ratio_satisfied_reschedule_ms);
        return;
    }

    ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadRun);

    FileCacheReserveStat stat;
    EvictionCandidates eviction_candidates;

    IFileCachePriority::CollectStatus desired_size_status;
    try
    {
        /// Collect at most `keep_up_free_space_remove_batch` elements to evict,
        /// (we use batches to make sure we do not block cache for too long,
        /// by default the batch size is quite small).
        desired_size_status = main_priority->collectCandidatesForEviction(
            desired_size, desired_elements_num, keep_up_free_space_remove_batch, stat, eviction_candidates, lock);

#ifdef DEBUG_OR_SANITIZER_BUILD
        /// Let's make sure that we correctly processed the limits.
        if (desired_size_status == IFileCachePriority::CollectStatus::SUCCESS
            && eviction_candidates.size() < keep_up_free_space_remove_batch)
        {
            const auto current_size = main_priority->getSize(lock);
            chassert(current_size >= stat.total_stat.releasable_size);
            chassert(!size_limit
                     || current_size - stat.total_stat.releasable_size <= desired_size);

            const auto current_elements_count = main_priority->getElementsCount(lock);
            chassert(current_elements_count >= stat.total_stat.releasable_count);
            chassert(!elements_limit
                     || current_elements_count - stat.total_stat.releasable_count <= desired_elements_num);
        }
#endif

        if (shutdown)
            return;

        if (eviction_candidates.size() > 0)
        {
            LOG_TRACE(log, "Current usage {}/{} in size, {}/{} in elements count "
                    "(trying to keep size ratio at {} and elements ratio at {}). "
                    "Collected {} eviction candidates, "
                    "skipped {} candidates while iterating",
                    main_priority->getSize(lock), size_limit,
                    main_priority->getElementsCount(lock), elements_limit,
                    desired_size, desired_elements_num,
                    eviction_candidates.size(), stat.total_stat.non_releasable_count);

            lock.unlock();

            /// Remove files from filesystem.
            eviction_candidates.evict();

            /// Take lock again to finalize eviction,
            /// e.g. to update the in-memory state.
            lock.lock();
            eviction_candidates.finalize(nullptr, lock);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        if (eviction_candidates.size() > 0)
            eviction_candidates.finalize(nullptr, lockCache());

        /// Let's catch such cases in ci,
        /// in general there should not be exceptions.
        chassert(false);
    }

    watch.stop();
    ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds, watch.elapsedMilliseconds());

    LOG_TRACE(log, "Free space ratio keeping thread finished in {} ms (status: {})",
              watch.elapsedMilliseconds(), desired_size_status);

    [[maybe_unused]] bool scheduled = false;
    switch (desired_size_status)
    {
        case IFileCachePriority::CollectStatus::SUCCESS: [[fallthrough]];
        case IFileCachePriority::CollectStatus::CANNOT_EVICT:
        {
            scheduled = keep_up_free_space_ratio_task->scheduleAfter(general_reschedule_ms);
            break;
        }
        case IFileCachePriority::CollectStatus::REACHED_MAX_CANDIDATES_LIMIT:
        {
            scheduled = keep_up_free_space_ratio_task->schedule();
            break;
        }
    }
    chassert(scheduled);
}

void FileCache::iterate(IterateFunc && func, const UserID & user_id)
{
    metadata.iterate([&](const LockedKey & locked_key)
    {
        for (const auto & file_segment_metadata : locked_key)
            func(FileSegment::getInfo(file_segment_metadata.second->file_segment));
    }, user_id);
}

void FileCache::removeKey(const Key & key, const UserID & user_id)
{
    assertInitialized();
    metadata.removeKey(key, /* if_exists */false, /* if_releasable */true, user_id);
}

void FileCache::removeKeyIfExists(const Key & key, const UserID & user_id)
{
    assertInitialized();
    metadata.removeKey(key, /* if_exists */true, /* if_releasable */true, user_id);
}

void FileCache::removeFileSegment(const Key & key, size_t offset, const UserID & user_id)
{
    assertInitialized();
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW, UserInfo(user_id));
    locked_key->removeFileSegment(offset);
}

void FileCache::removePathIfExists(const String & path, const UserID & user_id)
{
    removeKeyIfExists(Key::fromPath(path), user_id);
}

void FileCache::removeAllReleasable(const UserID & user_id)
{
    assertInitialized();

#ifdef DEBUG_OR_SANITIZER_BUILD
    assertCacheCorrectness();
#endif

    metadata.removeAllKeys(/* if_releasable */true, user_id);

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

    LOG_INFO(log, "Loading filesystem cache with {} threads from {}", load_metadata_threads, metadata.getBaseDirectory());

    for (size_t i = 0; i < load_metadata_threads; ++i)
    {
        try
        {
            loading_threads.emplace_back([&]
            {
                while (!stop_loading_metadata)
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
                        stop_loading_metadata = true;
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
            stop_loading_metadata = true;
            break;
        }
    }

    for (auto & thread : loading_threads)
        if (thread.joinable())
            thread.join();

    if (first_exception)
        std::rethrow_exception(first_exception);

#ifdef DEBUG_OR_SANITIZER_BUILD
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

    UserInfo user;
    if (write_cache_per_user_directory)
    {
        auto filename = keys_dir.filename().string();

        auto pos = filename.find_last_of('.');
        if (pos == std::string::npos)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected file format: {}", filename);

        user = UserInfo(filename.substr(0, pos), parse<UInt64>(filename.substr(pos + 1)));

        LOG_TEST(log, "Loading cache for user {}", user.user_id);
    }
    else
    {
        user = getCommonUser();
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
        auto key_metadata = metadata.getKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, user, /* is_initial_load */true);

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

                limits_satisfied = main_priority->canFit(size, 1, lock, nullptr, true);
                if (limits_satisfied)
                    cache_it = main_priority->add(key_metadata, offset, size, user, lock, /* best_effort */true);

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
            metadata.removeKey(key, false, false, getInternalUser().user_id);
        }
    }
}

FileCache::~FileCache()
{
    deactivateBackgroundOperations();
#ifdef DEBUG_OR_SANITIZER_BUILD
    assertCacheCorrectness();
#endif
}

void FileCache::deactivateBackgroundOperations()
{
    shutdown.store(true);

    stop_loading_metadata = true;
    if (load_metadata_main_thread.joinable())
        load_metadata_main_thread.join();

    metadata.shutdown();
    if (keep_up_free_space_ratio_task)
        keep_up_free_space_ratio_task->deactivate();
}

std::vector<FileSegment::Info> FileCache::getFileSegmentInfos(const UserID & user_id)
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
    }, user_id);
    return file_segments;
}

std::vector<FileSegment::Info> FileCache::getFileSegmentInfos(const Key & key, const UserID & user_id)
{
    std::vector<FileSegment::Info> file_segments;
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW_LOGICAL, UserInfo(user_id));
    for (const auto & [_, file_segment_metadata] : *locked_key)
        file_segments.push_back(FileSegment::getInfo(file_segment_metadata->file_segment));
    return file_segments;
}

IFileCachePriority::PriorityDumpPtr FileCache::dumpQueue()
{
    assertInitialized();
    return main_priority->dump(lockCache());
}

std::vector<String> FileCache::tryGetCachePaths(const Key & key)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, getInternalUser());
    if (!locked_key)
        return {};

    std::vector<String> cache_paths;

    for (const auto & [offset, file_segment_metadata] : *locked_key)
    {
        const auto & file_segment = *file_segment_metadata->file_segment;
        if (file_segment.state() == FileSegment::State::DOWNLOADED)
            cache_paths.push_back(locked_key->getKeyMetadata()->getFileSegmentPath(file_segment));
    }
    return cache_paths;
}

size_t FileCache::getUsedCacheSize() const
{
    /// We use this method for metrics, so it is ok to get approximate result.
    return main_priority->getSizeApprox();
}

size_t FileCache::getFileSegmentsNum() const
{
    /// We use this method for metrics, so it is ok to get approximate result.
    return main_priority->getElementsCountApprox();
}

void FileCache::assertCacheCorrectness()
{
    metadata.iterate([&](LockedKey & locked_key)
    {
        for (const auto & [_, file_segment_metadata] : locked_key)
        {
            chassert(file_segment_metadata->file_segment->assertCorrectness());
        }
    }, getInternalUser().user_id);
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
        EvictionCandidates eviction_candidates;
        bool modified_size_limit = false;

        /// In order to not block cache for the duration of cache resize,
        /// we do:
        /// a. Take a cache lock.
        ///     1. Collect eviction candidates,
        ///     2. Remove queue entries of eviction candidates.
        ///        This will release space we consider to be hold for them,
        ///        so that we can safely modify size limits.
        ///     3. Modify size limits of cache.
        /// b. Release a cache lock.
        ///     1. Do actual eviction from filesystem.
        {
            cache_is_being_resized.store(true, std::memory_order_relaxed);
            SCOPE_EXIT({
                cache_is_being_resized.store(false, std::memory_order_relaxed);
            });

            auto cache_lock = lockCache();

            FileCacheReserveStat stat;
            if (main_priority->collectCandidatesForEviction(
                    new_settings.max_size, new_settings.max_elements, 0/* max_candidates_to_evict */,
                    stat, eviction_candidates, cache_lock) == IFileCachePriority::CollectStatus::SUCCESS)
            {
                if (eviction_candidates.size() == 0)
                {
                    main_priority->modifySizeLimits(
                        new_settings.max_size, new_settings.max_elements,
                        new_settings.slru_size_ratio, cache_lock);
                }
                else
                {
                    /// Remove only queue entries of eviction candidates.
                    eviction_candidates.removeQueueEntries(cache_lock);
                    /// Note that (in-memory) metadata about corresponding file segments
                    /// (e.g. file segment info in CacheMetadata) will be removed
                    /// only after eviction from filesystem. This is needed to avoid
                    /// a race on removal of file from filesystsem and
                    /// addition of the same file as part of a newly cached file segment.

                    /// Modify cache size limits.
                    /// From this point cache eviction will follow them.
                    main_priority->modifySizeLimits(
                        new_settings.max_size, new_settings.max_elements,
                        new_settings.slru_size_ratio, cache_lock);

                    cache_lock.unlock();

                    SCOPE_EXIT({
                        try
                        {
                            if (eviction_candidates.needFinalize())
                            {
                                cache_lock.lock();
                                eviction_candidates.finalize(nullptr, cache_lock);
                            }
                        }
                        catch (...)
                        {
                            tryLogCurrentException(__PRETTY_FUNCTION__);
                            chassert(false);
                        }
                    });

                    /// Do actual eviction from filesystem.
                    eviction_candidates.evict();
                }

                modified_size_limit = true;
            }
        }

        if (modified_size_limit)
        {
            LOG_INFO(log, "Changed max_size from {} to {}, max_elements from {} to {}",
                    actual_settings.max_size, new_settings.max_size,
                    actual_settings.max_elements, new_settings.max_elements);

            chassert(main_priority->getSizeApprox() <= new_settings.max_size);
            chassert(main_priority->getElementsCountApprox() <= new_settings.max_elements);

            actual_settings.max_size = new_settings.max_size;
            actual_settings.max_elements = new_settings.max_elements;
        }
        else
        {
            LOG_WARNING(
                log, "Unable to modify size limit from {} to {}, elements limit from {} to {}. "
                "`max_size` and `max_elements` settings will remain inconsistent with config.xml. "
                "Next attempt to update them will happen on the next config reload. "
                "You can trigger it with SYSTEM RELOAD CONFIG.",
                actual_settings.max_size, new_settings.max_size,
                actual_settings.max_elements, new_settings.max_elements);
        }
    }

    if (new_settings.max_file_segment_size != actual_settings.max_file_segment_size)
    {
        max_file_segment_size = actual_settings.max_file_segment_size = new_settings.max_file_segment_size;
    }
}

FileCache::QueryContextHolderPtr FileCache::getQueryContextHolder(
    const String & query_id, const ReadSettings & read_settings)
{
    if (!query_limit || read_settings.filesystem_cache_max_download_size == 0)
        return {};

    auto lock = lockCache();
    auto context = query_limit->getOrSetQueryContext(query_id, read_settings, lock);
    return std::make_unique<QueryContextHolder>(query_id, this, query_limit.get(), std::move(context));
}

std::vector<FileSegment::Info> FileCache::sync()
{
    std::vector<FileSegment::Info> file_segments;
    metadata.iterate([&](LockedKey & locked_key)
    {
        auto broken = locked_key.sync();
        file_segments.insert(file_segments.end(), broken.begin(), broken.end());
    }, getInternalUser().user_id);
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
