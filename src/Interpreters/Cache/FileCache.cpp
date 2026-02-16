#include <Interpreters/Cache/FileCache.h>

#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/SLRUFileCachePriority.h>
#include <Interpreters/Cache/FileCacheUtils.h>
#include <Interpreters/Cache/EvictionCandidates.h>
#include <Interpreters/Context.h>
#include <base/hex.h>
#include <Common/callOnce.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/FailPoint.h>
#include <Common/randomSeed.h>
#include <Core/ServerUUID.h>
#include <Core/BackgroundSchedulePool.h>
#if ENABLE_DISTRIBUTED_CACHE
#include <Interpreters/Cache/OvercommitFileCachePriority.h>
#endif

#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>
#include <Interpreters/Cache/FileSegmentInfo.h>


namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event FilesystemCacheLoadMetadataMicroseconds;
    extern const Event FilesystemCacheReserveMicroseconds;
    extern const Event FilesystemCacheReserveAttempts;
    extern const Event FilesystemCacheGetOrSetMicroseconds;
    extern const Event FilesystemCacheGetMicroseconds;
    extern const Event FilesystemCacheFreeSpaceKeepingThreadRun;
    extern const Event FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds;
    extern const Event FilesystemCacheFailToReserveSpaceBecauseOfCacheResize;
    extern const Event FilesystemCacheBackgroundEvictedFileSegments;
    extern const Event FilesystemCacheBackgroundEvictedBytes;
    extern const Event FilesystemCacheCheckCorrectness;
    extern const Event FilesystemCacheCheckCorrectnessMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheDownloadQueueElements;
    extern const Metric FilesystemCacheReserveThreads;
    extern const Metric FilesystemCacheSizeLimit;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace FailPoints
{
    extern const char file_cache_stall_free_space_ratio_keeping_thread[];
}

namespace FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsUInt64 max_file_segment_size;
    extern const FileCacheSettingsUInt64 boundary_alignment;
    extern const FileCacheSettingsFileCachePolicy cache_policy;
    extern const FileCacheSettingsDouble slru_size_ratio;
    extern const FileCacheSettingsDouble check_cache_probability;
    extern const FileCacheSettingsUInt64 load_metadata_threads;
    extern const FileCacheSettingsBool load_metadata_asynchronously;
    extern const FileCacheSettingsUInt64 background_download_threads;
    extern const FileCacheSettingsUInt64 background_download_queue_size_limit;
    extern const FileCacheSettingsUInt64 background_download_max_file_segment_size;
    extern const FileCacheSettingsDouble keep_free_space_size_ratio;
    extern const FileCacheSettingsDouble keep_free_space_elements_ratio;
    extern const FileCacheSettingsUInt64 keep_free_space_remove_batch;
    extern const FileCacheSettingsBool enable_bypass_cache_with_threshold;
    extern const FileCacheSettingsUInt64 bypass_cache_threshold;
    extern const FileCacheSettingsBool write_cache_per_user_id_directory;
    extern const FileCacheSettingsUInt64 cache_hits_threshold;
    extern const FileCacheSettingsBool enable_filesystem_query_cache_limit;
    extern const FileCacheSettingsBool allow_dynamic_cache_resize;
    extern const FileCacheSettingsBool use_split_cache;
    extern const FileCacheSettingsDouble split_cache_ratio;
    extern const FileCacheSettingsUInt64 overcommit_eviction_evict_step;
}

namespace
{
    std::string getCommonUserID()
    {
        auto user_from_context = DB::Context::getGlobalContextInstance()->getFilesystemCacheUser();
        const auto user = user_from_context.empty() ? toString(ServerUUID::get()) : user_from_context;
        return user;
    }
}

void FileCacheReserveStat::update(size_t size, FileSegmentKind kind, State state)
{
    auto & local_stat = stat_by_kind[kind];
    switch (state)
    {
        case State::Releasable:
        {
            total_stat.releasable_size += size;
            ++total_stat.releasable_count;

            local_stat.releasable_size += size;
            ++local_stat.releasable_count;
            break;
        }
        case State::NonReleasable:
        {
            total_stat.non_releasable_size += size;
            ++total_stat.non_releasable_count;

            local_stat.non_releasable_size += size;
            ++local_stat.non_releasable_count;
            break;
        }
        case State::Evicting:
        {
            ++total_stat.evicting_count;
            ++local_stat.evicting_count;
            break;
        }
        case State::Moving:
        {
            ++total_stat.moving_count;
            ++local_stat.moving_count;
            break;
        }
        case State::Invalidated:
        {
            ++total_stat.invalidated_count;
            ++local_stat.invalidated_count;
            break;
        }
    }
}

std::string FileCacheReserveStat::Stat::toString() const
{
    WriteBufferFromOwnString wb;
    wb << "releasable size: " << releasable_size << ", ";
    wb << "releasable count: " << releasable_count << ", ";
    wb << "non-releasable size: " << non_releasable_size << ", ";
    wb << "non-releasable count: " << non_releasable_count << ", ";
    wb << "evicting count: " << evicting_count << ", ";
    wb << "moving count: " << moving_count << ", ";
    wb << "invalidated count: " << invalidated_count;
    return wb.str();
}

double normalizeProbability(double probability)
{
    if (probability < 0.0)
        probability = .0;
    else if (probability > 1.0)
        probability = 1.0;
    return probability;
}

FileCache::CheckCacheProbability::CheckCacheProbability(double probability, UInt64 seed)
    : rndgen(seed == 0 ? randomSeed() : seed)
    , distribution(normalizeProbability(probability))
{
}

bool FileCache::CheckCacheProbability::doCheck()
{
    std::lock_guard lock(mutex);
    return distribution(rndgen);
}

FileCache::FileCache(const std::string & cache_name, const FileCacheSettings & settings)
    : max_file_segment_size(settings[FileCacheSetting::max_file_segment_size])
    , bypass_cache_threshold(settings[FileCacheSetting::enable_bypass_cache_with_threshold] ? settings[FileCacheSetting::bypass_cache_threshold] : 0)
    , boundary_alignment(settings[FileCacheSetting::boundary_alignment])
    , background_download_max_file_segment_size(settings[FileCacheSetting::background_download_max_file_segment_size])
    , load_metadata_threads(settings[FileCacheSetting::load_metadata_threads])
    , load_metadata_asynchronously(settings[FileCacheSetting::load_metadata_asynchronously])
    , write_cache_per_user_directory(settings[FileCacheSetting::write_cache_per_user_id_directory])
    , allow_dynamic_cache_resize(settings[FileCacheSetting::allow_dynamic_cache_resize])
    , keep_current_size_to_max_ratio(1 - settings[FileCacheSetting::keep_free_space_size_ratio])
    , keep_current_elements_to_max_ratio(1 - settings[FileCacheSetting::keep_free_space_elements_ratio])
    , keep_up_free_space_remove_batch(settings[FileCacheSetting::keep_free_space_remove_batch])
    , use_split_cache(settings[FileCacheSetting::use_split_cache])
    , split_cache_ratio(settings[FileCacheSetting::split_cache_ratio])
    , name(cache_name)
    , log(getLogger("FileCache(" + cache_name + ")"))
    , metadata(settings[FileCacheSetting::path],
               settings[FileCacheSetting::background_download_queue_size_limit],
               settings[FileCacheSetting::background_download_threads],
               write_cache_per_user_directory)
    , check_cache_probability(settings[FileCacheSetting::check_cache_probability])
{
    CachePriorityCreatorFunction creator_function;
    switch (settings[FileCacheSetting::cache_policy].value)
    {
        case FileCachePolicy::LRU:
        {
            creator_function = [](size_t max_size, size_t max_elements, size_t /*size_ratio*/, size_t /*overcommit_eviction_evict_step*/, String description) -> IFileCachePriorityPtr
            {
                return std::make_unique<LRUFileCachePriority>(
                    max_size,
                    max_elements,
                    description);
            };
            break;
        }
        case FileCachePolicy::SLRU:
        {
            creator_function = [](size_t max_size, size_t max_elements, double size_ratio, size_t /*overcommit_eviction_evict_step*/, String description) -> IFileCachePriorityPtr
            {
                return std::make_unique<SLRUFileCachePriority>(
                    max_size,
                    max_elements,
                    size_ratio,
                    description);
            };
            break;
        }
#if ENABLE_DISTRIBUTED_CACHE
        case FileCachePolicy::LRU_OVERCOMMIT:
        {
            creator_function = [](size_t max_size, size_t max_elements, double /*size_ratio*/, size_t overcommit_eviction_evict_step, String /*description*/) -> IFileCachePriorityPtr
            {
                return std::make_unique<OvercommitFileCachePriority<LRUFileCachePriority>>(
                    overcommit_eviction_evict_steps,
                    max_size,
                    max_elements,
                    "overcommit");
            };
            break;
        }
        case FileCachePolicy::SLRU_OVERCOMMIT:
        {
            creator_function = [](size_t max_size, size_t max_elements, double size_ratio, size_t overcommit_eviction_evict_step, String /*description*/) -> IFileCachePriorityPtr
            {
                return std::make_unique<OvercommitFileCachePriority<SLRUFileCachePriority>>(
                    overcommit_eviction_evict_step,
                    max_size,
                    max_elements,
                    size_ratio,
                    "overcommit");
            };
            break;
        }
#endif
    }
    if (use_split_cache)
    {
        main_priority = std::make_unique<SplitFileCachePriority>(
            creator_function,
            settings[FileCacheSetting::max_size],
            settings[FileCacheSetting::max_elements],
            settings[FileCacheSetting::slru_size_ratio],
            settings[FileCacheSetting::split_cache_ratio],
            cache_name
        );
    }
    else
    {
        main_priority = creator_function(
            settings[FileCacheSetting::max_size],
            settings[FileCacheSetting::max_elements],
            settings[FileCacheSetting::slru_size_ratio],
            settings[FileCacheSetting::overcommit_eviction_evict_step],
            cache_name
        );
    }
    LOG_DEBUG(log, "Using {} cache policy", settings[FileCacheSetting::cache_policy].value);

    if (settings[FileCacheSetting::enable_filesystem_query_cache_limit])
        query_limit = std::make_unique<FileCacheQueryLimit>();

    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSizeLimit, settings[FileCacheSetting::max_size]);
}

const FileCache::OriginInfo & FileCache::getCommonOrigin()
{
    static OriginInfo origin(getCommonUserID(), 0, FileSegmentKeyType::General);
    return origin;
}

FileCache::OriginInfo FileCache::getCommonOriginWithSegmentKeyType(const fs::path & filename) const
{
    auto origin = FileCache::getCommonOrigin();
    if (!use_split_cache)
        return origin;

    const static std::set<std::string> system_cache_type = {".txt", ".json", ".idx", ".cidx", ".dat"};
    origin.segment_type = system_cache_type.contains(fs::path(filename).extension()) ? FileSegmentKeyType::System : FileSegmentKeyType::Data;
    return origin;
}

const FileCache::OriginInfo & FileCache::getInternalOrigin()
{
    static OriginInfo origin("internal");
    return origin;
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

String FileCache::getFileSegmentPath(const Key & key, size_t offset, FileSegmentKind segment_kind, const OriginInfo & origin) const
{
    return metadata.getFileSegmentPath(key, offset, segment_kind, origin);
}

String FileCache::getKeyPath(const Key & key, const OriginInfo & origin) const
{
    return metadata.getKeyPath(key, origin);
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

            auto fs_info = std::filesystem::space(getBasePath());
            const size_t size_limit = main_priority->getSizeLimit(cache_state_guard.lock());
            if (fs_info.capacity < size_limit)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "The total capacity of the disk containing cache path {} is less than the specified max_size {} bytes",
                                getBasePath(), std::to_string(size_limit));

            status_file = make_unique<StatusFile>(fs::path(getBasePath()) / "status", StatusFile::write_full_info);
        }
        catch (const std::filesystem::filesystem_error & e)
        {
            init_exception = std::current_exception();
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to retrieve filesystem information for cache path {}. Error: {}",
                            getBasePath(), e.what());
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
        keep_up_free_space_ratio_task = Context::getGlobalContextInstance()->getSchedulePool().createTask(StorageID::createEmpty(), log->name(), [this] { freeSpaceRatioKeepingThreadFunc(); });
        keep_up_free_space_ratio_task->schedule();
    }

    is_initialized = true;
    LOG_TEST(log, "Initialized cache from {}", metadata.getBaseDirectory());
}

CachePriorityGuard::WriteLock FileCache::lockCache() const
{
    return cache_guard.writeLock();
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
        if (file_segment_metadata.isEvictingOrRemoved(locked_key))
        {
            file_segment = std::make_shared<FileSegment>(
                locked_key.getKey(),
                file_segment_metadata.file_segment->offset(),
                file_segment_metadata.file_segment->range().size(),
                FileSegment::State::DETACHED);
        }
        else
        {
            file_segment = file_segment_metadata.file_segment;
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
        auto metadata_it = addFileSegment(locked_key, r.left, r.size(), FileSegment::State::EMPTY, create_settings);
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

FileSegmentsHolderPtr FileCache::trySet(
    const Key & key,
    size_t offset,
    size_t size,
    const CreateFileSegmentSettings & create_settings,
    const OriginInfo & origin)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin);
    FileSegment::Range range(offset, offset + size - 1);

    auto file_segments = getImpl(*locked_key, range, /* file_segments_limit */0);
    if (!file_segments.empty())
        return nullptr;

    if (create_settings.unbounded)
    {
        /// If the file is unbounded, we can create a single file_segment_metadata for it.
        auto file_segment_metadata_it = addFileSegment(
            *locked_key, offset, size, FileSegment::State::EMPTY, create_settings);
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

FileSegmentsHolderPtr FileCache::set(
    const Key & key,
    size_t offset,
    size_t size,
    const CreateFileSegmentSettings & create_settings,
    const OriginInfo & origin)
{
    if (auto holder = trySet(key, offset, size, create_settings, origin))
        return holder;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Having intersection with already existing cache");
}

FileSegmentsHolderPtr
FileCache::getOrSet(
    const Key & key,
    size_t offset,
    size_t size,
    size_t file_size,
    const CreateFileSegmentSettings & create_settings,
    size_t file_segments_limit,
    const OriginInfo & origin_info,
    std::optional<size_t> boundary_alignment_)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);

    assertInitialized();

    size_t initial_range_right_offset = (file_size ? std::min(offset + size, file_size) : offset + size) - 1;
    FileSegment::Range initial_range(offset, initial_range_right_offset);
    /// result_range is initial range, which will be adjusted according to
    /// 1. aligned_offset, aligned_end_offset
    /// 2. max_file_segments_limit
    FileSegment::Range result_range = initial_range;

    const size_t alignment = boundary_alignment_.value_or(boundary_alignment);
    const auto aligned_offset = FileCacheUtils::roundDownToMultiple(initial_range.left, alignment);
    auto aligned_end_offset = (file_size
        ? std::min(FileCacheUtils::roundUpToMultiple(initial_range.right + 1, alignment), file_size)
        : FileCacheUtils::roundUpToMultiple(initial_range.right + 1, alignment)) - 1;

    chassert(aligned_offset <= initial_range.left);
    chassert(aligned_end_offset >= initial_range.right);

    auto locked_key = metadata.lockKeyMetadata(
        key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin_info);

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
        auto prefix_range = FileSegment::Range(
            aligned_offset,
            file_segments.empty() ? result_range.left - 1 : file_segments.front()->range().left - 1);

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
        file_segments.splice(
            file_segments.end(),
            createFileSegmentsFromRanges(*locked_key, ranges, file_segments_count, file_segments_limit, create_settings));
    }
    else
    {
        chassert(file_segments.front()->range().right >= result_range.left);
        chassert(file_segments.back()->range().left <= result_range.right);

        fillHolesWithEmptyFileSegments(
            *locked_key, file_segments, result_range, offset + size - 1, file_segments_limit, /* fill_with_detached */false, create_settings);

        if (!file_segments.front()->range().contains(result_range.left))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Expected {} to include {} "
                "(end offset: {}, aligned offset: {}, aligned end offset: {})",
                file_segments.front()->range().toString(), offset,
                result_range.right, aligned_offset, aligned_end_offset);
        }
    }

    /// Compare with initial_range and not result_range,
    /// See comment in splitRange for explanation.
    chassert(file_segments_limit
             ? file_segments.back()->range().left <= initial_range.right
             : file_segments.back()->range().contains(initial_range.right),
             fmt::format(
                 "Unexpected state. Back: {}, result range: {}, "
                 "limit: {}, initial offset: {}, initial size: {}, file size: {}",
                 file_segments.back()->range().toString(), result_range.toString(),
                 file_segments_limit, offset, size, file_size));

    chassert(!file_segments_limit || file_segments.size() <= file_segments_limit);

    locked_key.reset();
    assertCacheCorrectnessWithProbability();
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

    std::unique_ptr<FileSegmentsHolder> holder;
    if (auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, OriginInfo(user_id));
        locked_key != nullptr)
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
            holder = std::make_unique<FileSegmentsHolder>(std::move(file_segments));
        }
    }

    if (!holder)
        holder = std::make_unique<FileSegmentsHolder>(FileSegments{std::make_shared<FileSegment>(key, offset, size, FileSegment::State::DETACHED)});

    assertCacheCorrectnessWithProbability();
    return holder;
}

KeyMetadata::iterator FileCache::addFileSegment(
    LockedKey & locked_key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    const CreateFileSegmentSettings & create_settings)
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

    FileSegment::State result_state = state;

    auto file_segment = std::make_shared<FileSegment>(
        key,
        offset,
        size,
        result_state,
        create_settings,
        metadata.isBackgroundDownloadEnabled(),
        this,
        locked_key.getKeyMetadata());

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

bool FileCache::tryIncreasePriority(FileSegment & file_segment)
{
    return main_priority->tryIncreasePriority(
        *file_segment.getQueueIterator(), file_segment.isCompleted(), cache_guard, cache_state_guard);
}

bool FileCache::tryReserve(
    FileSegment & file_segment,
    size_t size,
    FileCacheReserveStat & reserve_stat,
    const OriginInfo & origin_info,
    size_t lock_wait_timeout_milliseconds,
    std::string & failure_reason)
{
    CurrentMetrics::Increment increment(CurrentMetrics::FilesystemCacheReserveThreads);
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheReserveMicroseconds);
    ProfileEvents::increment(ProfileEvents::FilesystemCacheReserveAttempts);

    assertInitialized();

    /// Non-atomic optimization for dynamic cache resize, which can be made in parallel,
    /// but helps to avoid taking a mutex in some cases.
    if (cache_is_being_resized.load(std::memory_order_relaxed))
    {
        ProfileEvents::increment(ProfileEvents::FilesystemCacheFailToReserveSpaceBecauseOfCacheResize);
        failure_reason = "cache is being resized";
        return false;
    }

    cache_reserve_active_threads.fetch_add(1, std::memory_order_relaxed);
    SCOPE_EXIT({
        cache_reserve_active_threads.fetch_sub(1, std::memory_order_relaxed);
    });

    LOG_TEST(log, "Trying to reserve space ({} bytes) for {}:{}", size, file_segment.key(), file_segment.offset());

    return doTryReserve(
        file_segment, size, reserve_stat, origin_info, lock_wait_timeout_milliseconds,
        failure_reason);
}

bool FileCache::doTryReserve(
    FileSegment & file_segment,
    size_t size,
    FileCacheReserveStat & reserve_stat,
    const OriginInfo & origin_info,
    size_t /* lock_wait_timeout_milliseconds */,
    std::string & failure_reason)
{
    auto main_priority_iterator = file_segment.getQueueIterator();
#ifdef DEBUG_OR_SANITIZER_BUILD
    /// A file_segment_metadata acquires a priority iterator
    /// on first successful space reservation attempt,
    /// so main_priority_iterator == nullptr, if no space reservation took place yet.
    if (main_priority_iterator)
        chassert(file_segment.getReservedSize() > 0);
    else
        chassert(file_segment.getReservedSize() == 0);
#endif

    /// In case of per query cache limit (by default disabled),
    /// we add/remove entries from both (main_priority and query_priority) priority queues,
    /// but iterate entries in order of query_priority, while checking the limits in both.
    Priority * query_priority = nullptr;
    FileCacheQueryLimit::QueryContextPtr query_context;

    std::unique_ptr<EvictionInfo> main_eviction_info; /// Server scoped cache limits eviction info.
    std::unique_ptr<EvictionInfo> query_eviction_info; /// Query scoped cache limits eviction info.

    /// First collect "eviction info" under cache state lock, which will tell us
    /// how much do we need to evict in order to make sure we have enough space in cache.
    /// Also "eviction info" includes `HoldSpace` holders in case all/subset of cache size
    /// is already available, making sure current thread "locks" that part of the cache
    /// before starting to evict remaining space.
    {
        size_t required_elements_num = main_priority_iterator ? 0 : 1;

        auto lock = cache_state_guard.lock();

        /// Check per-query cache limits.
        if (query_limit)
        {
            query_context = query_limit->tryGetQueryContext(lock);
            if (query_context)
            {
                query_priority = &query_context->getPriority();
                if (!query_priority->canFit(size, required_elements_num, lock, /* reservee */nullptr, origin_info)
                    && !query_context->recacheOnFileCacheQueryLimitExceeded())
                {
                    LOG_TEST(
                        log, "Query limit exceeded, space reservation failed, "
                        "recache_on_query_limit_exceeded is disabled (while reserving for {}:{} with size {}): {}",
                        file_segment.key(), file_segment.offset(), size, query_priority->getStateInfoForLog(lock));

                    failure_reason = "query limit exceeded";
                    return false;
                }
                query_eviction_info = query_priority->collectEvictionInfo(
                    size,
                    required_elements_num,
                    main_priority_iterator.get(),
                    /* is_total_space_cleanup */false,
                    origin_info,
                    lock);
            }
        }

        /// Check server-wide cache limits.
        main_eviction_info = main_priority->collectEvictionInfo(
            size,
            required_elements_num,
            main_priority_iterator.get(),
            /* is_total_space_cleanup */false,
            origin_info,
            lock);

        /// Can we already just increment size for the queue entry and quit?
        /// TODO: allow to quit here if query_context != nullptr.
        if (main_priority_iterator && !main_eviction_info->requiresEviction() && !query_context)
        {
            main_eviction_info->releaseHoldSpace(lock);
            main_priority_iterator->incrementSize(size, lock);

            file_segment.reserved_size += size;
            chassert(file_segment.reserved_size == main_priority_iterator->getEntry()->size);
            return true;
        }
    }

    EvictionCandidates eviction_candidates;
    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;

    /// Collect candidates for eviction and
    /// evict them from in-memory state and from filesystem.
    if (!doEviction(
        *main_eviction_info, query_eviction_info.get(), file_segment, origin_info,
        main_priority_iterator, reserve_stat, eviction_candidates,
        invalidated_entries, query_priority, failure_reason))
    {
        chassert(!failure_reason.empty());
        return false;
    }

    bool added_new_main_entry = !main_priority_iterator;
    Priority::IteratorPtr query_priority_iterator;

    if (!main_priority_iterator || eviction_candidates.requiresAfterEvictWrite())
    {
        auto lock = cache_guard.writeLock();
        eviction_candidates.afterEvictWrite(lock);
        IFileCachePriority::removeEntries(invalidated_entries, lock);

        if (!main_priority_iterator)
        {
            /// Create a new queue entry with size 0.
            main_priority_iterator = main_priority->add(
                file_segment.getKeyMetadata(),
                file_segment.offset(),
                /* size */0,
                lock,
                nullptr);

            if (query_context)
            {
                query_priority_iterator = query_context->tryGet(file_segment.key(), file_segment.offset(), lock);
                if (!query_priority_iterator)
                    query_context->add(file_segment.getKeyMetadata(), file_segment.offset(), /* size */0, lock);
            }
        }
    }
    else if (!invalidated_entries.empty())
    {
        if (auto lock = cache_guard.tryWriteLock(); lock.owns_lock())
            IFileCachePriority::removeEntries(invalidated_entries, lock);
    }

    try
    {
        auto lock = cache_state_guard.lock();
        main_priority_iterator->check(lock);
        main_eviction_info->releaseHoldSpace(lock);
        if (query_eviction_info)
            query_eviction_info->releaseHoldSpace(lock);

        main_priority_iterator->check(lock);
        eviction_candidates.afterEvictState(lock);
        main_priority_iterator->incrementSize(size, lock);

        if (query_priority_iterator)
            query_priority_iterator->incrementSize(size, lock);
    }
    catch (...)
    {
        /// Protect against zombie queue entries which are not assigned to any file segment
        /// and are not "invalidated" (which makes them non-removable).
        if (main_priority_iterator && added_new_main_entry)
            main_priority_iterator->invalidate();

        throw;
    }

    /// Mark that size was successfully updated.
    if (added_new_main_entry)
        file_segment.setQueueIterator(main_priority_iterator);

    file_segment.reserved_size += size;
    chassert(file_segment.reserved_size == main_priority_iterator->getEntry()->size);

    if (!file_segment.getKeyMetadata()->createBaseDirectory())
    {
        failure_reason = "not enough space on device";
        return false;
    }

    return true;
}

bool FileCache::doEviction(
    const EvictionInfo & main_eviction_info,
    const EvictionInfo * query_eviction_info,
    FileSegment & file_segment,
    const OriginInfo & origin_info,
    const IFileCachePriority::IteratorPtr & main_priority_iterator,
    FileCacheReserveStat & reserve_stat,
    EvictionCandidates & eviction_candidates,
    IFileCachePriority::InvalidatedEntriesInfos & invalidated_entries,
    Priority * query_priority,
    std::string & failure_reason)
{
    LOG_TEST(log, "Main eviction info {}", main_eviction_info.toString());
    if (query_priority)
        LOG_TEST(log, "Query eviction info {}", query_eviction_info->toString());

    if (!main_eviction_info.requiresEviction()
        && (!query_eviction_info || !query_eviction_info->requiresEviction()))
        return true;

    /// If there is at least something we need to evict,
    /// we need to collect "eviction candidates".
    /// This is done under "read (shared) lock" for query priority queue.
    {
        auto on_cannot_evict_enough_space_message = [&](const IFileCachePriority & priority)
        {
            const auto & stat = reserve_stat.total_stat;
            return fmt::format(
                "cannot evict enough space "
                "(stat: {}, total size: {}/{}, total elements: {}/{}, "
                "background download elements: {})",
                stat.toString(),
                priority.getSizeApprox(), priority.getSizeLimitApprox(),
                priority.getElementsCountApprox(), priority.getElementsLimitApprox(),
                CurrentMetrics::get(CurrentMetrics::FilesystemCacheDownloadQueueElements));
        };

        bool continue_from_last_eviction_pos = cache_reserve_active_threads.load(std::memory_order_relaxed) > 1;
        if (!continue_from_last_eviction_pos)
            main_priority->resetEvictionPos();

        if (query_eviction_info && query_eviction_info->requiresEviction())
        {
            chassert(query_priority);
            if (!query_priority->collectCandidatesForEviction(
                    *query_eviction_info,
                    reserve_stat,
                    eviction_candidates,
                    invalidated_entries,
                    /* reservee */{},
                    continue_from_last_eviction_pos,
                    /* max_candidates_size */0,
                    /* is_total_space_cleanup */false,
                    origin_info,
                    cache_guard,
                    cache_state_guard))
            {
                failure_reason = on_cannot_evict_enough_space_message(*query_priority);
                return false;
            }

            LOG_TEST(log, "Query limits satisfied (while reserving for {}:{})",
                     file_segment.key(), file_segment.offset());
        }

        if (!main_priority->collectCandidatesForEviction(
                main_eviction_info,
                reserve_stat,
                eviction_candidates,
                invalidated_entries,
                main_priority_iterator,
                continue_from_last_eviction_pos,
                /* max_candidates_size */0,
                /* is_total_space_cleanup */false,
                origin_info,
                cache_guard,
                cache_state_guard))
        {
            failure_reason = on_cannot_evict_enough_space_message(*main_priority);
            return false;
        }
    }

    /// Remove eviction candidates from filesystem.
    /// This is done without any lock.
    if (eviction_candidates.size() > 0)
    {
        auto on_failed_evict = [&]()
        {
            {
                auto lock = cache_guard.writeLock();
                eviction_candidates.afterEvictWrite(lock);
                IFileCachePriority::removeEntries(invalidated_entries, lock);
            }
            eviction_candidates.afterEvictState(cache_state_guard.lock());
        };
        try
        {
            eviction_candidates.evict();
        }
        catch (...)
        {
            on_failed_evict();
            throw;
        }

        const auto & failed_candidates = eviction_candidates.getFailedCandidates();
        if (failed_candidates.size() > 0)
        {
            on_failed_evict();
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Failed to evict {} file segments (first error: {})",
                failed_candidates.size(), failed_candidates.getFirstErrorMessage());
        }
    }
    return true;
}

void FileCache::freeSpaceRatioKeepingThreadFunc()
{
    static constexpr auto lock_failed_reschedule_ms = 1000;
    static constexpr auto space_ratio_satisfied_reschedule_ms = 5000;
    static constexpr auto general_reschedule_ms = 5000;

    if (shutdown)
        return;

    while (fiu_fail(FailPoints::file_cache_stall_free_space_ratio_keeping_thread))
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    Stopwatch watch;

    std::unique_ptr<EvictionInfo> eviction_info;
    {
        auto lock = cache_state_guard.tryLock();

        /// To avoid deteriorating contention on cache,
        /// proceed only if cache is not heavily used.
        if (!lock)
        {
            keep_up_free_space_ratio_task->scheduleAfter(lock_failed_reschedule_ms);
            return;
        }

        const size_t size_limit = main_priority->getSizeLimit(lock);
        const size_t elements_limit = main_priority->getElementsLimit(lock);

        const size_t desired_size = std::lround(keep_current_size_to_max_ratio * static_cast<double>(size_limit));
        const size_t desired_elements_num = std::lround(keep_current_elements_to_max_ratio * static_cast<double>(elements_limit));

        const size_t current_size = main_priority->getSize(lock);
        const size_t current_elements_num = main_priority->getElementsCount(lock);

        const size_t size_to_evict = current_size && (current_size > desired_size)
            ? current_size - desired_size
            : 0;
        const size_t elements_to_evict = current_elements_num && (current_elements_num > desired_elements_num)
            ? current_elements_num - desired_elements_num
            : 0;

        if (!size_to_evict && !elements_to_evict)
        {
            /// Nothing to free - all limits are satisfied.
            keep_up_free_space_ratio_task->scheduleAfter(space_ratio_satisfied_reschedule_ms);
            return;
        }

        LOG_TRACE(
            log, "Starting an iteration to maintain desired size. "
            "Current size {}/{}, elements {}/{}. Desired size: {}, desired elements: {}",
            main_priority->getSize(lock), size_limit,
            main_priority->getElementsCount(lock), elements_limit,
            desired_size, desired_elements_num);

        eviction_info = main_priority->collectEvictionInfo(
            size_to_evict,
            elements_to_evict,
            /* reservee */nullptr,
            /* is_total_space_cleanup */true,
            getInternalOrigin(),
            lock);
    }

    chassert(!eviction_info->hasHoldSpace());
    chassert(eviction_info->requiresEviction());

    ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadRun);

    FileCacheReserveStat stat;
    EvictionCandidates eviction_candidates;

    IFileCachePriority::CollectStatus desired_size_status =  IFileCachePriority::CollectStatus::CANNOT_EVICT;
    /// Collect at most `keep_up_free_space_remove_batch` elements to evict,
    /// (we use batches to make sure we do not block cache for too long,
    /// by default the batch size is quite small).

    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;
    main_priority->collectCandidatesForEviction(
        *eviction_info,
        stat,
        eviction_candidates,
        invalidated_entries,
        /* reservee */nullptr,
        /* continue_from_last_eviction_pos */false,
        /* max_candidates_size */keep_up_free_space_remove_batch,
        /* is_total_space_cleanup */true,
        getInternalOrigin(),
        cache_guard,
        cache_state_guard);

    if (eviction_candidates.size() > 0)
    {
        desired_size_status = IFileCachePriority::CollectStatus::SUCCESS;
        eviction_candidates.evict();
    }

    /// Take lock again to finalize eviction,
    /// e.g. to update the in-memory state.
    {
        auto lock = cache_guard.writeLock();
        eviction_candidates.afterEvictWrite(lock);
        IFileCachePriority::removeEntries(invalidated_entries, lock);
    }
    eviction_candidates.afterEvictState(cache_state_guard.lock());

    ProfileEvents::increment(ProfileEvents::FilesystemCacheBackgroundEvictedFileSegments, eviction_candidates.size());
    ProfileEvents::increment(ProfileEvents::FilesystemCacheBackgroundEvictedBytes, eviction_candidates.bytes());

    watch.stop();
    ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds, watch.elapsedMilliseconds());

    LOG_TRACE(log, "Free space ratio keeping thread finished with status `{}` in {} ms",
              desired_size_status, watch.elapsedMilliseconds());

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

FileCache::CacheIteratorPtr FileCache::getCacheIterator(const UserID & user_id)
{
    return metadata.getIterator(user_id);
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
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW, OriginInfo(user_id));
    locked_key->removeFileSegment(offset);
}

void FileCache::removeFileSegmentIfExists(const Key & key, size_t offset, const UserID & user_id)
{
    assertInitialized();
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, OriginInfo(user_id));
    if (locked_key)
        locked_key->removeFileSegmentIfExists(offset);
}

void FileCache::removePathIfExists(const String & path, const UserID & user_id)
{
    removeKeyIfExists(Key::fromPath(path), user_id);
}

void FileCache::removeAllReleasable(const UserID & user_id)
{
    assertInitialized();
    assertCacheCorrectness();

    metadata.removeAllKeys(/* if_releasable */true, user_id);
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
    main_priority->shuffle(cache_guard.writeLock());
}

void FileCache::loadMetadataImpl()
{
    auto parse_user = [&](const fs::path & path) -> std::optional<OriginInfo>
    {
        auto filename = path.filename().string();

        auto pos = filename.find_last_of('.');
        if (pos == std::string::npos)
            return std::nullopt;

        return OriginInfo(filename.substr(0, pos), parse<UInt64>(filename.substr(pos + 1)));
    };

    auto get_keys_dir_to_process_with_user_dir = [
        &,
        initialized = false,
        user_it = fs::directory_iterator{},
        origin = OriginInfo{},
        key_prefix_it = fs::directory_iterator{},
        get_key_mutex = std::mutex()]
        () mutable -> std::optional<std::pair<fs::path, OriginInfo>>
    {
        std::lock_guard lk(get_key_mutex);
        while (true)
        {
            if (key_prefix_it == fs::directory_iterator{})
            {
                if (initialized)
                {
                    if (user_it == fs::directory_iterator{})
                    {
                        /// No more user directories.
                        return std::nullopt;
                    }

                    /// Go to next user.
                    ++user_it;
                }
                else
                {
                    user_it = fs::directory_iterator{metadata.getBaseDirectory()};
                    initialized = true;
                }

                if (user_it == fs::directory_iterator{})
                {
                    /// No more user directories.
                    return std::nullopt;
                }

                if (user_it->path().filename() == "status")
                    continue;

                key_prefix_it = fs::directory_iterator{user_it->path()};
                if (key_prefix_it == fs::directory_iterator())
                {
                    /// User directory is empty.
                    fs::remove(user_it->path());
                    continue;
                }

                auto parsed_result = parse_user(user_it->path());
                if (parsed_result.has_value())
                {
                    origin = parsed_result.value();
                }
                else
                {
                    LOG_WARNING(log, "Unexpected file format: {}", user_it->path().string());
                    continue;
                }
            }

            auto path = key_prefix_it->path();
            if (key_prefix_it->is_directory())
            {
                ++key_prefix_it;
                return std::pair{path, origin};
            }
            ++key_prefix_it;
        }
    };

    std::vector<FileSegmentKeyType> key_types_to_load{FileSegmentKeyType::Data, FileSegmentKeyType::System};
    if (!use_split_cache)
        key_types_to_load.push_back(FileSegmentKeyType::General);

    auto get_keys_dir_to_process = [
        &,
        key_prefix_it = fs::directory_iterator{},
        key_type_index = 0ull,
        origin = getCommonOrigin(),
        get_key_mutex = std::mutex()]
        () mutable -> std::optional<std::pair<fs::path, OriginInfo>>
    {
        std::lock_guard lk(get_key_mutex);

        while (true)
        {
            if (key_prefix_it == fs::directory_iterator())
            {
                if (key_type_index == key_types_to_load.size())
                    return std::nullopt;

                auto type = key_types_to_load[key_type_index];
                auto dir_path = fs::path(metadata.getBaseDirectory()).append(getKeyTypePrefix(type));
                origin.segment_type = type;
                key_type_index++;

                if (!fs::exists(dir_path))
                    continue;
                key_prefix_it = fs::directory_iterator{dir_path};
                continue;
            }

            auto path = key_prefix_it->path();

            const std::string key_dir = path.filename();
            if (key_prefix_it->is_directory() &&
                key_dir != getKeyTypePrefix(FileSegmentKeyType::Data) &&
                key_dir != getKeyTypePrefix(FileSegmentKeyType::System)
            )
            {
                key_prefix_it++;
                return std::pair{path, origin};
            }

            if (!key_prefix_it->is_directory() && key_prefix_it->path().filename() != "status")
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

    if (write_cache_per_user_directory && use_split_cache)
        LOG_WARNING(log, "use_split_cache currently unsupported with write_cache_per_user_directory. Will ignore use_split_cache.");

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
                        fs::path path;
                        OriginInfo origin;
                        if (write_cache_per_user_directory)
                        {
                            auto result = get_keys_dir_to_process_with_user_dir();
                            if (!result.has_value())
                                return;
                            path = result.value().first;
                            origin = result.value().second;

                            LOG_TEST(log, "Loading cache for user {} (weight: {})", origin.user_id, origin.weight.value());
                        }
                        else
                        {
                            auto result = get_keys_dir_to_process();
                            if (!result.has_value())
                                return;

                            path = result.value().first;
                            origin = result.value().second;
                            LOG_TEST(log, "Loading cache keys from {}", path.c_str());
                        }

                        loadMetadataForKeys(path, origin);
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

    assertCacheCorrectness();
}

void FileCache::loadMetadataForKeys(const fs::path & keys_dir, const OriginInfo & origin_info)
{
    fs::directory_iterator key_it{keys_dir};
    if (key_it == fs::directory_iterator{})
    {
        LOG_DEBUG(log, "Removing empty key prefix directory: {}", keys_dir.string());
        fs::remove(keys_dir);
        return;
    }

    UInt64 offset = 0;
    UInt64 size = 0;
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
        auto key_metadata = metadata.getKeyMetadata(
            key,
            CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY,
            origin_info,
            /* is_initial_load */true);

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
                auto lock = cache_guard.writeLock();
                auto state_lock = cache_state_guard.lock();
                size_limit = main_priority->getSizeLimit(state_lock);

                limits_satisfied = main_priority->canFit(size, 1, state_lock, /* reservee */nullptr, origin_info, true);
                if (limits_satisfied)
                    cache_it = main_priority->add(
                        key_metadata, offset, size, lock, &state_lock, /* best_effort */true);

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
                    cache_it->remove(cache_guard.writeLock());
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
            metadata.removeKey(key, false, false, origin_info.user_id);
        }
    }
}

FileCache::~FileCache()
{
    deactivateBackgroundOperations();
    assertCacheCorrectness();
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
    assertCacheCorrectness();

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
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW_LOGICAL, OriginInfo(user_id));
    for (const auto & [_, file_segment_metadata] : *locked_key)
        file_segments.push_back(FileSegment::getInfo(file_segment_metadata->file_segment));
    return file_segments;
}

IFileCachePriority::PriorityDumpPtr FileCache::dumpQueue()
{
    assertInitialized();
    return main_priority->dump(cache_guard.readLock());
}

IFileCachePriority::Type FileCache::getEvictionPolicyType()
{
    assertInitialized();
    return main_priority->getType();
}

std::unordered_map<std::string, FileCache::UsageStat> FileCache::getUsageStatPerClient()
{
    assertInitialized();
    return main_priority->getUsageStatPerClient();
}

std::vector<String> FileCache::tryGetCachePaths(const Key & key)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, getInternalOrigin());
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

size_t FileCache::getMaxCacheSize() const
{
    return main_priority->getSizeLimitApprox();
}

size_t FileCache::getFileSegmentsNum() const
{
    /// We use this method for metrics, so it is ok to get approximate result.
    return main_priority->getElementsCountApprox();
}

void FileCache::assertCacheCorrectnessWithProbability()
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    if (check_cache_probability.doCheck())
    {
        assertCacheCorrectness();
    }
#endif
}

void FileCache::assertCacheCorrectness()
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    LOG_TEST(log, "Checking cache correctness");
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheCheckCorrectnessMicroseconds);
    ProfileEvents::increment(ProfileEvents::FilesystemCacheCheckCorrectness);

    metadata.iterate([&](LockedKey & locked_key)
    {
        for (const auto & [_, file_segment_metadata] : locked_key)
        {
            chassert(file_segment_metadata->file_segment->assertCorrectness());
        }
    }, getInternalOrigin().user_id);

    FileCacheReserveStat stat;
    main_priority->iterate([](LockedKey &, const FileSegmentMetadataPtr & file_segment_metadata)
    {
        chassert(file_segment_metadata->file_segment->assertCorrectness());
        return IFileCachePriority::IterationResult::CONTINUE;
    }, stat, cache_guard.readLock());

    main_priority->check(cache_state_guard.lock());
#endif
}

void FileCache::applySettingsIfPossible(const FileCacheSettings & new_settings, FileCacheSettings & actual_settings)
{
    if (!is_initialized || shutdown || new_settings == actual_settings)
        return;

    std::lock_guard lock(apply_settings_mutex);

    if (new_settings[FileCacheSetting::background_download_queue_size_limit] != actual_settings[FileCacheSetting::background_download_queue_size_limit]
        && metadata.setBackgroundDownloadQueueSizeLimit(new_settings[FileCacheSetting::background_download_queue_size_limit]))
    {
        LOG_INFO(log, "Changed background_download_queue_size from {} to {}",
                 actual_settings[FileCacheSetting::background_download_queue_size_limit].value,
                 new_settings[FileCacheSetting::background_download_queue_size_limit].value);

        actual_settings[FileCacheSetting::background_download_queue_size_limit] = new_settings[FileCacheSetting::background_download_queue_size_limit];
    }

    if (new_settings[FileCacheSetting::background_download_threads] != actual_settings[FileCacheSetting::background_download_threads])
    {
        bool updated = false;
        try
        {
            updated = metadata.setBackgroundDownloadThreads(new_settings[FileCacheSetting::background_download_threads]);
        }
        catch (...)
        {
            actual_settings[FileCacheSetting::background_download_threads] = metadata.getBackgroundDownloadThreads();
            throw;
        }

        if (updated)
        {
            LOG_INFO(log, "Changed background_download_threads from {} to {}",
                    actual_settings[FileCacheSetting::background_download_threads].value,
                    new_settings[FileCacheSetting::background_download_threads].value);

            actual_settings[FileCacheSetting::background_download_threads] = new_settings[FileCacheSetting::background_download_threads];
        }
    }

    if (new_settings[FileCacheSetting::background_download_max_file_segment_size] != actual_settings[FileCacheSetting::background_download_max_file_segment_size])
    {
        background_download_max_file_segment_size = new_settings[FileCacheSetting::background_download_max_file_segment_size];

        LOG_INFO(log, "Changed background_download_max_file_segment_size from {} to {}",
                actual_settings[FileCacheSetting::background_download_max_file_segment_size].value,
                new_settings[FileCacheSetting::background_download_max_file_segment_size].value);

        actual_settings[FileCacheSetting::background_download_max_file_segment_size] = new_settings[FileCacheSetting::background_download_max_file_segment_size];
    }

    {
        SizeLimits desired_limits{
            .max_size = new_settings[FileCacheSetting::max_size],
            .max_elements = new_settings[FileCacheSetting::max_elements],
            .slru_size_ratio = new_settings[FileCacheSetting::slru_size_ratio]
        };
        SizeLimits current_limits{
            .max_size = actual_settings[FileCacheSetting::max_size],
            .max_elements = actual_settings[FileCacheSetting::max_elements],
            .slru_size_ratio = actual_settings[FileCacheSetting::slru_size_ratio]
        };

        const bool max_size_changed = desired_limits.max_size != current_limits.max_size;
        const bool max_elements_changed = desired_limits.max_elements != current_limits.max_elements;
        const bool slru_ratio_changed = desired_limits.slru_size_ratio != current_limits.slru_size_ratio;

        const bool do_dynamic_resize = (max_size_changed || max_elements_changed) && !slru_ratio_changed;

        if (allow_dynamic_cache_resize && do_dynamic_resize && !main_priority->isOvercommitEviction())
        {
            auto result_limits = doDynamicResize(current_limits, desired_limits);

            actual_settings[FileCacheSetting::max_size] = result_limits.max_size;
            actual_settings[FileCacheSetting::max_elements] = result_limits.max_elements;
        }
        else if (do_dynamic_resize)
        {
            LOG_WARNING(
                log, "Filesystem cache size was modified, "
                "but dynamic cache resize is disabled, "
                "therefore cache size will not be changed without server restart. "
                "To enable dynamic cache resize, "
                "add `allow_dynamic_cache_resize` to cache configuration");
        }
        else
        {
            LOG_DEBUG(
                log, "Nothing to resize in filesystem cache. "
                "Current max size: {}, max_elements: {}, slru ratio: {}",
                current_limits.max_size, current_limits.max_elements, current_limits.slru_size_ratio);
        }
    }

    if (new_settings[FileCacheSetting::max_file_segment_size] != actual_settings[FileCacheSetting::max_file_segment_size])
    {
        max_file_segment_size = actual_settings[FileCacheSetting::max_file_segment_size] = new_settings[FileCacheSetting::max_file_segment_size];
    }
}

FileCache::SizeLimits FileCache::doDynamicResize(const SizeLimits & prev_limits, const SizeLimits & desired_limits)
{
    if (prev_limits.slru_size_ratio != desired_limits.slru_size_ratio)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dynamic resize of size ratio is not allowed");

    struct ResizeHolder
    {
        std::atomic_bool & hold;
        explicit ResizeHolder(std::atomic_bool & hold_) : hold(hold_) { hold.store(true, std::memory_order_relaxed); }
        ~ResizeHolder() { hold.store(false, std::memory_order_relaxed); }
    };

    SizeLimits result_limits;
    bool modified_size_limit = false;
    {
        ResizeHolder hold(cache_is_being_resized);
        auto cache_lock = cache_state_guard.lock();

        if (prev_limits.max_size != main_priority->getSizeLimit(cache_lock))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Current limits inconsistency in size");

        if (prev_limits.max_elements != main_priority->getElementsLimit(cache_lock))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Current limits inconsistency in elements number");

        try
        {
            modified_size_limit = doDynamicResizeImpl(prev_limits, desired_limits, result_limits, cache_lock);
            chassert(result_limits.max_size && result_limits.max_elements);
        }
        catch (...)
        {
            LOG_ERROR(
                log, "Unexpected error during dynamic cache resize: {}",
                getCurrentExceptionMessage(true));

            if (!cache_lock.owns_lock())
                cache_lock.lock();

            size_t max_size = main_priority->getSizeLimit(cache_lock);
            size_t max_elements = main_priority->getElementsLimit(cache_lock);

            if (result_limits.max_size != max_size || result_limits.max_elements != max_elements)
            {
                LOG_DEBUG(
                    log, "Resetting max size from {} to {}, max elements from {} to {}",
                    result_limits.max_size, max_size, result_limits.max_elements, max_elements);

                result_limits.max_size = max_size;
                result_limits.max_elements = max_elements;
            }

            cache_lock.unlock();
            assertCacheCorrectness();
            throw;
        }
    }

    if (modified_size_limit)
    {
        LOG_INFO(log, "Changed max_size from {} to {}, max_elements from {} to {}",
                 prev_limits.max_size, result_limits.max_size,
                 prev_limits.max_elements, result_limits.max_elements);

        chassert(result_limits.max_size == desired_limits.max_size);
        chassert(result_limits.max_elements == desired_limits.max_elements);
    }
    else
    {
        LOG_WARNING(
            log, "Unable to modify size limit from {} to {}, elements limit from {} to {}. "
            "`max_size` and `max_elements` settings will remain inconsistent with config.xml. "
            "Next attempt to update them will happen on the next config reload. "
            "You can trigger it with SYSTEM RELOAD CONFIG.",
            prev_limits.max_size, desired_limits.max_size,
            prev_limits.max_elements, desired_limits.max_elements);
    }

    chassert(main_priority->getSizeApprox() <= result_limits.max_size);
    chassert(main_priority->getElementsCountApprox() <= result_limits.max_elements);

    chassert(main_priority->getSizeLimit(cache_state_guard.lock()) == result_limits.max_size);
    chassert(main_priority->getElementsLimit(cache_state_guard.lock()) == result_limits.max_elements);

    assertCacheCorrectness();
    return result_limits;
}

bool FileCache::doDynamicResizeImpl(
    const SizeLimits & prev_limits,
    const SizeLimits & desired_limits,
    SizeLimits & result_limits,
    CacheStateGuard::Lock & state_lock)
{
    /// In order to not block cache for the duration of cache resize,
    /// we do:
    /// a. Take a cache lock.
    ///     1. Collect eviction candidates,
    ///     2. If eviction candidates size is non-zero,
    ///        remove their queue entries.
    ///        This will release space we consider to be hold for them,
    ///        so that we can safely modify size limits.
    ///     3. Modify size limits of cache.
    /// b. Release a cache lock.
    ///     1. Do actual eviction from filesystem.

    auto eviction_info = main_priority->collectEvictionInfoForResize(
        desired_limits.max_size,
        desired_limits.max_elements,
        getInternalOrigin(),
        state_lock);

    chassert(!eviction_info->hasHoldSpace());

    EvictionCandidates eviction_candidates;
    if (!eviction_info->requiresEviction())
    {
        /// Nothing needs to be evicted,
        /// just modify the limits and we are done.

        main_priority->modifySizeLimits(
            desired_limits.max_size,
            desired_limits.max_elements,
            desired_limits.slru_size_ratio,
            state_lock);

        result_limits = desired_limits;

        LOG_INFO(
            log, "Nothing needs to be evicted for new size limits ({})",
            main_priority->getStateInfoForLog(state_lock));
        return true;
    }

    state_lock.unlock();

    FileCacheReserveStat stat;
    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;
    if (!main_priority->collectCandidatesForEviction(
            *eviction_info,
            stat,
            eviction_candidates,
            invalidated_entries,
            /* reservee */nullptr,
            /* continue_from_last_eviction_pos */false,
            /* max_candidates_size */0,
            /* is_total_space_cleanup */true,
            getInternalOrigin(),
            cache_guard,
            cache_state_guard))
    {
        result_limits = prev_limits;
        LOG_INFO(log, "Dynamic cache resize is not possible at the moment");
        return false;
    }

    /// Remove only queue entries of eviction candidates.
    /// Hold the write lock until after modifying size limits,
    /// to prevent concurrent `tryIncreasePriority` from promoting entries
    /// into the protected queue between removal and limit modification.
    auto write_lock = cache_guard.writeLock();
    eviction_candidates.removeQueueEntries(write_lock);

    /// Note that (in-memory) metadata about corresponding file segments
    /// (e.g. file segment info in CacheMetadata) will be removed
    /// only after eviction from filesystem. This is needed to avoid
    /// a race on removal of file from filesystsem and
    /// addition of the same file as part of a newly cached file segment.

    state_lock.lock();

    /// Modify cache size limits.
    /// From this point cache eviction will follow them.
    main_priority->modifySizeLimits(
        desired_limits.max_size,
        desired_limits.max_elements,
        desired_limits.slru_size_ratio,
        state_lock);

    state_lock.unlock();
    write_lock.unlock();

    /// Do actual eviction from filesystem.
    eviction_candidates.evict();

    chassert(!eviction_candidates.requiresAfterEvictWrite());
    IFileCachePriority::removeEntries(invalidated_entries, cache_guard.writeLock());

    auto failed_candidates = eviction_candidates.getFailedCandidates();
    if (failed_candidates.size() == 0)
    {
        LOG_INFO(
            log, "Successfully evicted {} cache elements needed for resize",
            eviction_candidates.size());

        result_limits = desired_limits;
        return true;
    }

    result_limits.max_size = std::min(
        prev_limits.max_size,
        desired_limits.max_size + failed_candidates.total_cache_size);

    result_limits.max_elements = std::min(
        prev_limits.max_elements,
        desired_limits.max_elements + failed_candidates.total_cache_elements);

    LOG_INFO(
        log, "Having {} failed candidates with total size {}. "
        "Will set current limits as {} in size and {} in elements number",
        failed_candidates.total_cache_elements, failed_candidates.total_cache_size,
        result_limits.max_size, result_limits.max_elements);

    /// The below code corresponds to the case where
    /// we failed to execute eviction from filesystem.
    /// So here we need to restore removed queue entries to avoid broken cache state.
    /// As this case should be very rare (as it can only happen because of a bug)
    /// we allow ourselves to be suboptional here in favour of being robust
    /// and take two locks at the same time to do the restore in the most straightforward way.
    auto cache_write_lock = cache_guard.writeLock();
    state_lock.lock();

    /// Increase the max size and max elements
    /// to the size and number of failed candidates.
    main_priority->modifySizeLimits(
        result_limits.max_size,
        result_limits.max_elements,
        result_limits.slru_size_ratio,
        state_lock);

    /// Add failed candidates back to queue.
    for (const auto & [key_metadata, key_candidates, _] : failed_candidates.failed_candidates_per_key)
    {
        chassert(!key_candidates.empty());

        auto locked_key = key_metadata->tryLock();
        if (!locked_key)
        {
            /// Key cannot be removed,
            /// because if we failed to remove something from it above,
            /// then we did not remove it from key metadata,
            /// so key lock must remain valid.
            LOG_ERROR(log, "Unexpected state: key {} does not exist", key_metadata->key);
            chassert(false);
            continue;
        }

        for (const auto & candidate : key_candidates)
        {
            const auto & file_segment = candidate->file_segment;

            LOG_DEBUG(
                log, "Adding back file segment after failed eviction: {}:{}, size: {}",
                file_segment->key(), file_segment->offset(), file_segment->getDownloadedSize());

            auto main_priority_iterator = main_priority->add(
                key_metadata,
                file_segment->offset(),
                file_segment->getDownloadedSize(),
                cache_write_lock,
                &state_lock,
                false);

            candidate->setRemovedFlag(*locked_key, /* value */false);
            file_segment->setQueueIterator(main_priority_iterator);
        }
    }

    return false;
}

FileCache::QueryContextHolderPtr FileCache::getQueryContextHolder(
    const String & query_id, const ReadSettings & read_settings)
{
    if (!query_limit || read_settings.filesystem_cache_max_download_size == 0)
        return {};

    auto lock = cache_guard.writeLock();
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
    }, getInternalOrigin().user_id);
    return file_segments;
}

}
