#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <boost/functional/hash.hpp>

#include <Common/callOnce.h>
#include <Common/ThreadPool.h>
#include <Common/StatusFile.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/QueryLimit.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Cache/UserInfo.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <filesystem>


namespace DB
{
struct ReadSettings;

/// Track acquired space in cache during reservation
/// to make error messages when no space left more informative.
struct FileCacheReserveStat
{
    struct Stat
    {
        size_t releasable_size = 0;
        size_t releasable_count = 0;

        size_t non_releasable_size = 0;
        size_t non_releasable_count = 0;

        Stat & operator +=(const Stat & other)
        {
            releasable_size += other.releasable_size;
            releasable_count += other.releasable_count;
            non_releasable_size += other.non_releasable_size;
            non_releasable_count += other.non_releasable_count;
            return *this;
        }
    };

    Stat total_stat;
    std::unordered_map<FileSegmentKind, Stat> stat_by_kind;

    void update(size_t size, FileSegmentKind kind, bool releasable);

    FileCacheReserveStat & operator +=(const FileCacheReserveStat & other)
    {
        total_stat += other.total_stat;
        for (const auto & [name, stat_] : other.stat_by_kind)
            stat_by_kind[name] += stat_;
        return *this;
    }
};

/// Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
/// Different caching algorithms are implemented using IFileCachePriority.
class FileCache : private boost::noncopyable
{
public:
    using Key = DB::FileCacheKey;
    using QueryLimit = DB::FileCacheQueryLimit;
    using Priority = IFileCachePriority;
    using PriorityEntry = IFileCachePriority::Entry;
    using QueryContextHolder = FileCacheQueryLimit::QueryContextHolder;
    using UserInfo = FileCacheUserInfo;
    using UserID = UserInfo::UserID;

    FileCache(const std::string & cache_name, const FileCacheSettings & settings);

    ~FileCache();

    void initialize();

    bool isInitialized() const;

    /// Throws if `!load_metadata_asynchronously` and there is an exception in `init_exception`
    void throwInitExceptionIfNeeded();

    const String & getBasePath() const;

    static const UserInfo & getCommonUser();

    static const UserInfo & getInternalUser();

    String getFileSegmentPath(const Key & key, size_t offset, FileSegmentKind segment_kind, const UserInfo & user) const;

    String getKeyPath(const Key & key, const UserInfo & user) const;

    /**
     * Given an `offset` and `size` representing [offset, offset + size) bytes interval,
     * return list of cached non-overlapping non-empty
     * file segments `[segment1, ..., segmentN]` which intersect with given interval.
     *
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * As long as pointers to returned file segments are held
     * it is guaranteed that these file segments are not removed from cache.
     */
    FileSegmentsHolderPtr getOrSet(
        const Key & key,
        size_t offset,
        size_t size,
        size_t file_size,
        const CreateFileSegmentSettings & settings,
        size_t file_segments_limit,
        const UserInfo & user,
        std::optional<size_t> boundary_alignment_ = std::nullopt);

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * If file segment has state EMPTY, then it is also marked as "detached". E.g. it is "detached"
     * from cache (not owned by cache), and as a result will never change it's state and will be destructed
     * with the destruction of the holder, while in getOrSet() EMPTY file segments can eventually change
     * it's state (and become DOWNLOADED).
     */
    FileSegmentsHolderPtr get(
        const Key & key,
        size_t offset,
        size_t size,
        size_t file_segments_limit,
        const UserID & user_id);

    FileSegmentsHolderPtr set(
        const Key & key,
        size_t offset,
        size_t size,
        const CreateFileSegmentSettings & settings,
        const UserInfo & user);

    /// Remove file segment by `key` and `offset`. Throws if file segment does not exist.
    void removeFileSegment(const Key & key, size_t offset, const UserID & user_id);

    /// Remove files by `key`. Throws if key does not exist.
    void removeKey(const Key & key, const UserID & user_id);

    /// Remove files by `key`.
    void removeKeyIfExists(const Key & key, const UserID & user_id);

    /// Removes files by `path`.
    void removePathIfExists(const String & path, const UserID & user_id);

    /// Remove files by `key`.
    void removeAllReleasable(const UserID & user_id);

    std::vector<String> tryGetCachePaths(const Key & key);

    size_t getUsedCacheSize() const;
    size_t getMaxCacheSize() const;

    size_t getFileSegmentsNum() const;

    size_t getMaxFileSegmentSize() const { return max_file_segment_size; }

    size_t getBackgroundDownloadMaxFileSegmentSize() const { return background_download_max_file_segment_size.load(); }

    size_t getBoundaryAlignment() const { return boundary_alignment; }

    bool tryReserve(
        FileSegment & file_segment,
        size_t size,
        FileCacheReserveStat & stat,
        const UserInfo & user,
        size_t lock_wait_timeout_milliseconds,
        std::string & failure_reason);

    std::vector<FileSegment::Info> getFileSegmentInfos(const UserID & user_id);

    std::vector<FileSegment::Info> getFileSegmentInfos(const Key & key, const UserID & user_id);


    IFileCachePriority::PriorityDumpPtr dumpQueue();

    void deactivateBackgroundOperations();

    CachePriorityGuard::Lock lockCache() const;
    CachePriorityGuard::Lock tryLockCache(std::optional<std::chrono::milliseconds> acquire_timeout = std::nullopt) const;

    std::vector<FileSegment::Info> sync();

    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;
    QueryContextHolderPtr getQueryContextHolder(const String & query_id, const ReadSettings & settings);

    using IterateFunc = std::function<void(const FileSegmentInfo &)>;
    void iterate(IterateFunc && func, const UserID & user_id);

    void applySettingsIfPossible(const FileCacheSettings & new_settings, FileCacheSettings & actual_settings);

    void freeSpaceRatioKeepingThreadFunc();

private:
    using KeyAndOffset = FileCacheKeyAndOffset;

    std::atomic<size_t> max_file_segment_size;
    const size_t bypass_cache_threshold;
    const size_t boundary_alignment;
    std::atomic<size_t> background_download_max_file_segment_size;
    size_t load_metadata_threads;
    const bool load_metadata_asynchronously;
    std::atomic<bool> stop_loading_metadata = false;
    ThreadFromGlobalPool load_metadata_main_thread;
    const bool write_cache_per_user_directory;
    const bool allow_dynamic_cache_resize;

    BackgroundSchedulePoolTaskHolder keep_up_free_space_ratio_task;
    const double keep_current_size_to_max_ratio;
    const double keep_current_elements_to_max_ratio;
    const size_t keep_up_free_space_remove_batch;

    LoggerPtr log;

    std::exception_ptr init_exception;
    std::atomic<bool> is_initialized = false;
    OnceFlag initialize_called;
    mutable std::mutex init_mutex;
    std::unique_ptr<StatusFile> status_file;
    std::atomic<bool> shutdown = false;
    std::atomic<bool> cache_is_being_resized = false;

    std::mutex apply_settings_mutex;

    CacheMetadata metadata;

    FileCachePriorityPtr main_priority;
    mutable CachePriorityGuard cache_guard;

    struct HitsCountStash
    {
        HitsCountStash(size_t hits_threashold_, size_t queue_size_);
        void clear();

        const size_t hits_threshold;
        const size_t queue_size;

        std::unique_ptr<LRUFileCachePriority> queue;
        using Records = std::unordered_map<KeyAndOffset, Priority::IteratorPtr, FileCacheKeyAndOffsetHash>;
        Records records;
    };

    /**
     * A HitsCountStash allows to cache certain data only after it reached
     * a certain hit rate, e.g. if hit rate it 5, then data is cached on 6th cache hit.
     */
    mutable std::unique_ptr<HitsCountStash> stash;
    /**
     * A QueryLimit allows to control cache write limit per query.
     * E.g. if a query needs n bytes from cache, but it has only k bytes, where 0 <= k <= n
     * then allowed loaded cache size is std::min(n - k, max_query_cache_size).
     */
    FileCacheQueryLimitPtr query_limit;

    void initializeImpl(bool load_metadata);

    void assertInitialized() const;
    void assertCacheCorrectness();

    void loadMetadata();
    void loadMetadataImpl();
    void loadMetadataForKeys(const std::filesystem::path & keys_dir);

    /// Get all file segments from cache which intersect with `range`.
    /// If `file_segments_limit` > 0, return no more than first file_segments_limit
    /// file segments.
    FileSegments getImpl(
        const LockedKey & locked_key,
        const FileSegment::Range & range,
        size_t file_segments_limit) const;

    /// Split range into subranges by max_file_segment_size,
    /// each subrange size must be less or equal to max_file_segment_size.
    std::vector<FileSegment::Range> splitRange(size_t offset, size_t size, size_t aligned_size);

    FileSegments createFileSegmentsFromRanges(
        LockedKey & locked_key,
        const std::vector<FileSegment::Range> & ranges,
        size_t & file_segments_count,
        size_t file_segments_limit,
        const CreateFileSegmentSettings & create_settings);

    void fillHolesWithEmptyFileSegments(
        LockedKey & locked_key,
        FileSegments & file_segments,
        const FileSegment::Range & range,
        size_t non_aligned_right_offset,
        size_t file_segments_limit,
        bool fill_with_detached_file_segments,
        const CreateFileSegmentSettings & settings);

    KeyMetadata::iterator addFileSegment(
        LockedKey & locked_key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        const CreateFileSegmentSettings & create_settings,
        const CachePriorityGuard::Lock *);

    struct SizeLimits
    {
        size_t max_size;
        size_t max_elements;
        double slru_size_ratio;
    };
    SizeLimits doDynamicResize(const SizeLimits & current_limits, const SizeLimits & desired_limits);
    bool doDynamicResizeImpl(
        const SizeLimits & current_limits,
        const SizeLimits & desired_limits,
        SizeLimits & result_limits,
        CachePriorityGuard::Lock &);
};

}
