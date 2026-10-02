#include <Interpreters/FileCache/FileCache.h>

#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/FileCache/FileCacheSettings.h>
#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/CacheUsage.h>
#include <Interpreters/FileCache/LRUFileCachePriority.h>
#include <Interpreters/FileCache/SLRUFileCachePriority.h>
#include <Interpreters/FileCache/FileCacheUtils.h>
#include <Interpreters/FileCache/EvictionCandidates.h>
#include <Interpreters/Context.h>
#include <base/hex.h>
#include <Common/callOnce.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/FailPoint.h>
#include <Common/randomSeed.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/ServerUUID.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/DimensionalMetrics.h>
#include <Common/HistogramMetrics.h>
#include <base/unit.h>
#if ENABLE_DISTRIBUTED_CACHE
#include <Interpreters/FileCache/OvercommitFileCachePriority.h>
#endif

#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>
#include <Interpreters/FileCache/FileSegmentInfo.h>


namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event FilesystemCacheLoadMetadataMicroseconds;
    extern const Event FilesystemCacheReserveMicroseconds;
    extern const Event FilesystemCacheReserveAttempts;
    extern const Event FilesystemCacheFailedReserveAttempts;
    extern const Event FilesystemCacheGetOrSetMicroseconds;
    extern const Event FilesystemCacheGetMicroseconds;
    extern const Event FilesystemCacheFreeSpaceKeepingThreadRun;
    extern const Event FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds;
    extern const Event FilesystemCacheFreeSpaceKeepingThreadErrors;
    extern const Event FilesystemCacheFailToReserveSpaceBecauseOfCacheResize;
    extern const Event FilesystemCacheBackgroundEvictedFileSegments;
    extern const Event FilesystemCacheBackgroundEvictedBytes;
    extern const Event FilesystemCacheEvictedFileSegments;
    extern const Event FilesystemCacheEvictedBytes;
    extern const Event FilesystemCacheCheckCorrectness;
    extern const Event FilesystemCacheCheckCorrectnessMicroseconds;
    extern const Event FilesystemCacheIdleClientEvictions;
    extern const Event FilesystemCacheInvalidatedEntriesCleanupThreadWorkMilliseconds;
}

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheDownloadQueueElements;
    extern const Metric FilesystemCacheReserveThreads;
    extern const Metric FilesystemCacheSizeLimit;
    extern const Metric FilesystemCacheEvictionThreads;
    extern const Metric FilesystemCacheEvictionThreadsActive;
    extern const Metric FilesystemCacheEvictionThreadsScheduled;
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
    extern const char file_cache_pause_before_do_eviction[];
    extern const char file_cache_simulate_evicting_segment[];
    extern const char file_cache_background_eviction_push_fail[];
}

namespace FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsUInt64 max_file_segment_size;
    extern const FileCacheSettingsUInt64 boundary_alignment;
    extern const FileCacheSettingsUInt64 reserve_granularity;
    extern const FileCacheSettingsFileCachePolicy cache_policy;
    extern const FileCacheSettingsDouble slru_size_ratio;
    extern const FileCacheSettingsDouble check_cache_probability;
    extern const FileCacheSettingsNonZeroUInt64 load_metadata_threads;
    extern const FileCacheSettingsBool load_metadata_asynchronously;
    extern const FileCacheSettingsUInt64 background_download_threads;
    extern const FileCacheSettingsUInt64 background_download_queue_size_limit;
    extern const FileCacheSettingsUInt64 background_download_max_file_segment_size;
    extern const FileCacheSettingsDouble keep_free_space_size_ratio;
    extern const FileCacheSettingsDouble keep_free_space_elements_ratio;
    extern const FileCacheSettingsUInt64 keep_free_space_remove_batch;
    extern const FileCacheSettingsNonZeroUInt64 keep_free_space_eviction_threads;
    extern const FileCacheSettingsNonZeroUInt64 invalidated_entries_cleanup_interval_ms;
    extern const FileCacheSettingsNonZeroUInt64 invalidated_entries_cleanup_threshold;
    extern const FileCacheSettingsNonZeroUInt64 invalidated_entries_cleanup_remove_batch;
    extern const FileCacheSettingsBool enable_bypass_cache_with_threshold;
    extern const FileCacheSettingsUInt64 bypass_cache_threshold;
    extern const FileCacheSettingsBool write_cache_per_user_id_directory;
    extern const FileCacheSettingsUInt64 cache_hits_threshold;
    extern const FileCacheSettingsBool enable_filesystem_query_cache_limit;
    extern const FileCacheSettingsBool allow_dynamic_cache_resize;
    extern const FileCacheSettingsUInt64 dynamic_resize_lock_wait_ms;
    extern const FileCacheSettingsBool use_split_cache;
    extern const FileCacheSettingsDouble split_cache_ratio;
    extern const FileCacheSettingsUInt64 overcommit_eviction_evict_step;
    extern const FileCacheSettingsBool skip_cache_on_disk_failure;
    extern const FileCacheSettingsUInt64 idle_client_ttl_sec;
    extern const FileCacheSettingsUInt64 idle_client_check_interval_sec;
    extern const FileCacheSettingsNonZeroUInt64 idle_client_eviction_threads;
    extern const FileCacheSettingsBool expose_prometheus_eviction_metrics;
    extern const FileCacheSettingsBool expose_prometheus_eviction_metrics_per_user;
}

namespace
{
    std::string getCommonUserID()
    {
        auto user_from_context = DB::Context::getGlobalContextInstance()->getFilesystemCacheUser();
        const auto user = user_from_context.empty() ? toString(ServerUUID::get()) : user_from_context;
        return user;
    }

    const HistogramMetrics::Buckets hits_buckets = {0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 1024, 8192};
    const HistogramMetrics::Buckets size_buckets = {4_KiB, 16_KiB, 64_KiB, 256_KiB, 1_MiB, 4_MiB, 16_MiB, 64_MiB};

    DimensionalMetrics::MetricFamily & filesystem_cache_evictions_total = DimensionalMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evictions_total",
        "Number of file segments evicted from a filesystem cache, labelled by cache name.",
        {"cache_name"},
        {},
        DimensionalMetrics::MetricType::Counter);

    DimensionalMetrics::MetricFamily & filesystem_cache_evicted_bytes_total = DimensionalMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_bytes_total",
        "Total bytes of file segments evicted from a filesystem cache, labelled by cache name.",
        {"cache_name"},
        {},
        DimensionalMetrics::MetricType::Counter);

    HistogramMetrics::MetricFamily & filesystem_cache_evicted_segment_hits = HistogramMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_segment_hits",
        "Distribution of cache-hit counts on file segments at the moment of their eviction, labelled by cache name.",
        hits_buckets, {"cache_name"});

    HistogramMetrics::MetricFamily & filesystem_cache_evicted_segment_size_bytes = HistogramMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_segment_size_bytes",
        "Distribution of byte sizes of evicted file segments, labelled by cache name.",
        size_buckets, {"cache_name"});

    DimensionalMetrics::MetricFamily & filesystem_cache_evictions_by_user_total = DimensionalMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evictions_by_user_total",
        "Number of file segments evicted from a filesystem cache, additionally labelled by user id. Disabled by default; enable via `expose_prometheus_eviction_metrics_per_user`.",
        {"cache_name", "user_id"},
        {},
        DimensionalMetrics::MetricType::Counter);

    DimensionalMetrics::MetricFamily & filesystem_cache_evicted_bytes_by_user_total = DimensionalMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_bytes_by_user_total",
        "Total bytes of file segments evicted, additionally labelled by user id. Disabled by default; enable via `expose_prometheus_eviction_metrics_per_user`.",
        {"cache_name", "user_id"},
        {},
        DimensionalMetrics::MetricType::Counter);

    HistogramMetrics::MetricFamily & filesystem_cache_evicted_segment_hits_by_user = HistogramMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_segment_hits_by_user",
        "Distribution of cache-hit counts on evicted file segments, additionally labelled by user id. Disabled by default; enable via `expose_prometheus_eviction_metrics_per_user`.",
        hits_buckets, {"cache_name", "user_id"});

    HistogramMetrics::MetricFamily & filesystem_cache_evicted_segment_size_bytes_by_user = HistogramMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_segment_size_bytes_by_user",
        "Distribution of byte sizes of evicted file segments, additionally labelled by user id. Disabled by default; enable via `expose_prometheus_eviction_metrics_per_user`.",
        size_buckets, {"cache_name", "user_id"});
}

void FileCacheReserveStat::update(size_t size, FileSegmentKind kind, State state)
{
    auto & local_stat = getStatByKind(kind);
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
    wb << "invalidated count: " << invalidated_count << ", ";
    wb << "candidates iteration steps: " << candidates_iteration_steps << ", ";
    wb << "clients iterated: " << clients_iterated;
    return wb.str();
}

static double normalizeProbability(double probability)
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
    , reserve_granularity(settings[FileCacheSetting::reserve_granularity])
    , background_download_max_file_segment_size(settings[FileCacheSetting::background_download_max_file_segment_size])
    , load_metadata_threads(settings[FileCacheSetting::load_metadata_threads])
    , load_metadata_asynchronously(settings[FileCacheSetting::load_metadata_asynchronously])
    , write_cache_per_user_directory(settings[FileCacheSetting::write_cache_per_user_id_directory])
    , allow_dynamic_cache_resize(settings[FileCacheSetting::allow_dynamic_cache_resize])
    , dynamic_resize_lock_wait_ms(settings[FileCacheSetting::dynamic_resize_lock_wait_ms])
    , keep_current_size_to_max_ratio(1 - settings[FileCacheSetting::keep_free_space_size_ratio])
    , keep_current_elements_to_max_ratio(1 - settings[FileCacheSetting::keep_free_space_elements_ratio])
    , keep_up_free_space_remove_batch(settings[FileCacheSetting::keep_free_space_remove_batch])
    , keep_up_free_space_eviction_threads(settings[FileCacheSetting::keep_free_space_eviction_threads])
    , invalidated_entries_cleanup_threshold(settings[FileCacheSetting::invalidated_entries_cleanup_threshold])
    , invalidated_entries_cleanup_interval_ms(settings[FileCacheSetting::invalidated_entries_cleanup_interval_ms])
    , invalidated_entries_cleanup_remove_batch(settings[FileCacheSetting::invalidated_entries_cleanup_remove_batch])
    , idle_client_ttl_sec(settings[FileCacheSetting::idle_client_ttl_sec])
    , idle_client_check_interval_sec(settings[FileCacheSetting::idle_client_check_interval_sec])
    , idle_client_eviction_threads(settings[FileCacheSetting::idle_client_eviction_threads])
    , use_split_cache(settings[FileCacheSetting::use_split_cache])
    , split_cache_ratio(settings[FileCacheSetting::split_cache_ratio])
    , skip_cache_on_disk_failure(settings[FileCacheSetting::skip_cache_on_disk_failure])
    , expose_eviction_metrics(settings[FileCacheSetting::expose_prometheus_eviction_metrics])
    , expose_eviction_metrics_per_user(settings[FileCacheSetting::expose_prometheus_eviction_metrics_per_user])
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
            creator_function = [](IFileCachePriority::QueueType queue_type, size_t max_size, size_t max_elements, size_t /*size_ratio*/, size_t /*overcommit_eviction_evict_step*/, String description) -> IFileCachePriorityPtr
            {
                return std::make_unique<LRUFileCachePriority>(
                    queue_type,
                    max_size,
                    max_elements,
                    description);
            };
            break;
        }
        case FileCachePolicy::SLRU:
        {
            creator_function = [](IFileCachePriority::QueueType queue_type, size_t max_size, size_t max_elements, double size_ratio, size_t /*overcommit_eviction_evict_step*/, String description) -> IFileCachePriorityPtr
            {
                return std::make_unique<SLRUFileCachePriority>(
                    queue_type,
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
            creator_function = [](IFileCachePriority::QueueType queue_type, size_t max_size, size_t max_elements, double /*size_ratio*/, size_t overcommit_eviction_evict_step, String /*description*/) -> IFileCachePriorityPtr
            {
                return std::make_unique<OvercommitFileCachePriority<LRUFileCachePriority>>(
                    overcommit_eviction_evict_step,
                    queue_type,
                    max_size,
                    max_elements,
                    "overcommit");
            };
            break;
        }
        case FileCachePolicy::SLRU_OVERCOMMIT:
        {
            creator_function = [](IFileCachePriority::QueueType queue_type, size_t max_size, size_t max_elements, double size_ratio, size_t overcommit_eviction_evict_step, String /*description*/) -> IFileCachePriorityPtr
            {
                return std::make_unique<OvercommitFileCachePriority<SLRUFileCachePriority>>(
                    overcommit_eviction_evict_step,
                    queue_type,
                    max_size,
                    max_elements,
                    size_ratio,
                    "overcommit");
            };
            break;
        }
#else
        case FileCachePolicy::LRU_OVERCOMMIT:
        case FileCachePolicy::SLRU_OVERCOMMIT:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Overcommit cache policies are not supported without distributed cache");
#endif
    }
    if (use_split_cache)
    {
        /// Backstop for programmatically built settings which bypass
        /// `FileCacheSettings::validate` (config-loaded settings are rejected there).
        const auto policy = settings[FileCacheSetting::cache_policy].value;
        if (policy == FileCachePolicy::LRU_OVERCOMMIT || policy == FileCachePolicy::SLRU_OVERCOMMIT)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "`use_split_cache` is not supported with overcommit cache policies");

        main_priority = std::make_unique<SplitFileCachePriority>(
            IFileCachePriority::QueueType::Main,
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
            IFileCachePriority::QueueType::Main,
            settings[FileCacheSetting::max_size],
            settings[FileCacheSetting::max_elements],
            settings[FileCacheSetting::slru_size_ratio],
            settings[FileCacheSetting::overcommit_eviction_evict_step],
            cache_name
        );
    }

    LOG_DEBUG(log, "Using {} cache policy", settings[FileCacheSetting::cache_policy].value);

    main_priority->setOnEvictCallback([this](const FileSegment & segment, const String & user_id)
    {
        onSegmentEvicted(segment, user_id);
    });

    /// Idle-client eviction needs per-client usage, which only overcommit policies keep.
    /// The TTL itself is reloadable; this only fixes whether tracking is possible at all.
    const auto cache_policy = settings[FileCacheSetting::cache_policy].value;
    const bool is_overcommit_policy =
        cache_policy == FileCachePolicy::LRU_OVERCOMMIT || cache_policy == FileCachePolicy::SLRU_OVERCOMMIT;
    client_tracking_possible = is_overcommit_policy && write_cache_per_user_directory;
    if (write_cache_per_user_directory && idle_client_ttl_sec.load() > 0 && !is_overcommit_policy)
        LOG_WARNING(log, "idle_client_ttl_sec is set but the cache policy does not track "
                         "per-client usage; idle-client eviction is disabled");

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
    origin.segment_type = system_cache_type.contains(filename.extension().string()) ? FileSegmentKeyType::System : FileSegmentKeyType::Data;
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

bool FileCache::skipCacheOnDiskFailure() const
{
    return skip_cache_on_disk_failure;
}

String FileCache::getFileSegmentPath(const Key & key, size_t offset, FileSegmentKind segment_kind, const OriginInfo & origin, std::optional<size_t> size) const
{
    return metadata.getFileSegmentPath(key, offset, segment_kind, origin, size);
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
            load_metadata_main_thread = std::make_unique<ThreadFromGlobalPool>([this, need_to_load_metadata] { initializeImpl(need_to_load_metadata); });
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
        /// Record per-client accesses for idle-client eviction. Installed whenever
        /// tracking is possible (not gated on the TTL), so the TTL can be toggled live.
        if (client_tracking_possible)
            metadata.setClientAccessCallback([this](const std::string & user_id) { main_priority->touchClientAccess(user_id); });

        /// The single maintenance task (invalidated-entries cleanup + idle eviction).
        /// Publish the wake notifier before loadMetadata can invalidate entries.
        background_cleanup_task = Context::getGlobalContextInstance()->getSchedulePool().createTask(
            StorageID::createEmpty(), "FileCacheBackgroundCleanup", [this] { backgroundCleanupTaskFunc(); });
        main_priority->setInvalidateNotifier(
            invalidated_entries_cleanup_threshold, [this] { background_cleanup_task->schedule(); });
        background_cleanup_task->scheduleAfter(backgroundCleanupIntervalMs());

        if (load_metadata)
            loadMetadata();

        metadata.startup();
    }
    catch (...)
    {
        /// The cleanup task may already be scheduled; stop it so a retried
        /// initialization does not leave an orphaned task rescheduling on `this`.
        if (background_cleanup_task)
            background_cleanup_task->deactivate();
        init_exception = std::current_exception();
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    if (keep_current_size_to_max_ratio != 1 || keep_current_elements_to_max_ratio != 1)
    {
        eviction_pool = std::make_unique<ThreadPool>(
            CurrentMetrics::FilesystemCacheEvictionThreads,
            CurrentMetrics::FilesystemCacheEvictionThreadsActive,
            CurrentMetrics::FilesystemCacheEvictionThreadsScheduled,
            /* max_threads */keep_up_free_space_eviction_threads,
            /* max_free_threads */0,
            /* queue_size */keep_up_free_space_eviction_threads);

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

FileSegments FileCache::getImpl(
    const LockedKey & locked_key, const FileSegment::Range & range, size_t file_segments_limit, bool ignore_bypass_threshold) const
{
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    if (!ignore_bypass_threshold && bypass_cache_threshold && range.size() > bypass_cache_threshold)
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

        bool evicting_or_removed = file_segment_metadata.isEvictingOrRemoved(locked_key);
        fiu_do_on(FailPoints::file_cache_simulate_evicting_segment, { evicting_or_removed = true; });

        FileSegmentPtr file_segment;
        if (evicting_or_removed)
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

    chassert(!file_segments.empty());

    auto it = file_segments.begin();
    size_t processed_count = 0;
    auto segment_range = (*it)->range();

    size_t current_pos = 0;
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

        chassert(current_pos < segment_range.left);

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

    /// The bypass-threshold shortcut is a read-time optimization; on a write there is no remote to
    /// bypass to, so ignore it and let the overlap check inspect the real key contents.
    auto file_segments = getImpl(*locked_key, range, /* file_segments_limit */0, /* ignore_bypass_threshold */true);
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

FileSegmentsHolderPtr FileCache::getDownloadedContiguousOrEmpty(
    const Key & key,
    size_t offset,
    size_t size,
    const UserID & user_id)
{
    assertInitialized();
    chassert(size);

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, OriginInfo(user_id));
    if (!locked_key)
        return std::make_unique<FileSegmentsHolder>();

    const FileSegment::Range range(offset, offset + size - 1);
    /// `ignore_bypass_threshold` = true: temporary data has no remote backing, and this
    /// function must inspect the actual downloaded segments. Without it, a read larger than
    /// `bypass_cache_threshold` (when `enable_bypass_cache_with_threshold` is on) would get a
    /// synthetic DETACHED placeholder from getImpl() and be wrongly reported as missing.
    auto file_segments = getImpl(*locked_key, range, /* file_segments_limit */0, /* ignore_bypass_threshold */true);

    if (file_segments.empty()
        || file_segments.front()->range().left > offset
        || file_segments.back()->range().right < range.right)
        return std::make_unique<FileSegmentsHolder>();

    size_t expected_left = file_segments.front()->range().left;
    for (const auto & file_segment : file_segments)
    {
        /// A hole: part of the range was never written or was already removed.
        if (file_segment->range().left != expected_left)
            return std::make_unique<FileSegmentsHolder>();

        /// The downloaded prefix must reach the needed part of the range; this also rejects
        /// DETACHED placeholders from getImpl() — their downloaded prefix is empty.
        if (file_segment->getCurrentWriteOffset() < std::min(file_segment->range().right + 1, offset + size))
            return std::make_unique<FileSegmentsHolder>();

        expected_left = file_segment->range().right + 1;
    }

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
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
    std::shared_lock lock(dynamic_resize_lock, std::try_to_lock);
    /// Skip priority increase if cache resize is currently in progress.
    /// We cannot do queue moves during dynamic resize.
    if (!lock.owns_lock())
        return false;
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

    /// Skip space reservation if dynamic cache resize is currently in progress.
    /// We cannot do both at the same time.
    std::shared_lock resize_shared_lock(dynamic_resize_lock, std::try_to_lock);
    if (!resize_shared_lock.owns_lock())
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

    const bool success = doTryReserve(
        file_segment, size, reserve_stat, origin_info, lock_wait_timeout_milliseconds,
        failure_reason);
    if (!success)
        ProfileEvents::increment(ProfileEvents::FilesystemCacheFailedReserveAttempts);
    return success;
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

    EvictionCandidates eviction_candidates(main_priority->getOnEvictCallback());
    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;

    FailPointInjection::pauseFailPoint(FailPoints::file_cache_pause_before_do_eviction);

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
        main_eviction_info->releaseHoldSpace(lock);
        if (query_eviction_info)
            query_eviction_info->releaseHoldSpace(lock);

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

    if (auto ec = file_segment.getKeyMetadata()->createBaseDirectory(); ec)
    {
        failure_reason = "Failed to create base directory for key, error: " + ec.message();
        return false;
    }

    return true;
}

bool FileCache::doEviction(
    EvictionInfo & main_eviction_info,
    EvictionInfo * query_eviction_info,
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

        const bool resume_reserve_cursor = cache_reserve_active_threads.load(std::memory_order_relaxed) > 1;
        const auto eviction_cursor = resume_reserve_cursor
            ? IFileCachePriority::EvictionCursor::Reserve
            : IFileCachePriority::EvictionCursor::FromHead;
        if (!resume_reserve_cursor)
            main_priority->resetEvictionPos(IFileCachePriority::EvictionCursor::Reserve);

        if (query_eviction_info && query_eviction_info->requiresEviction())
        {
            chassert(query_priority);
            if (!query_priority->collectCandidatesForEviction(
                    *query_eviction_info,
                    reserve_stat,
                    eviction_candidates,
                    invalidated_entries,
                    /* reservee */{},
                    eviction_cursor,
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
                eviction_cursor,
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

namespace
{
/// Reschedule delays (ms) and the state-lock acquire timeout for background free-space keeping.
constexpr size_t free_space_keeping_reschedule_ms = 5000;
constexpr size_t free_space_keeping_retry_reschedule_ms = 1000;
constexpr size_t free_space_keeping_state_lock_timeout_ms = 1000;

/// A batch handed to a remover: file segments to delete, plus zero-size queue entries to drop.
struct EvictionBatch
{
    EvictionCandidatesPtr candidates;
    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;
};
using EvictionBatchPtr = std::shared_ptr<EvictionBatch>;
}

void FileCache::freeSpaceRatioKeepingThreadFunc()
{
    if (shutdown)
        return;

    while (fiu_fail(FailPoints::file_cache_stall_free_space_ratio_keeping_thread))
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    Stopwatch watch;
    size_t reschedule_ms = free_space_keeping_reschedule_ms;

    /// The task must never throw out: otherwise it stops being rescheduled and background
    /// keeping dies for the lifetime of the cache.
    try
    {
        freeSpaceRatioImpl(reschedule_ms);
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadErrors);
        tryLogCurrentException(log, "Error in free space ratio keeping thread");
    }

    watch.stop();
    ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds, watch.elapsedMilliseconds());

    [[maybe_unused]] bool scheduled = keep_up_free_space_ratio_task->scheduleAfter(reschedule_ms);
    chassert(scheduled);
}

std::unique_ptr<EvictionInfo> FileCache::collectFreeSpaceEvictionInfo(
    const CacheStateGuard::Lock & lock, size_t in_flight_size, size_t in_flight_elements)
{
    const size_t size_limit = main_priority->getSizeLimit(lock);
    const size_t elements_limit = main_priority->getElementsLimit(lock);

    const size_t desired_size = std::lround(keep_current_size_to_max_ratio * static_cast<double>(size_limit));
    const size_t desired_elements = std::lround(keep_current_elements_to_max_ratio * static_cast<double>(elements_limit));

    /// Discount what is already collected: its space will be freed once the in-flight removals finalize.
    const size_t current_size = main_priority->getSize(lock);
    const size_t current_elements = main_priority->getElementsCount(lock);
    const size_t projected_size = current_size > in_flight_size ? current_size - in_flight_size : 0;
    const size_t projected_elements = current_elements > in_flight_elements ? current_elements - in_flight_elements : 0;

    const size_t size_to_evict = projected_size > desired_size ? projected_size - desired_size : 0;
    const size_t elements_to_evict = projected_elements > desired_elements ? projected_elements - desired_elements : 0;

    if (!size_to_evict && !elements_to_evict)
        return nullptr;

    auto eviction_info = main_priority->collectEvictionInfo(
        size_to_evict, elements_to_evict, /* reservee */nullptr,
        /* is_total_space_cleanup */true, getInternalOrigin(), lock);

    chassert(eviction_info);
    chassert(!eviction_info->hasHoldSpace());
    chassert(eviction_info->requiresEviction());
    return eviction_info;
}

void FileCache::freeSpaceRatioImpl(size_t & reschedule_ms)
{
    /// First eviction info, collected against the live size (nothing in flight yet).
    /// Kept and reused for the first loop iteration below instead of being recomputed.
    std::unique_ptr<EvictionInfo> eviction_info;
    {
        auto lock = cache_state_guard.tryLockFor(std::chrono::milliseconds(free_space_keeping_state_lock_timeout_ms));
        if (!lock)
        {
            reschedule_ms = free_space_keeping_retry_reschedule_ms;
            return;
        }
        eviction_info = collectFreeSpaceEvictionInfo(lock, 0, 0);
    }
    if (!eviction_info)
        return;

    ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadRun);

    /// Outcome of the drain loop below, reported in the final log line:
    /// - SUCCESS: drained until nothing more needs to be evicted;
    /// - CANNOT_EVICT: bailed out early (could not acquire the state lock, or shutdown).
    auto status = IFileCachePriority::CollectStatus::SUCCESS;

    const size_t queue_capacity = std::max<size_t>(2 * keep_up_free_space_eviction_threads, 2);
    /// Pipeline: the collector (this thread) pushes candidate batches to pending_eviction_queue;
    /// N removers evict them (delete files) and forward to pending_finalization_queue, which the
    /// collector drains and finalizes.
    ConcurrentBoundedQueue<EvictionBatchPtr> pending_eviction_queue(queue_capacity);
    ConcurrentBoundedQueue<EvictionBatchPtr> pending_finalization_queue(queue_capacity);

    std::atomic<size_t> running_removers = 0;
    bool removers_scheduled = false;

    /// Collected but not yet finalized (full batch sizes);
    /// discounted from the live size when deciding how much more to evict.
    size_t in_flight_size = 0;
    size_t in_flight_elements = 0;
    /// Actually removed from disk (failures excluded); for stats only.
    size_t evicted_size = 0;
    size_t evicted_elements = 0;

    auto finalize_removed = [&](bool blocking)
    {
        EvictionBatchPtr batch;
        while (blocking ? pending_finalization_queue.pop(batch) : pending_finalization_queue.tryPop(batch))
        {
            /// Subtract the full footprint, failures included: at this point we know what failed, and
            /// those bytes are still in `current_size` (their queue entries were never invalidated),
            /// so dropping them from `in_flight` puts them back into the projection and the next pass
            /// re-targets them. While a batch is in flight its failures are unknown, so the projection
            /// is optimistic for at most one iteration; the periodic reschedule self-corrects.
            in_flight_size -= batch->candidates->bytes();
            in_flight_elements -= batch->candidates->size();
            try
            {
                {
                    auto lock = cache_guard.writeLock();
                    batch->candidates->afterEvictWrite(lock);
                    IFileCachePriority::removeEntries(batch->invalidated_entries, lock);
                }
                batch->candidates->afterEvictState(cache_state_guard.lock());

                /// Per-segment eviction metrics are updated in `onSegmentEvictedInTheBackground`.
                /// Here we only sum the actually removed amount (failures excluded) for the log.
                const auto failed = batch->candidates->getFailedCandidates();
                evicted_size += batch->candidates->bytes() - failed.total_cache_size;
                evicted_elements += batch->candidates->size() - failed.total_cache_elements;
            }
            catch (...)
            {
                ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadErrors);
                tryLogCurrentException(log, "Failed to finalize evicted file segments");
                chassert(false);
            }
        }
    };

    try
    {
        for (size_t i = 0; i < keep_up_free_space_eviction_threads; ++i)
        {
            eviction_pool->scheduleOrThrowOnError([&]
            {
                try
                {
                    EvictionBatchPtr batch;
                    while (pending_eviction_queue.pop(batch))
                    {
                        try
                        {
                            batch->candidates->evict();
                        }
                        catch (...)
                        {
                            ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadErrors);
                            tryLogCurrentException(log, "Failed to evict file segments in background");
                        }
                        if (!pending_finalization_queue.push(std::move(batch)))
                        {
                            /// Unreachable: the queue is finished only by the last remover to exit,
                            /// and a remover inside push() is still counted in `running_removers`.
                            /// If reached, the batch destructor invalidates the evicted entries.
                            chassert(false);
                            break;
                        }
                    }
                }
                catch (...)
                {
                    ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadErrors);
                    tryLogCurrentException(log, "Background eviction remover failed");
                }

                /// The last remover to exit closes the result queue so the collector's drain ends.
                if (running_removers.fetch_sub(1) == 1)
                    pending_finalization_queue.finish();
            });
            ++running_removers;
            removers_scheduled = true;
        }

        main_priority->resetEvictionPos(IFileCachePriority::EvictionCursor::Background);

        while (!shutdown)
        {
            /// On the first iteration `eviction_info` is the one collected above; afterwards it is
            /// recomputed against the live size, discounting what is already in flight.
            if (!eviction_info)
            {
                /// Finalize what the removers have deleted, then re-evaluate against the live size.
                finalize_removed(/* blocking */false);

                auto lock = cache_state_guard.tryLockFor(std::chrono::milliseconds(free_space_keeping_state_lock_timeout_ms));
                if (!lock)
                {
                    reschedule_ms = free_space_keeping_retry_reschedule_ms;
                    status = IFileCachePriority::CollectStatus::CANNOT_EVICT;
                    break;
                }
                eviction_info = collectFreeSpaceEvictionInfo(lock, in_flight_size, in_flight_elements);
                if (!eviction_info)
                    break;
            }

            LOG_TRACE(log, "Collecting eviction candidates to keep free space ({}), in flight: {}/{} (size/elements)",
                      eviction_info->toString(), in_flight_size, in_flight_elements);

            auto batch = std::make_shared<EvictionBatch>();
            batch->candidates = std::make_unique<EvictionCandidates>(getOnBackgroundEvictCallback());
            FileCacheReserveStat stat;
            main_priority->collectCandidatesForEviction(
                *eviction_info, stat, *batch->candidates, batch->invalidated_entries,
                /* reservee */nullptr, IFileCachePriority::EvictionCursor::Background,
                /* max_candidates_size */keep_up_free_space_remove_batch,
                /* is_total_space_cleanup */true, getInternalOrigin(),
                cache_guard, cache_state_guard);

            /// Consumed: the next iteration recomputes against the updated live size.
            eviction_info.reset();

            const size_t batch_size = batch->candidates->size();
            if (batch_size == 0)
            {
                /// Zero-size (already invalidated) entries have no file to delete.
                if (!batch->invalidated_entries.empty())
                {
                    IFileCachePriority::removeEntries(batch->invalidated_entries, cache_guard.writeLock());
                    continue;
                }
                /// Eviction was required (`eviction_info` was non-null) but nothing could be
                /// collected: the cache is still above the target with no releasable candidates.
                /// Report CANNOT_EVICT so the final log line does not claim the target was drained.
                status = IFileCachePriority::CollectStatus::CANNOT_EVICT;
                break;
            }

            const size_t batch_bytes = batch->candidates->bytes();

            /// Keep finalizing while waiting for room, so removers never block on a full queue.
            constexpr size_t push_timeout_ms = 10;
            constexpr size_t max_push_attempts = 1000;
            bool pushed = false;
            bool force_push_fail = false;
            fiu_do_on(FailPoints::file_cache_background_eviction_push_fail, { force_push_fail = true; });
            for (size_t attempt = 0; !force_push_fail && !pushed && attempt < max_push_attempts; ++attempt)
            {
                pushed = pending_eviction_queue.tryPush(batch, push_timeout_ms);
                if (!pushed)
                    finalize_removed(/* blocking */false);
            }
            if (!pushed)
            {
                LOG_WARNING(
                    log, "Background eviction workers take too much time to evict "
                    "(max_push_attempts: {}, push_timeout_ms: {})",
                    max_push_attempts, push_timeout_ms);

                status = IFileCachePriority::CollectStatus::CANNOT_EVICT;
                reschedule_ms = free_space_keeping_retry_reschedule_ms;
                break;
            }

            in_flight_size += batch_bytes;
            in_flight_elements += batch_size;
        }
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadErrors);
        tryLogCurrentException(log, "Error while collecting background eviction candidates");
        status = IFileCachePriority::CollectStatus::CANNOT_EVICT;
    }

    /// Interrupted by shutdown before the drain could complete.
    if (shutdown)
        status = IFileCachePriority::CollectStatus::CANNOT_EVICT;

    /// Let the removers finish the queued batches, finalize all of them, then join.
    pending_eviction_queue.finish();
    if (!removers_scheduled)
        pending_finalization_queue.finish();
    finalize_removed(/* blocking */true);
    eviction_pool->wait();

    LOG_TRACE(log, "Free space ratio keeping thread finished with status `{}`, evicted {} file segments ({} bytes)",
              status, evicted_elements, evicted_size);

    assertCacheCorrectness();
}

UInt64 FileCache::backgroundCleanupIntervalMs() const
{
    /// Invalidated-entries interval, shrunk to the idle-client check cadence
    /// (default `ttl/10`) while idle-client eviction is enabled.
    const auto ttl_sec = idle_client_ttl_sec.load();
    if (ttl_sec == 0)
        return invalidated_entries_cleanup_interval_ms;
    const auto check_sec = idle_client_check_interval_sec.load();
    const auto interval_sec = check_sec > 0 ? check_sec : std::max<UInt64>(1, ttl_sec / 10);
    return std::min<UInt64>(invalidated_entries_cleanup_interval_ms, interval_sec * 1000);
}

void FileCache::backgroundCleanupTaskFunc()
{
    size_t removed = 0;
    {
        Stopwatch watch;
        try
        {
            removed = main_priority->removeInvalidatedEntries(invalidated_entries_cleanup_remove_batch, cache_guard);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        ProfileEvents::increment(
            ProfileEvents::FilesystemCacheInvalidatedEntriesCleanupThreadWorkMilliseconds, watch.elapsedMilliseconds());
    }

    /// Idle-client eviction shares this thread; self-throttled to its own interval.
    /// Possible only for client-tracking (overcommit) policies; the live TTL gates it.
    if (client_tracking_possible)
    {
        try
        {
            evictIdleClients();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (shutdown || !background_cleanup_task)
        return;

    /// A full batch likely means there is more to clean: come back immediately.
    if (removed == invalidated_entries_cleanup_remove_batch)
        background_cleanup_task->schedule();
    else
        background_cleanup_task->scheduleAfter(backgroundCleanupIntervalMs());
}

void FileCache::evictIdleClients()
{
    /// Wait for `is_initialized`: the cleanup task is scheduled before loadMetadata,
    /// and purging a client mid-load could race a key still being reconstructed.
    if (!is_initialized || shutdown)
        return;

    /// TTL is reloadable: zero means eviction is currently disabled. Access tracking
    /// keeps running, so last-access stays fresh if it is re-enabled later.
    const auto ttl_sec = idle_client_ttl_sec.load();
    if (ttl_sec == 0)
        return;

    /// Self-throttle to the check interval: the cleanup task can fire more often.
    /// Runs only on the cleanup-task thread, so `last_idle_eviction` needs no sync.
    const auto now = std::chrono::steady_clock::now();
    const auto configured = idle_client_check_interval_sec.load();
    const std::chrono::seconds check_interval(configured > 0 ? configured : std::max<UInt64>(1, ttl_sec / 10));
    if (last_idle_eviction.time_since_epoch().count() != 0 && now - last_idle_eviction < check_interval)
        return;
    last_idle_eviction = now;

    const std::chrono::seconds ttl(ttl_sec);
    const auto idle_clients = main_priority->collectIdleClients(ttl);
    if (idle_clients.empty())
        return;

    /// Purge clients in parallel: `removeAllKeys` scans all metadata buckets, so a
    /// large idle set would otherwise serialize behind one another.
    const auto check_now = std::chrono::steady_clock::now();
    std::atomic<size_t> next_index = 0;
    auto purge_worker = [&]
    {
        for (size_t i = next_index.fetch_add(1, std::memory_order_relaxed);
             i < idle_clients.size() && !shutdown;
             i = next_index.fetch_add(1, std::memory_order_relaxed))
        {
            const auto & [user_id, usage] = idle_clients[i];
            try
            {
                /// Re-check via the carried usage to skip clients touched since the scan.
                if (!usage->idleFor(ttl, check_now))
                    continue;

                if (usage->total_size == 0)
                    continue;

                /// Report success only on a full purge; held segments are retried next sweep.
                if (metadata.removeAllKeys(user_id))
                {
                    ProfileEvents::increment(ProfileEvents::FilesystemCacheIdleClientEvictions);
                    LOG_INFO(log, "Purged cache for idle client '{}' (idle for >= {} sec)", user_id, ttl_sec);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to purge idle client cache");
            }
        }
    };

    /// The current (cleanup-task) thread participates, so spawn one fewer worker.
    const size_t num_extra_threads = std::min<size_t>(idle_clients.size(), idle_client_eviction_threads) - 1;
    std::vector<ThreadFromGlobalPool> threads;
    try
    {
        threads.reserve(num_extra_threads);
        for (size_t t = 0; t < num_extra_threads; ++t)
            threads.emplace_back(purge_worker);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to spawn idle client eviction threads");
    }
    purge_worker();
    for (auto & thread : threads)
        thread.join();
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
    metadata.removeKey(key, /* if_exists */false, user_id);
}

void FileCache::removeKeyIfExists(const Key & key, const UserID & user_id)
{
    assertInitialized();
    metadata.removeKey(key, /* if_exists */true, user_id);
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

    metadata.removeAllKeys(user_id);
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

            const std::string key_prefix_dir_name = path.filename();
            if (key_prefix_it->is_directory() &&
                key_prefix_dir_name != getKeyTypePrefix(FileSegmentKeyType::Data) &&
                key_prefix_dir_name != getKeyTypePrefix(FileSegmentKeyType::System)
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

    const UInt64 num_listing_threads = std::max(UInt64(1), load_metadata_threads / 2);
    const UInt64 num_loading_threads = load_metadata_threads - num_listing_threads;

    LOG_INFO(log, "Loading filesystem cache from {} using {} listing thread(s) and {} loading thread(s)",
             metadata.getBaseDirectory(), num_listing_threads, num_loading_threads);

    if (write_cache_per_user_directory && use_split_cache)
        LOG_WARNING(log, "use_split_cache currently unsupported with write_cache_per_user_directory. Will ignore use_split_cache.");

    /// Bounded queue of individual key directories fed by listing threads, drained by loading threads.
    /// Size 0 when there are no loading threads: tryPush always fails immediately,
    /// so listing threads load all keys directly.
    ConcurrentBoundedQueue<std::pair<fs::path, OriginInfo>> key_dirs_queue(num_loading_threads == 0 ? 0 : 1000);

    std::exception_ptr first_exception;
    std::mutex exception_mutex;
    /// Tracks how many listing threads are still running.
    std::atomic<UInt64> listing_threads_remaining{num_listing_threads};

    auto handle_exception = [&]()
    {
        std::lock_guard lock(exception_mutex);
        if (!first_exception)
            first_exception = std::current_exception();
        stop_loading_metadata = true;
        key_dirs_queue.finish();
    };

    /// Load enqueued key directories until the queue is exhausted and finished.
    /// pop() blocks when the queue is empty until either a new item is pushed by a listing
    /// thread or finish() is called (after all listing threads stopped producing).
    auto drain_queue = [&]()
    {
        std::pair<fs::path, OriginInfo> item;
        while (key_dirs_queue.pop(item))
        {
            if (stop_loading_metadata)
                return;
            try
            {
                loadMetadataForKey(item.first, item.second);
            }
            catch (...)
            {
                handle_exception();
                return;
            }
        }
    };

    /// Listing threads: each picks up key_prefix_dirs in parallel and enqueues individual key dirs.
    /// Once all prefixes are listed, the last listing thread calls finish() on the queue, and every
    /// listing thread then joins the loading threads in draining it (so all threads load the - usually
    /// dominant - remaining keys instead of sitting idle once listing is done).
    std::vector<ThreadFromGlobalPool> listing_threads;
    for (UInt64 i = 0; i < num_listing_threads; ++i)
    {
        try
        {
            listing_threads.emplace_back([&]
            {
                while (!stop_loading_metadata)
                {
                    try
                    {
                        std::optional<std::pair<fs::path, OriginInfo>> prefix_result;
                        if (write_cache_per_user_directory)
                            prefix_result = get_keys_dir_to_process_with_user_dir();
                        else
                            prefix_result = get_keys_dir_to_process();

                        if (!prefix_result.has_value())
                            break;

                        const auto & [key_prefix_dir, origin] = prefix_result.value();

                        fs::directory_iterator key_it{key_prefix_dir};
                        if (key_it == fs::directory_iterator{})
                        {
                            LOG_DEBUG(log, "Removing empty key prefix directory: {}", key_prefix_dir.string());
                            fs::remove(key_prefix_dir);
                            continue;
                        }

                        for (; key_it != fs::directory_iterator(); ++key_it)
                        {
                            if (stop_loading_metadata)
                                break;
                            if (!key_it->is_directory())
                            {
                                LOG_DEBUG(log, "Unexpected file: {} (not a directory). Expected a directory", key_it->path().string());
                                continue;
                            }
                            const auto key_dir_path = key_it->path();
                            /// tryPush returns false if the queue is full (timeout=0) or finish() was called.
                            /// Distinguish the two by checking stop_loading_metadata.
                            /// If the queue is full, load directly instead of waiting.
                            if (!key_dirs_queue.tryPush({key_dir_path, origin}))
                            {
                                if (stop_loading_metadata)
                                    break;
                                loadMetadataForKey(key_dir_path, origin);
                            }
                        }
                    }
                    catch (...)
                    {
                        handle_exception();
                    }
                }

                if (listing_threads_remaining.fetch_sub(1) == 1)
                    key_dirs_queue.finish();

                /// Listing is done for this thread; help load the remaining enqueued keys.
                drain_queue();
            });
        }
        catch (...)
        {
            handle_exception();
            break;
        }
    }

    /// Loading threads: drain the queue and load individual key dirs.
    std::vector<ThreadFromGlobalPool> loading_threads;
    for (UInt64 i = 0; i < num_loading_threads; ++i)
    {
        try
        {
            loading_threads.emplace_back([&] { drain_queue(); });
        }
        catch (...)
        {
            handle_exception();
            break;
        }
    }

    for (auto & thread : listing_threads)
        if (thread.joinable())
            thread.join();
    for (auto & thread : loading_threads)
        if (thread.joinable())
            thread.join();

    if (first_exception)
        std::rethrow_exception(first_exception);

    main_priority->check(cache_state_guard.lock());

    assertCacheCorrectness();
}

void FileCache::loadMetadataForKey(const fs::path & key_directory, const OriginInfo & origin_info)
{
    /// Open the directory once and reuse the iterator for the scan below.
    /// `fs::is_empty` would open the directory just to test for emptiness,
    /// which costs a redundant opendir/readdir per key directory.
    fs::directory_iterator offset_it{key_directory};
    if (offset_it == fs::directory_iterator())
    {
        LOG_DEBUG(log, "Removing empty key directory: {}", key_directory.string());
        fs::remove(key_directory);
        return;
    }

    const auto key = Key::fromKeyString(key_directory.filename().string());
    auto key_metadata = metadata.getKeyMetadata(
        key,
        CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY,
        origin_info,
        /* is_initial_load */true);

    /// Phase 1: scan and parse all segment files for this key (no lock held).
    struct SegmentToLoad
    {
        UInt64 offset;
        UInt64 size;
        FileSegmentKind kind;
        fs::path path;
        IFileCachePriority::IteratorPtr cache_it; /// filled in phase 2
        bool size_in_filename; /// whether the size was read from the file name (no `stat` needed)
    };
    std::vector<SegmentToLoad> segments;
    /// Deduplicate by offset. Encoding the size in the name (`<offset>` -> `<offset>_<size>`, see
    /// `renameToIncludeSizeInNameUnlocked`) is best-effort: if the target name is already occupied the
    /// rename fails and the real segment stays under its legacy `<offset>` name next to a stale
    /// `<offset>_<size>` artifact. Both parse to the same offset, so without deduplication we would
    /// build two priority entries for one offset and later hit the duplicate-offset path in phase 3
    /// (a `chassert(false)` in debug builds; a nondeterministic deletion of the real file in release).
    std::unordered_map<UInt64, size_t> offset_to_index;

    for (; offset_it != fs::directory_iterator(); ++offset_it)
    {
        /// Only regular files hold cache segments. A rename target occupied by a non-regular entry
        /// (e.g. a leftover directory, as exercised by `RenameToIncludeSizeInNameFailureKeepsSegmentConsistent`)
        /// is exactly what makes the best-effort rename fail; skip it here so its name is never parsed
        /// and trusted as a segment (the size of a `<offset>_<size>` entry is read from the name without
        /// a `stat`). Uses the directory-entry type cached by `directory_iterator` (no extra syscall on
        /// local filesystems, which always report the entry type).
        if (!offset_it->is_regular_file())
        {
            LOG_WARNING(log, "Unexpected non-regular entry in cache directory: {}", offset_it->path().string());
            continue;
        }

        auto offset_with_suffix = offset_it->path().filename().string();
        bool parsed = false;
        UInt64 offset = 0;
        std::optional<UInt64> size_from_name;

        auto delim_pos = offset_with_suffix.find('_');
        if (delim_pos == std::string::npos)
        {
            parsed = tryParse<UInt64>(offset, offset_with_suffix);
        }
        else
        {
            parsed = tryParse<UInt64>(offset, offset_with_suffix.substr(0, delim_pos));

            const auto suffix = offset_with_suffix.substr(delim_pos + 1);
            if (suffix == "persistent")
            {
                /// For compatibility. Persistent files are no longer supported.
                fs::remove(offset_it->path());
                continue;
            }
            if (suffix == "temporary")
            {
                fs::remove(offset_it->path());
                continue;
            }

            /// New format `<offset>_<size>`: the size is encoded in the name, so we can
            /// load this segment without a `stat` syscall.
            UInt64 size_value = 0;
            if (parsed && tryParse<UInt64>(size_value, suffix))
                size_from_name = size_value;
            else
                parsed = false;
        }

        if (!parsed)
        {
            LOG_WARNING(log, "Unexpected file: {}", offset_it->path().string());
            continue;
        }

        /// Legacy `<offset>` files (and files left by a download that was interrupted before
        /// the size was encoded into the name) require a `stat` to obtain the size.
        UInt64 size = size_from_name.has_value() ? *size_from_name : offset_it->file_size();
        if (!size)
        {
            fs::remove(offset_it->path());
            continue;
        }

        /// If two names map to the same offset (a legacy `<offset>` next to a stale `<offset>_<size>`
        /// left by a failed best-effort rename), keep the legacy file — its size comes from a real
        /// `stat`, and it is the segment whose rename failed — and ignore the suffixed duplicate,
        /// whose size is trusted from the name. Leaving the stale artifact on disk is deliberate:
        /// removing it here would be a destructive action on this recovery path.
        if (auto [it, is_new] = offset_to_index.try_emplace(offset, segments.size()); !is_new)
        {
            auto & existing = segments[it->second];
            const bool replace_existing = !size_from_name.has_value() && existing.size_in_filename;
            LOG_WARNING(
                log,
                "Duplicate cache files for offset {}: keeping '{}', ignoring '{}'",
                offset,
                replace_existing ? offset_it->path().string() : existing.path.string(),
                replace_existing ? existing.path.string() : offset_it->path().string());
            if (replace_existing)
                existing = {offset, size, FileSegmentKind::Regular, offset_it->path(), nullptr, /* size_in_filename */false};
            continue;
        }

        segments.push_back({offset, size, FileSegmentKind::Regular, offset_it->path(), nullptr, size_from_name.has_value()});
    }

    /// Phase 2: add all segments for the key under a single write lock acquisition.
    /// TODO: we can get rid of this lockCache() if we first load everything in parallel
    /// without any mutual lock between loading threads, and only after do removeOverflow().
    /// This will be better because overflow here may
    /// happen only if cache configuration changed and max_size became less than it was.
    size_t size_limit = 0;
    {
        auto lock = cache_guard.writeLock();
        auto state_lock = cache_state_guard.lock();
        size_limit = main_priority->getSizeLimit(state_lock);

        for (auto & segment : segments)
        {
            if (main_priority->canFit(
                    segment.size,
                    /* elements */1,
                    state_lock,
                    /* reservee */nullptr,
                    origin_info,
                    /* is_initial_load */true))
            {
                segment.cache_it = main_priority->add(
                    key_metadata,
                    segment.offset,
                    segment.size,
                    lock,
                    &state_lock,
                    /* is_initial_load */true);
            }
        }
    }

    /// Phase 3: construct FileSegment objects and emplace
    /// (no lock held, because a single key is loaded by a single thread).
    size_t failed_to_fit = 0;
    for (auto & segment : segments)
    {
        if (segment.cache_it)
        {
            bool inserted = false;
            try
            {
                auto file_segment = std::make_shared<FileSegment>(
                    key,
                    segment.offset,
                    segment.size,
                    FileSegment::State::DOWNLOADED,
                    CreateFileSegmentSettings(segment.kind),
                    /* background_download_enabled */false,
                    this,
                    key_metadata,
                    segment.cache_it,
                    /* size_in_filename */segment.size_in_filename);

                inserted = key_metadata->emplaceUnlocked(segment.offset, std::make_shared<FileSegmentMetadata>(std::move(file_segment))).second;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                chassert(false);
            }

            if (inserted)
            {
                LOG_TEST(log, "Added file segment {}:{} (size: {}) with path: {}", key, segment.offset, segment.size, segment.path.string());
            }
            else
            {
                segment.cache_it->remove(cache_guard.writeLock());
                fs::remove(segment.path);
                chassert(false);
            }
        }
        else
        {
            ++failed_to_fit;
            fs::remove(segment.path);
        }
    }

    if (failed_to_fit)
    {
        LOG_WARNING(
            log,
            "Cache capacity changed (max size: {}), "
            "{} file(s) for key {} do not fit in cache anymore",
            size_limit, failed_to_fit, key);
    }

    if (key_metadata->sizeUnlocked() == 0)
    {
        metadata.removeKey(key, /* if_exists */false, origin_info.user_id);
    }
}

FileCache::~FileCache()
{
    deactivateBackgroundOperations();
    assertCacheCorrectness();
}

void FileCache::onSegmentEvicted(const FileSegment & segment, const String & user_id) const
{
    ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedFileSegments);
    ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedBytes, segment.getReservedSize());

    if (!expose_eviction_metrics.load(std::memory_order_relaxed))
        return;

    const size_t bytes = segment.getReservedSize();
    const size_t hits = segment.getHitsCount();
    filesystem_cache_evictions_total.withLabels({name}).increment();
    filesystem_cache_evicted_bytes_total.withLabels({name}).increment(static_cast<DimensionalMetrics::Value>(bytes));
    filesystem_cache_evicted_segment_hits.withLabels({name}).observe(static_cast<HistogramMetrics::Value>(hits));
    filesystem_cache_evicted_segment_size_bytes.withLabels({name}).observe(static_cast<HistogramMetrics::Value>(bytes));

    if (!expose_eviction_metrics_per_user.load(std::memory_order_relaxed))
        return;
    filesystem_cache_evictions_by_user_total.withLabels({name, user_id}).increment();
    filesystem_cache_evicted_bytes_by_user_total.withLabels({name, user_id}).increment(static_cast<DimensionalMetrics::Value>(bytes));
    filesystem_cache_evicted_segment_hits_by_user.withLabels({name, user_id}).observe(static_cast<HistogramMetrics::Value>(hits));
    filesystem_cache_evicted_segment_size_bytes_by_user.withLabels({name, user_id}).observe(static_cast<HistogramMetrics::Value>(bytes));
}

IFileCachePriority::OnEvictCallback FileCache::getOnBackgroundEvictCallback() const
{
    return std::bind_front(&FileCache::onSegmentEvictedInTheBackground, this);
}

void FileCache::onSegmentEvictedInTheBackground(const FileSegment & segment, const String & user_id) const
{
    ProfileEvents::increment(ProfileEvents::FilesystemCacheBackgroundEvictedFileSegments);
    ProfileEvents::increment(ProfileEvents::FilesystemCacheBackgroundEvictedBytes, segment.getReservedSize());
    onSegmentEvicted(segment, user_id);
}

void FileCache::deactivateBackgroundOperations()
{
    shutdown.store(true);

    stop_loading_metadata = true;
    if (load_metadata_main_thread && load_metadata_main_thread->joinable())
        load_metadata_main_thread->join();

    if (keep_up_free_space_ratio_task)
        keep_up_free_space_ratio_task->deactivate();
    /// The single maintenance task: invalidated-entries cleanup + idle-client eviction.
    if (background_cleanup_task)
        background_cleanup_task->deactivate();

    /// The task is stopped, so no new eviction jobs can be scheduled - drain the rest.
    if (eviction_pool)
        eviction_pool->wait();

    metadata.shutdown();
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

    if (new_settings[FileCacheSetting::reserve_granularity] != actual_settings[FileCacheSetting::reserve_granularity])
    {
        reserve_granularity.store(new_settings[FileCacheSetting::reserve_granularity], std::memory_order_relaxed);

        LOG_INFO(log, "Changed reserve_granularity from {} to {}",
                actual_settings[FileCacheSetting::reserve_granularity].value,
                new_settings[FileCacheSetting::reserve_granularity].value);

        actual_settings[FileCacheSetting::reserve_granularity] = new_settings[FileCacheSetting::reserve_granularity];
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

    if (new_settings[FileCacheSetting::idle_client_ttl_sec] != actual_settings[FileCacheSetting::idle_client_ttl_sec])
    {
        const UInt64 new_ttl = new_settings[FileCacheSetting::idle_client_ttl_sec];
        const bool was_enabled = idle_client_ttl_sec.load() > 0;
        idle_client_ttl_sec.store(new_ttl);

        LOG_INFO(log, "Changed idle_client_ttl_sec from {} to {}",
                 actual_settings[FileCacheSetting::idle_client_ttl_sec].value, new_ttl);
        actual_settings[FileCacheSetting::idle_client_ttl_sec] = new_settings[FileCacheSetting::idle_client_ttl_sec];

        if (new_ttl > 0 && !client_tracking_possible)
            LOG_WARNING(log, "idle_client_ttl_sec is set but the cache policy does not track "
                             "per-client usage; idle-client eviction is disabled");
        /// Just enabled: wake the task so the first sweep does not wait a full interval.
        else if (new_ttl > 0 && !was_enabled && client_tracking_possible && background_cleanup_task)
            background_cleanup_task->schedule();
    }

    if (new_settings[FileCacheSetting::idle_client_check_interval_sec] != actual_settings[FileCacheSetting::idle_client_check_interval_sec])
    {
        idle_client_check_interval_sec.store(new_settings[FileCacheSetting::idle_client_check_interval_sec]);

        LOG_INFO(log, "Changed idle_client_check_interval_sec from {} to {}",
                 actual_settings[FileCacheSetting::idle_client_check_interval_sec].value,
                 new_settings[FileCacheSetting::idle_client_check_interval_sec].value);
        actual_settings[FileCacheSetting::idle_client_check_interval_sec] = new_settings[FileCacheSetting::idle_client_check_interval_sec];
    }

    if (new_settings[FileCacheSetting::expose_prometheus_eviction_metrics] != actual_settings[FileCacheSetting::expose_prometheus_eviction_metrics])
    {
        expose_eviction_metrics.store(new_settings[FileCacheSetting::expose_prometheus_eviction_metrics], std::memory_order_relaxed);
        actual_settings[FileCacheSetting::expose_prometheus_eviction_metrics] = new_settings[FileCacheSetting::expose_prometheus_eviction_metrics];
    }

    if (new_settings[FileCacheSetting::expose_prometheus_eviction_metrics_per_user] != actual_settings[FileCacheSetting::expose_prometheus_eviction_metrics_per_user])
    {
        expose_eviction_metrics_per_user.store(new_settings[FileCacheSetting::expose_prometheus_eviction_metrics_per_user], std::memory_order_relaxed);
        actual_settings[FileCacheSetting::expose_prometheus_eviction_metrics_per_user] = new_settings[FileCacheSetting::expose_prometheus_eviction_metrics_per_user];
    }
}

FileCache::SizeLimits FileCache::doDynamicResize(const SizeLimits & prev_limits, const SizeLimits & desired_limits)
{
    if (prev_limits.slru_size_ratio != desired_limits.slru_size_ratio)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dynamic resize of size ratio is not allowed");

    std::unique_lock resize_lock(dynamic_resize_lock, std::defer_lock);
    if (!resize_lock.try_lock_for(std::chrono::milliseconds(dynamic_resize_lock_wait_ms)))
    {
        LOG_WARNING(log, "Dynamic resize skipped: could not acquire resize lock within {}ms",
                    dynamic_resize_lock_wait_ms);
        return prev_limits;
    }

    SizeLimits result_limits;
    bool modified_size_limit = false;
    {
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

    EvictionCandidates eviction_candidates(main_priority->getOnEvictCallback());
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
            IFileCachePriority::EvictionCursor::FromHead,
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

    /// Restore to previous limits. Using prev_limits is safe because
    /// the entries existed under those limits before the resize attempt,
    /// so each sub-queue had enough room. Computing a tighter bound
    /// (desired + failed) would be incorrect for SLRU: when all failed
    /// entries belong to one sub-queue (e.g. protected with ratio 0.6),
    /// the total might not translate into enough per-sub-queue space
    /// after the ratio split.
    result_limits = prev_limits;

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
            /// Restore the original queue entry size. For partial segments it is reserved size.
            const auto restored_size = candidate->size();

            LOG_DEBUG(
                log, "Adding back file segment after failed eviction: {}:{}, restored size: {}, downloaded size: {}",
                file_segment->key(), file_segment->offset(), restored_size, file_segment->getDownloadedSize());

            auto original_queue_type = eviction_candidates.getOriginalQueueType(candidate.get());

            auto main_priority_iterator = main_priority->addForRestore(
                key_metadata,
                file_segment->offset(),
                restored_size,
                original_queue_type,
                cache_write_lock,
                &state_lock);

            candidate->setRemovedFlag(*locked_key, /* value */false);
            file_segment->restoreQueueIteratorAfterDelayedRemoval(main_priority_iterator);
        }
    }

    return false;
}

FileCache::QueryContextHolderPtr FileCache::getQueryContextHolder(
    const String & query_id, const FilesystemCacheSettings & cache_settings)
{
    if (!query_limit || cache_settings.max_download_size_per_query == 0)
        return {};

    auto lock = cache_guard.writeLock();
    auto context = query_limit->getOrSetQueryContext(query_id, cache_settings, lock);
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
