#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Common/SipHash.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueOrderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueUnorderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueExclusiveFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueFilenameParser.h>
#include <Storages/StorageSnapshot.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <Common/DimensionalMetrics.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/FailPoint.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/randomSeed.h>
#include <Common/DNSResolver.h>
#include <Interpreters/DDLTask.h>
#include <shared_mutex>
#include <Core/ServerUUID.h>
#include <Core/UUID.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueueCleanupMaxSetSizeOrTTLMicroseconds;
};

namespace CurrentMetrics
{
    extern const Metric ObjectStorageQueueRegisteredServers;
    extern const Metric ObjectStorageQueueMetadataCacheSizeBytes;
    extern const Metric ObjectStorageQueueMetadataCacheSizeElements;
};

namespace DimensionalMetrics
{
    extern MetricFamily & ObjectStorageQueueNewestSeenTimestamp;
    extern MetricFamily & ObjectStorageQueueNewestCommittedTimestamp;
}

namespace DB
{

namespace FailPoints
{
    extern const char object_storage_queue_pause_before_cleanup_lock_read[];
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int REPLICA_ALREADY_EXISTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int NOT_IMPLEMENTED;
    extern const int KEEPER_EXCEPTION;
}

namespace Setting
{
    extern const SettingsBool cloud_mode;
    extern const SettingsBool s3queue_migrate_old_metadata_to_buckets;
    extern const SettingsFloat s3queue_keeper_fault_injection_probability;
    extern const SettingsUInt64 keeper_max_retries;
    extern const SettingsUInt64 keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 keeper_retry_max_backoff_ms;
}

namespace ObjectStorageQueueSetting
{
    extern const ObjectStorageQueueSettingsObjectStorageQueueMode mode;
}

namespace
{
    UInt64 getCurrentTime()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }

    size_t generateRescheduleInterval(size_t min, size_t max)
    {
        /// Use more or less random interval for unordered mode cleanup task.
        /// So that distributed processing cleanup tasks would not schedule cleanup at the same time.
        pcg64 rng(randomSeed());
        size_t interval = min + rng() % (max - min + 1);
        LOG_TEST(getLogger("ObjectStorageQueueMetadata"), "Reschedule interval: {}", interval);
        return interval;
    }

    bool isUnordered(ObjectStorageQueueMode mode)
    {
        return mode == ObjectStorageQueueMode::UNORDERED;
    }

    bool isExclusive(ObjectStorageQueueMode mode)
    {
        return mode == ObjectStorageQueueMode::EXCLUSIVE;
    }

    UInt128 getMetadataCacheKey(const std::string & path)
    {
        SipHash hash;
        hash.update(path);
        return hash.get128();
    }
}

ObjectStorageQueueMetadata::ObjectStorageQueueMetadata(
    ObjectStorageType storage_type_,
    const std::string & zookeeper_name_,
    const fs::path & zookeeper_path_,
    const ObjectStorageQueueTableMetadata & table_metadata_,
    size_t cleanup_interval_min_ms_,
    size_t cleanup_interval_max_ms_,
    bool use_persistent_processing_nodes_,
    size_t persistent_processing_nodes_ttl_seconds_,
    size_t keeper_multiread_batch_size_,
    size_t metadata_cache_size_bytes_,
    size_t metadata_cache_size_elements_)
    : table_metadata(table_metadata_)
    , storage_type(storage_type_)
    , mode(table_metadata.getMode())
    , bucketing_mode(table_metadata.getBucketingMode())
    , partitioning_mode(table_metadata.getPartitioningMode())
    , zookeeper_name(zookeeper_name_)
    , zookeeper_path(zookeeper_path_)
    , keeper_multiread_batch_size(keeper_multiread_batch_size_)
    , cleanup_processed_files(isUnordered(mode) && table_metadata.hasTrackedFilesLimit())
    /// Two independent reasons to sweep `/failed`, and either one on its own is enough: the
    /// count-based `tracked_files_limit`, and the time-based `failed_files_ttl_sec`. They are separate
    /// controls, so this is a union rather than a choice between them.
    ///
    /// `tracked_files_limit` alone, not `hasTrackedFilesLimit`: the latter is also true for
    /// `tracked_files_ttl_sec`, which is the retention of `/processed` and says nothing about `/failed`.
    /// Mirrors the per-run decision in `cleanupThreadFuncImpl`, so this coarse "could this table ever
    /// need a sweep" answer cannot disagree with what a run actually does.
    , cleanup_failed_files(
          (!isExclusive(mode) && table_metadata.tracked_files_limit)
          || (isUnordered(mode) && table_metadata.failed_files_ttl_sec))
    , cleanup_processing_files(!isExclusive(mode) && use_persistent_processing_nodes_ && persistent_processing_nodes_ttl_seconds_)
    , cleanup_interval_min_ms(cleanup_interval_min_ms_)
    , cleanup_interval_max_ms(cleanup_interval_max_ms_)
    , use_persistent_processing_nodes(use_persistent_processing_nodes_)
    , persistent_processing_node_ttl_seconds(persistent_processing_nodes_ttl_seconds_)
    , buckets_num(table_metadata_.getBucketsNum())
    , log(getLogger(fmt::format(
        "StorageObjectStorageQueue({}{})",
        zookeeper_name_ == zkutil::DEFAULT_ZOOKEEPER_NAME ? "" : zookeeper_name_ + ":",
        zookeeper_path_.string())))
    , local_file_statuses(
        CurrentMetrics::ObjectStorageQueueMetadataCacheSizeBytes,
        CurrentMetrics::ObjectStorageQueueMetadataCacheSizeElements,
        metadata_cache_size_bytes_,
        metadata_cache_size_elements_)
{
    // Initialize regex-based parser if configured
    if (partitioning_mode == ObjectStorageQueuePartitioningMode::REGEX)
    {
        LOG_DEBUG(log, "Initializing regex-based filename parser - partition_regex: '{}', partition_component: '{}'",
                 table_metadata.partition_regex, table_metadata.partition_component);

        filename_parser = std::make_unique<ObjectStorageQueueFilenameParser>(
            table_metadata.partition_regex,
            table_metadata.partition_component);

        if (!filename_parser->isValid())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Failed to initialize filename parser: {}",
                filename_parser->getError());
        }

        LOG_DEBUG(log, "Successfully initialized regex-based filename parser for partitioning");
    }

    LOG_TRACE(
        log, "Mode: {}, buckets: {}, processing threads: {}, metadata_cache_size_bytes: {},"
        "metadata_cache_size_elements: {}, result buckets num: {}, use persistent processing nodes: {}, "
        "cleanup processing files: {}, cleanup processed files: {}, cleanup failed files: {}",
        table_metadata.mode, table_metadata.buckets.load(),
        table_metadata.processing_threads_num.load(), metadata_cache_size_bytes_,
        metadata_cache_size_elements_, buckets_num,
        use_persistent_processing_nodes.load(), cleanup_processing_files, cleanup_processed_files, cleanup_failed_files);
}

ObjectStorageQueueMetadata::~ObjectStorageQueueMetadata()
{
    shutdown();
}

ZooKeeperWithFaultInjection::Ptr ObjectStorageQueueMetadata::getZooKeeper(LoggerPtr log, const String & zookeeper_name)
{
    auto context = Context::getGlobalContextInstance();
    auto zk_client = context->getDefaultOrAuxiliaryZooKeeper(zookeeper_name);
    if (context->getSettingsRef()[Setting::s3queue_keeper_fault_injection_probability] != 0.0f)
    {
        return ZooKeeperWithFaultInjection::createInstance(
            static_cast<double>(context->getSettingsRef()[Setting::s3queue_keeper_fault_injection_probability]),
            /* seed */0,
            zk_client,
            "S3Queue",
            log);
    }
    return std::make_shared<ZooKeeperWithFaultInjection>(zk_client);
}

ZooKeeperRetriesControl ObjectStorageQueueMetadata::getKeeperRetriesControl(LoggerPtr log)
{
    auto context = Context::getGlobalContextInstance();
    const auto & settings = context->getSettingsRef();
    return ZooKeeperRetriesControl{
        "S3Queue",
        log,
        ZooKeeperRetriesInfo{
            settings[Setting::keeper_max_retries],
            settings[Setting::keeper_retry_initial_backoff_ms],
            settings[Setting::keeper_retry_max_backoff_ms],
            context->getProcessListElement()}};
}

void ObjectStorageQueueMetadata::startup()
{
    if (startup_called.exchange(true))
         return;

    /// Union of both guards: master narrowed this to the three flags, which are fixed at construction,
    /// while `isUnordered(mode)` covers an unordered table whose cleanup settings are only turned on
    /// later by `ALTER`. Dropping the mode term would leave such a table with no sweep at all.
    if (!cleanup_task
        && (isUnordered(mode) || cleanup_processed_files || cleanup_failed_files || cleanup_processing_files))
    {
        cleanup_task = Context::getGlobalContextInstance()->getSchedulePool()->createTask(
            StorageID::createEmpty(), "ObjectStorageQueueCleanupFunc",
            [this] { cleanupThreadFunc(); });

        cleanup_task->activate();
        cleanup_task->scheduleAfter(
            generateRescheduleInterval(
                cleanup_interval_min_ms, cleanup_interval_max_ms));
    }
    if (!update_registry_thread)
        update_registry_thread = std::make_unique<ThreadFromGlobalPool>([this](){ updateRegistryFunc(); });
}

void ObjectStorageQueueMetadata::shutdown()
{
    shutdown_called = true;
    if (cleanup_task)
        cleanup_task->deactivate();
    if (update_registry_thread && update_registry_thread->joinable())
        update_registry_thread->join();
}

ObjectStorageQueueMetadata::FileMetadataPtr ObjectStorageQueueMetadata::getFileMetadata(
    const std::string & path,
    ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr bucket_info)
{
    chassert(metadata_ref_count);
    auto [file_status, _] = local_file_statuses.getOrSet(
        getMetadataCacheKey(path), [&]()
        {
            return std::make_shared<ObjectStorageQueueIFileMetadata::FileStatus>(path);
        });
    switch (mode)
    {
        case ObjectStorageQueueMode::ORDERED:
            return std::make_shared<ObjectStorageQueueOrderedFileMetadata>(
                zookeeper_path,
                path,
                file_status,
                bucket_info,
                buckets_num,
                table_metadata.loading_retries,
                *metadata_ref_count,
                use_persistent_processing_nodes,
                zookeeper_name,
                bucketing_mode,
                partitioning_mode,
                filename_parser.get(),
                log);
        case ObjectStorageQueueMode::UNORDERED:
            return std::make_shared<ObjectStorageQueueUnorderedFileMetadata>(
                zookeeper_path,
                path,
                file_status,
                table_metadata.loading_retries,
                *metadata_ref_count,
                use_persistent_processing_nodes,
                zookeeper_name,
                log);
        case ObjectStorageQueueMode::EXCLUSIVE:
            return std::make_shared<ObjectStorageQueueExclusiveFileMetadata>(
                path,
                file_status,
                table_metadata.loading_retries,
                *metadata_ref_count,
                *this,
                zookeeper_name,
                log);
    }
}

bool ObjectStorageQueueMetadata::tryAcquireExclusiveProcessing(const std::string & path)
{
    std::lock_guard lock(exclusive_processing_paths_mutex);
    return exclusive_processing_paths.insert(getMetadataCacheKey(path)).second;
}

void ObjectStorageQueueMetadata::releaseExclusiveProcessing(const std::string & path)
{
    std::lock_guard lock(exclusive_processing_paths_mutex);
    exclusive_processing_paths.erase(getMetadataCacheKey(path));
}

bool ObjectStorageQueueMetadata::useBucketsForProcessing() const
{
    return mode == ObjectStorageQueueMode::ORDERED && (buckets_num > 1);
}

ObjectStorageQueueMetadata::Bucket ObjectStorageQueueMetadata::getBucketForPath(const std::string & path) const
{
    return getBucketForPath(path, buckets_num, bucketing_mode, partitioning_mode, filename_parser.get());
}

ObjectStorageQueueMetadata::Bucket ObjectStorageQueueMetadata::getBucketForPath(
    const std::string & path,
    size_t buckets_num,
    ObjectStorageQueueBucketingMode bucketing_mode,
    ObjectStorageQueuePartitioningMode partitioning_mode,
    const ObjectStorageQueueFilenameParser * parser)
{
    return ObjectStorageQueueOrderedFileMetadata::getBucketForPath(path, buckets_num, bucketing_mode, partitioning_mode, parser);
}

std::optional<std::string> ObjectStorageQueueMetadata::getStartAfterForListing() const
{
    /// Returning std::nullopt is a best-effort fallback: listing proceeds from the prefix and remains correct.
    /// StartAfter is only safe for non-partitioned ordered S3 queues.
    /// With partitioned processing there is no single global last-processed key
    /// that can be used here without risking skipped files.
    if (storage_type != ObjectStorageType::S3
        || mode != ObjectStorageQueueMode::ORDERED
        || partitioning_mode != ObjectStorageQueuePartitioningMode::NONE)
        return std::nullopt;

    const size_t buckets = std::max<size_t>(getBucketsNum(), 1);
    const auto last_processed_paths = ObjectStorageQueueOrderedFileMetadata::getLastProcessedPaths(
        zookeeper_path, buckets, partitioning_mode, zookeeper_name, log);

    /// Resume listing only when every bucket has already advanced at least once.
    /// Then we can safely use the minimum processed key across buckets.
    if (last_processed_paths.size() != buckets)
        return std::nullopt;

    std::optional<std::string> min_path;

    /// One Keeper multi-read for all buckets to avoid O(buckets) round-trips.
    for (const auto & last : last_processed_paths)
    {
        chassert(!last.empty());

        /// Use the smallest processed key across buckets to avoid skipping unprocessed files.
        if (!min_path || last < *min_path)
            min_path = last;
    }

    return min_path;
}

ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr
ObjectStorageQueueMetadata::tryAcquireBucket(const Bucket & bucket)
{
    return ObjectStorageQueueOrderedFileMetadata::tryAcquireBucket(
        zookeeper_path, bucket, use_persistent_processing_nodes, persistent_processing_node_ttl_seconds, zookeeper_name, log);
}

void ObjectStorageQueueMetadata::alterSettings(const SettingsChanges & changes, const ContextPtr & context)
{
    bool is_initial_query = !context->isDDLOrOnClusterInternal() ||
                            (context->getZooKeeperMetadataTransaction() && context->getZooKeeperMetadataTransaction()->isInitialQuery());

    const fs::path alter_settings_lock_path = zookeeper_path / "alter_settings_lock";
    zkutil::EphemeralNodeHolder::Ptr alter_settings_lock;
    auto zookeeper = getZooKeeper();

    if (is_initial_query)
    {
        /// We will retry taking alter_settings_lock for the duration of 5 seconds.
        /// Do we need to add a setting for this?
        const size_t num_tries = 100;
        for (size_t i = 0; i < num_tries; ++i)
        {
            alter_settings_lock = zkutil::EphemeralNodeHolder::tryCreate(alter_settings_lock_path, *zookeeper->getKeeper(), toString(getCurrentTime()));

            if (alter_settings_lock)
                break;

            if (i == num_tries - 1)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Failed to take alter setting lock after 5 seconds");

            sleepForMilliseconds(50);
        }
    }

    Coordination::Stat stat;
    auto metadata_str = zookeeper->get(fs::path(zookeeper_path) / "metadata", &stat);
    auto metadata_from_zk = ObjectStorageQueueTableMetadata::parse(metadata_str);
    auto new_table_metadata{table_metadata};

    for (const auto & change : changes)
    {
        if (change.name == "metadata_cache_size_bytes")
        {
            const auto value = change.value.safeGet<UInt64>();
            LOG_INFO(log, "Setting new metadata cache size to {}", value);
            local_file_statuses.setMaxSizeInBytes(value);
            continue;
        }

        if (change.name == "metadata_cache_size_elements")
        {
            const auto value = change.value.safeGet<UInt64>();
            LOG_INFO(log, "Setting new metadata cache elements to {}", value);
            local_file_statuses.setMaxCount(value);
            continue;
        }

        if (!ObjectStorageQueueTableMetadata::isStoredInKeeper(change.name))
            continue;

        if (change.name == "processing_threads_num")
        {
            const auto value = change.value.safeGet<UInt64>();
            if (table_metadata.processing_threads_num == value)
            {
                LOG_TRACE(log, "Setting `processing_threads_num` already equals {}. "
                        "Will do nothing", value);
                continue;
            }
            new_table_metadata.processing_threads_num = value;
        }
        else if (change.name == "loading_retries")
        {
            const auto value = change.value.safeGet<UInt64>();
            if (table_metadata.loading_retries == value)
            {
                LOG_TRACE(log, "Setting `loading_retries` already equals {}. "
                        "Will do nothing", value);
                continue;
            }
            new_table_metadata.loading_retries = value;
        }
        else if (change.name == "after_processing")
        {
            const auto value = ObjectStorageQueueTableMetadata::actionFromString(change.value.safeGet<String>());
            if (table_metadata.after_processing == value)
            {
                LOG_TRACE(log, "Setting `after_processing` already equals {}. "
                        "Will do nothing", value);
                continue;
            }
            new_table_metadata.after_processing = value;
        }
        else if (change.name == "tracked_files_limit")
        {
            const auto value = change.value.safeGet<UInt64>();
            if (table_metadata.tracked_files_limit == value)
            {
                LOG_TRACE(log, "Setting `tracked_files_limit` already equals {}. "
                        "Will do nothing", value);
                continue;
            }
            new_table_metadata.tracked_files_limit = value;
        }
        else if (change.name == "tracked_file_ttl_sec")
        {
            const auto value = change.value.safeGet<UInt64>();
            if (table_metadata.tracked_files_ttl_sec == value)
            {
                LOG_TRACE(log, "Setting `tracked_file_ttl_sec` already equals {}. "
                        "Will do nothing", value);
                continue;
            }
            new_table_metadata.tracked_files_ttl_sec = value;
        }
        else if (change.name == "failed_files_ttl_sec")
        {
            const auto value = change.value.safeGet<UInt64>();
            if (table_metadata.failed_files_ttl_sec == value)
            {
                LOG_TRACE(log, "Setting `failed_files_ttl_sec` already equals {}. "
                        "Will do nothing", value);
                continue;
            }
            new_table_metadata.failed_files_ttl_sec = value;
        }
        else if (change.name == "buckets")
        {
            if (mode != ObjectStorageQueueMode::ORDERED)
            {
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "Changing `buckets` setting is allowed only for Ordered mode");
            }

            if (!context->getSettingsRef()[Setting::s3queue_migrate_old_metadata_to_buckets])
            {
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "Changing `buckets` setting is allowed only for migration of old metadata structure. "
                    "To allow migration set s3queue_migrate_old_metadata_to_buckets = 1");
            }

            const auto value = change.value.safeGet<UInt64>();
            if (table_metadata.buckets == value)
            {
                LOG_TRACE(log, "Setting `buckets` already equals {}. Will do nothing", value);
                continue;
            }
            if (table_metadata.buckets > 1)
            {
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "It is not allowed to modify `buckets` settings "
                    "when it is already set to a non-zero value");
            }
            migrateToBucketsInKeeper(value);
            new_table_metadata.buckets = value;
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Setting `{}` is not changeable", change.name);
        }
    }

    const auto new_metadata_str = new_table_metadata.toString();
    LOG_TRACE(log, "New metadata: {}", new_metadata_str);

    const fs::path table_metadata_path = zookeeper_path / "metadata";
    /// Here we intentionally do not add zk retries,
    /// because we modify metadata under ephemeral metadata lock,
    /// so we do not want to retry if it expires.
    if (is_initial_query)
        zookeeper->set(table_metadata_path, new_metadata_str, stat.version);

    table_metadata.syncChangeableSettings(new_table_metadata);
}

void ObjectStorageQueueMetadata::migrateToBucketsInKeeper(size_t value)
{
    chassert(table_metadata.buckets == 0 || table_metadata.buckets == 1);
    chassert(buckets_num == 1, "Buckets: " + toString(buckets_num));
    LOG_TRACE(log, "Changing buckets value from {} to {}", table_metadata.buckets.load(), value);
    ObjectStorageQueueOrderedFileMetadata::migrateToBuckets(
        zookeeper_path,
        value,
        /* prev_value */table_metadata.buckets,
        zookeeper_name);
    table_metadata.buckets = value;
    buckets_num = table_metadata.getBucketsNum();
}

ObjectStorageQueueTableMetadata ObjectStorageQueueMetadata::syncWithKeeper(
    const String & zookeeper_name,
    const fs::path & zookeeper_path,
    const ObjectStorageQueueSettings & settings,
    const ColumnsDescription & columns,
    const std::string & format,
    const ContextPtr & context,
    bool is_attach,
    LoggerPtr log)
{
    ObjectStorageQueueTableMetadata table_metadata(settings, columns, format);

    std::vector<std::string> metadata_paths;
    size_t buckets_num = 0;
    if (settings[ObjectStorageQueueSetting::mode] == ObjectStorageQueueMode::ORDERED)
    {
        buckets_num = table_metadata.getBucketsNum();
        if (buckets_num == 0)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot have zero values of `processing_threads_num` and `buckets`");

        LOG_TRACE(log, "Local buckets num: {}", buckets_num);

        metadata_paths = ObjectStorageQueueOrderedFileMetadata::getMetadataPaths(buckets_num);
    }
    else if (settings[ObjectStorageQueueSetting::mode] == ObjectStorageQueueMode::EXCLUSIVE)
    {
        metadata_paths = ObjectStorageQueueExclusiveFileMetadata::getMetadataPaths();
    }
    else
    {
        metadata_paths = ObjectStorageQueueUnorderedFileMetadata::getMetadataPaths();
    }

    auto zk_retries = getKeeperRetriesControl(log);
    const auto table_metadata_path = zookeeper_path / "metadata";
    bool warned = false;

    zk_retries.retryLoop([&] { getZooKeeper(log, zookeeper_name)->createAncestors(zookeeper_path); });

    for (size_t i = 0; i < 1000; ++i)
    {
        Coordination::Requests requests;
        Coordination::Responses responses;
        std::optional<Coordination::Error> code;
        zk_retries.resetFailures();
        zk_retries.retryLoop([&]
        {
            auto zk_client = getZooKeeper(log, zookeeper_name);
            std::optional<ObjectStorageQueueTableMetadata> metadata_from_zk;
            if (zk_client->exists(table_metadata_path))
            {
                const auto metadata_str = zk_client->get(table_metadata_path);
                LOG_TRACE(log, "Metadata in keeper: {}", metadata_str);
                metadata_from_zk.emplace(ObjectStorageQueueTableMetadata::parse(metadata_str));
            }
            if (metadata_from_zk.has_value())
            {
                table_metadata.adjustFromKeeper(metadata_from_zk.value());
                table_metadata.checkEquals(metadata_from_zk.value());
                return;
            }

            const auto & settings_ref = context->getSettingsRef();
            if (!warned && settings_ref[Setting::cloud_mode]
                && table_metadata.getMode() == ObjectStorageQueueMode::ORDERED
                && table_metadata.buckets <= 1 && table_metadata.processing_threads_num <= 1)
            {
                const std::string message = "Ordered mode in cloud without "
                    "either `buckets`>1 or `processing_threads_num`>1 (works as `buckets` if it's not specified) "
                    "will not work properly. Please specify them in the CREATE query. See documentation for more details.";

                if (is_attach)
                {
                    LOG_WARNING(log, "{}", message);
                    warned = true;
                }
                else
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}", message);
                }
            }

            requests.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
            requests.emplace_back(zkutil::makeCreateRequest(
                                    table_metadata_path, table_metadata.toString(), zkutil::CreateMode::Persistent));

            for (const auto & path : metadata_paths)
            {
                const auto zk_path = zookeeper_path / path;
                requests.emplace_back(zkutil::makeCreateRequest(zk_path, "", zkutil::CreateMode::Persistent));
            }

            if (!table_metadata.last_processed_path.empty())
            {
                std::atomic<size_t> noop = 0;

                /// Create parser for regex partitioning mode.
                /// Parser is needed to correctly compute partition keys and bucket assignments.
                std::unique_ptr<ObjectStorageQueueFilenameParser> parser;
                if (table_metadata.getPartitioningMode() == ObjectStorageQueuePartitioningMode::REGEX)
                {
                    parser = std::make_unique<ObjectStorageQueueFilenameParser>(
                        table_metadata.partition_regex,
                        table_metadata.partition_component);
                }

                ObjectStorageQueueOrderedFileMetadata(
                    zookeeper_path,
                    table_metadata.last_processed_path,
                    std::make_shared<ObjectStorageQueueIFileMetadata::FileStatus>(table_metadata.last_processed_path),
                    /* bucket_info */nullptr,
                    buckets_num,
                    table_metadata.loading_retries,
                    noop,
                    /* use_persistent_processing_nodes */false, /// Processing nodes will not be created.
                    zookeeper_name,
                    table_metadata.getBucketingMode(),
                    table_metadata.getPartitioningMode(),
                    parser.get(),
                    log).prepareProcessedAtStartRequests(requests);
            }

            code = zk_client->tryMulti(requests, responses);
        });
        if (code.has_value())
        {
            if (*code == Coordination::Error::ZNODEEXISTS)
            {
                auto exception = zkutil::KeeperMultiException(*code, requests, responses);

                LOG_INFO(log, "Got code `{}` for path: {}. "
                        "It looks like the table {} was created by another server at the same moment, "
                        "will retry",
                        *code, exception.getPathForFirstFailedOp(), zookeeper_path.string());
                continue;
            }
            if (*code != Coordination::Error::ZOK)
                zkutil::KeeperMultiException::check(*code, requests, responses);
        }

        return table_metadata;
    }

    throw Exception(
        ErrorCodes::REPLICA_ALREADY_EXISTS,
        "Cannot create table, because it is created concurrently every time or because "
        "of wrong zookeeper path or because of logical error");
}

namespace
{
    struct Info
    {
        std::string hostname;
        std::string table_id;
        std::string server_uuid;

        size_t version = 1;

        bool operator ==(const Info & other) const
        {
            return hostname == other.hostname && table_id == other.table_id
                && (version == 0 || other.version == 0 || server_uuid == other.server_uuid);
        }

        static Info create(const StorageID & storage_id)
        {
            Info self;
            self.hostname = DNSResolver::instance().getHostName();
            self.table_id = storage_id.hasUUID() ? toString(storage_id.uuid) : storage_id.getFullTableName();
            self.server_uuid = toString(ServerUUID::get());
            return self;
        }

        UInt128 hash() const
        {
            SipHash hash;
            hash.update(hostname);
            hash.update(table_id);
            hash.update(server_uuid);
            return hash.get128();
        }

        std::string serialize() const
        {
            WriteBufferFromOwnString buf;
            buf << version << "\n";
            buf << hostname << "\n";
            buf << table_id << "\n";
            if (version >= 1)
                buf << server_uuid << "\n";
            return buf.str();
        }

        static Info deserialize(std::string_view str)
        {
            ReadBufferFromString buf(str);
            Info info;
            buf >> info.version >> "\n";
            buf >> info.hostname >> "\n";
            buf >> info.table_id >> "\n";
            if (info.version >= 1)
                buf >> info.server_uuid >> "\n";
            return info;
        }
    };
}

void ObjectStorageQueueMetadata::updateNewestSeenTimestamp(time_t timestamp, const StorageID & storage_id)
{
    std::lock_guard lock(pipeline_lag_watermarks_mutex);
    auto & watermarks = pipeline_lag_watermarks[storage_id.getFullTableName()];
    if (timestamp > watermarks.newest_seen)
    {
        watermarks.newest_seen = timestamp;
        DimensionalMetrics::set(
            DimensionalMetrics::ObjectStorageQueueNewestSeenTimestamp,
            {storage_id.getDatabaseName(), storage_id.getTableName()},
            static_cast<double>(timestamp));
    }
}

void ObjectStorageQueueMetadata::updateNewestCommittedTimestamp(time_t timestamp, const StorageID & storage_id)
{
    std::lock_guard lock(pipeline_lag_watermarks_mutex);
    auto & watermarks = pipeline_lag_watermarks[storage_id.getFullTableName()];
    if (timestamp > watermarks.newest_committed)
    {
        watermarks.newest_committed = timestamp;
        DimensionalMetrics::set(
            DimensionalMetrics::ObjectStorageQueueNewestCommittedTimestamp,
            {storage_id.getDatabaseName(), storage_id.getTableName()},
            static_cast<double>(timestamp));
    }
}

void ObjectStorageQueueMetadata::registerActive(const StorageID & storage_id)
{
    const auto id = getProcessorID(storage_id);
    const auto table_path = zookeeper_path / "registry" / id;
    const auto self = Info::create(storage_id);

    Coordination::Error code = {};
    getKeeperRetriesControl(log).retryLoop([&]
    {
        code = getZooKeeper()->tryCreate(
            table_path,
            self.serialize(),
            zkutil::CreateMode::Ephemeral);
    });

    if (code != Coordination::Error::ZOK
        && code != Coordination::Error::ZNODEEXISTS)
        throw zkutil::KeeperException(code);

    LOG_TRACE(log, "Added {} to active registry ({})", self.table_id, id);
}

void ObjectStorageQueueMetadata::registerNonActive(const StorageID & storage_id, bool & created_new_metadata)
{
    const auto registry_path = zookeeper_path / "registry";
    const auto self = Info::create(storage_id);
    const auto drop_lock_path = zookeeper_path / "drop";

    auto zk_retries = getKeeperRetriesControl(log);

    Coordination::Error code = {};
    const size_t max_tries = 1000;
    for (size_t i = 0; i < max_tries; ++i)
    {
        Coordination::Stat stat;
        std::string registry_str;

        Coordination::Requests requests;
        Coordination::Responses responses;

        zk_retries.resetFailures();
        zk_retries.retryLoop([&]
        {
            auto zk_client = getZooKeeper();
            bool registry_exists = zk_client->tryGet(registry_path, registry_str, &stat);
            if (registry_exists)
            {
                std::vector<std::string_view> registered;
                splitInto<','>(registered, registry_str);

                if (zk_retries.isRetry() && registered.size() == 1 && (Info::deserialize(registered[0]) == self))
                {
                    LOG_TRACE(log, "Table {} is already registered after retry", self.table_id);
                    created_new_metadata = true;
                    code = Coordination::Error::ZOK;
                    return;
                }

                created_new_metadata = false;

                for (auto elem : registered)
                {
                    if (elem.empty())
                        continue;

                    auto info = Info::deserialize(elem);
                    if (info == self)
                    {
                        LOG_TRACE(log, "Table {} is already registered ({})", self.table_id, registered.size());
                        code = Coordination::Error::ZOK;
                        return;
                    }
                }

                auto new_registry_str = registry_str + "," + self.serialize();
                requests.push_back(zkutil::makeSetRequest(registry_path, new_registry_str, stat.version));
            }
            else
            {
                created_new_metadata = true;

                requests.push_back(zkutil::makeCreateRequest(
                    registry_path,
                    self.serialize(),
                    zkutil::CreateMode::Persistent));

                if (!zk_client->isFeatureEnabled(DB::KeeperFeatureFlag::REMOVE_RECURSIVE))
                    zkutil::addCheckNotExistsRequest(requests, *getZooKeeper(), drop_lock_path);
            }

            code = zk_client->tryMulti(requests, responses);
        });
        if (code == Coordination::Error::ZOK)
        {
            LOG_TRACE(log, "Added {} to registry", self.table_id);
            return;
        }

        if ((code == Coordination::Error::ZBADVERSION
            || code == Coordination::Error::ZNODEEXISTS
            || code == Coordination::Error::ZNONODE
            || code == Coordination::Error::ZSESSIONEXPIRED) && (i < max_tries - 1))
        {
            continue;
        }

        zkutil::KeeperMultiException::check(code, requests, responses);
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot register in keeper. Last error: {}", code);
}

Strings ObjectStorageQueueMetadata::getRegistered(bool active)
{
    const auto registry_path = zookeeper_path / "registry";
    auto zk_retries = getKeeperRetriesControl(log);
    Strings registered;
    if (active)
    {
        Coordination::Error code = {};
        zk_retries.retryLoop([&] { code = getZooKeeper()->tryGetChildren(registry_path, registered); });
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
            throw zkutil::KeeperException(code);
    }
    else
    {
        std::string registry_str;
        zk_retries.retryLoop([&] { getZooKeeper()->tryGet(registry_path, registry_str); });
        if (!registry_str.empty())
            splitInto<','>(registered, registry_str);
    }
    return registered;
}

void ObjectStorageQueueMetadata::unregisterActive(const StorageID & storage_id)
{
    const auto registry_path = zookeeper_path / "registry";
    const auto table_path = registry_path / getProcessorID(storage_id);

    Coordination::Error code = {};
    getKeeperRetriesControl(log).retryLoop([&] { code = getZooKeeper()->tryRemove(table_path); });

    if (code == Coordination::Error::ZOK)
    {
        LOG_TRACE(
            log, "Table '{}' has been removed from the active registry "
            "(table path: {})",
            storage_id.getNameForLogs(), table_path);
    }
    else
    {
        LOG_DEBUG(
            log,
            "Cannot remove table '{}' from the active registry, reason: {} "
            "(table path: {})",
            storage_id.getNameForLogs(),
            Coordination::errorMessage(code),
            table_path);
    }
}

void ObjectStorageQueueMetadata::unregisterNonActive(const StorageID & storage_id, bool remove_metadata_if_no_registered)
{
    const auto registry_path = zookeeper_path / "registry";
    const auto drop_lock_path = zookeeper_path / "drop";
    const auto self = Info::create(storage_id);

    Coordination::Error code = Coordination::Error::ZOK;

    bool is_retry = false;
    bool allow_remove_recursive = true;
    for (size_t i = 0; i < 1000; ++i)
    {
        Coordination::Requests requests;
        Coordination::Responses responses;
        size_t count = 0;

        bool supports_remove_recursive = true;
        /// Here we intentionally do not use zk retries,
        /// because we have try-catch and retry this block of code as a whole.
        ZooKeeperWithFaultInjection::Ptr zk_client;

        try
        {
            zk_client = getZooKeeper();
            supports_remove_recursive = allow_remove_recursive && zk_client->isFeatureEnabled(DB::KeeperFeatureFlag::REMOVE_RECURSIVE);

            Coordination::Stat stat;
            std::string registry_str;
            bool node_exists = zk_client->tryGet(registry_path, registry_str, &stat);
            if (!node_exists)
            {
                if (is_retry)
                {
                    LOG_TRACE(log, "Table is unregistered after retry");
                }
                else
                {
                    LOG_WARNING(log, "Cannot unregister: registry does not exist");
                    chassert(false);
                }
                return;
            }

            std::vector<std::string_view> registered;
            splitInto<','>(registered, registry_str);

            bool found = false;
            std::string new_registry_str;
            for (const auto & elem : registered)
            {
                if (elem.empty())
                    continue;

                auto info = Info::deserialize(elem);
                if (info == self)
                    found = true;
                else
                {
                    if (!new_registry_str.empty())
                        new_registry_str += ",";
                    new_registry_str += elem;
                    count += 1;
                }
            }

            if (!found)
            {
                if (is_retry)
                {
                    LOG_TRACE(log, "Table is unregistered after retry");
                    return;
                }
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unregister: table '{}' is not registered", self.table_id);
            }

            LOG_TRACE(log, "Registered count: {}, remove metadata: {}", count, remove_metadata_if_no_registered);

            if (remove_metadata_if_no_registered && count == 0)
            {
                LOG_TRACE(log, "Removing all metadata in keeper by path: {}", zookeeper_path.string());
                if (supports_remove_recursive)
                {
                    requests.push_back(zkutil::makeCheckRequest(registry_path, stat.version));
                    requests.push_back(zkutil::makeRemoveRecursiveRequest(*zk_client, zookeeper_path, /*remove_nodes_limit=*/10000));
                }
                else
                {
                    requests.push_back(zkutil::makeCheckRequest(registry_path, stat.version));
                    requests.push_back(zkutil::makeCreateRequest(drop_lock_path, "", zkutil::CreateMode::Ephemeral));
                }
                code = zk_client->tryMulti(requests, responses);
            }
            else
            {
                code = zk_client->trySet(registry_path, new_registry_str, stat.version);
            }
        }
        catch (const zkutil::KeeperMultiException & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                LOG_TEST(log, "Lost connection to zookeeper, will retry");
                is_retry = true;
                continue;
            }
            throw;
        }
        catch (const zkutil::KeeperException & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                LOG_TEST(log, "Lost connection to zookeeper, will retry");
                is_retry = true;
                continue;
            }
            throw;
        }

        if (code == Coordination::Error::ZOK)
        {
            LOG_TRACE(log, "Table '{}' has been removed from the registry", self.table_id);

            if (!supports_remove_recursive && remove_metadata_if_no_registered && count == 0)
            {
                /// Take a drop lock and do recursive remove as a separate request.
                /// In case of unsupported "remove_recursive" feature, it will
                /// do getChildren and remove them one by one.
                auto drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *zk_client->getKeeper());
                try
                {
                    zk_client->removeRecursive(zookeeper_path);
                }
                catch (const zkutil::KeeperMultiException & e)
                {
                    if (Coordination::isHardwareError(e.code))
                    {
                        LOG_TEST(log, "Lost connection to zookeeper, will retry");
                        is_retry = true;
                        continue;
                    }
                    throw;
                }
                catch (const zkutil::KeeperException & e)
                {
                    if (Coordination::isHardwareError(e.code))
                    {
                        LOG_TEST(log, "Lost connection to zookeeper, will retry");
                        is_retry = true;
                        continue;
                    }
                    throw;
                }
            }
            return;
        }

        if (!responses.empty() && supports_remove_recursive && code == Coordination::Error::ZNOTEMPTY) /// potentiall we reached RemoveRecursive node limit, let's try without it
        {
            allow_remove_recursive = false;
            is_retry = true;
            continue;
        }

        if (Coordination::isHardwareError(code)
            || code == Coordination::Error::ZBADVERSION)
        {
            is_retry = true;
            continue;
        }

        if (!responses.empty())
        {
            zkutil::KeeperMultiException::check(code, requests, responses);
        }
        throw zkutil::KeeperException(code);
    }

    if (Coordination::isHardwareError(code))
        throw zkutil::KeeperException(code);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unregister in keeper. Last error: {}", code);
}

class ObjectStorageQueueMetadata::ServersHashRing
{
public:
    ServersHashRing(size_t total_nodes_, LoggerPtr log_) : total_nodes(total_nodes_), log(log_) {}

    void rebuild(const NameSet & servers)
    {
        virtual_nodes.clear();
        if (servers.empty())
            return;

        size_t virtual_nodes_num = std::max<size_t>(1, total_nodes / servers.size());
        for (const auto & server : servers)
        {
            for (size_t i = 0; i < virtual_nodes_num; ++i)
                virtual_nodes.emplace(hash(server + DB::toString(i)), server);

            LOG_TRACE(log, "Adding node {}, virtual_nodes: {}", server, virtual_nodes_num);
        }
        nodes_num = servers.size();
    }

    std::string chooseServer(const UInt128 & hash) const
    {
        if (virtual_nodes.empty())
            return {};
        auto it = virtual_nodes.lower_bound(hash);
        if (it == virtual_nodes.end())
            it = virtual_nodes.begin();
        return it->second;
    }

    size_t size() const { return nodes_num; }

    template<typename... Args>
    static UInt128 hash(Args... args)
    {
        auto hash = SipHash();
        (hash.update(args), ...);
        return hash.get128();
    }

private:
    const size_t total_nodes;
    LoggerPtr log;
    std::map<UInt128, std::string> virtual_nodes;
    size_t nodes_num{};
};

std::string ObjectStorageQueueMetadata::getProcessorID(const StorageID & storage_id)
{
    return toString(Info::create(storage_id).hash());
}

void ObjectStorageQueueMetadata::filterOutForProcessor(Strings & paths, const StorageID & storage_id) const
{
    std::shared_lock lock(active_servers_mutex);
    if (active_servers.empty() || !active_servers_hash_ring)
        return;

    const auto self = getProcessorID(storage_id);
    Strings result;
    for (auto & path : paths)
    {
        const auto chosen = active_servers_hash_ring->chooseServer(ServersHashRing::hash(path));
        if (chosen == self)
            result.emplace_back(std::move(path));
        else
            LOG_TEST(log, "Will skip file {}: it should be processed by {} (self {})", path, chosen, self);
    }
    paths = std::move(result);
}

void ObjectStorageQueueMetadata::updateRegistryFunc()
{
    auto component_guard = Coordination::setCurrentComponent("ObjectStorageQueueMetadata::updateRegistry");

    try
    {
        Coordination::EventPtr wait_event = std::make_shared<Poco::Event>();
        while (!shutdown_called.load())
        {
            try
            {
                updateRegistry(getRegistered(/* active */true));
            }
            catch (const Coordination::Exception & e)
            {
                if (Coordination::isHardwareError(e.code))
                {
                    LOG_INFO(
                        log, "Lost ZooKeeper connection, will try to connect again: {}",
                        DB::getCurrentExceptionMessage(true));

                    sleepForSeconds(1);
                }
                else
                {
                    DB::tryLogCurrentException(log);
                    chassert(false);
                }
                continue;
            }
            catch (...)
            {
                DB::tryLogCurrentException(log);
                chassert(false);
            }

            if (shutdown_called.load())
                break;

            wait_event->tryWait(1000);
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException(log);
        chassert(false);
    }
}

void ObjectStorageQueueMetadata::updateRegistry(const DB::Strings & registered_)
{
    NameSet registered_set(registered_.begin(), registered_.end());
    if (registered_set == active_servers)
        return;

    std::unique_lock lock(active_servers_mutex);

    CurrentMetrics::sub(CurrentMetrics::ObjectStorageQueueRegisteredServers, active_servers.size());

    active_servers = registered_set;

    CurrentMetrics::add(CurrentMetrics::ObjectStorageQueueRegisteredServers, active_servers.size());

    if (!active_servers_hash_ring)
        active_servers_hash_ring = std::make_shared<ServersHashRing>(1000, log); /// TODO: Add a setting.

    active_servers_hash_ring->rebuild(active_servers);
}

void ObjectStorageQueueMetadata::cleanupThreadFunc()
{
    /// A background task is responsible for maintaining
    /// table_metadata.tracked_files_limit and max_set_age settings for `unordered` processing mode.

    if (shutdown_called)
        return;

    auto component_guard = Coordination::setCurrentComponent("ObjectStorageQueueMetadata::cleanupThreadFunc");
    try
    {
        cleanupThreadFuncImpl();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to cleanup nodes in zookeeper: {}", getCurrentExceptionMessage(true));
    }

    if (shutdown_called)
        return;

    cleanup_task->scheduleAfter(
        generateRescheduleInterval(
            cleanup_interval_min_ms, cleanup_interval_max_ms));
}

void ObjectStorageQueueMetadata::cleanupThreadFuncImpl()
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::ObjectStorageQueueCleanupMaxSetSizeOrTTLMicroseconds);

    const fs::path zookeeper_cleanup_lock_path = zookeeper_path / "cleanup_lock";
    const auto zk_client = getZooKeeper();

    /// Create a lock so that with distributed processing
    /// multiple nodes do not execute cleanup in parallel.
    /// Store "background_cleanup" in the lock value to distinguish from manual dropFailedFiles.
    static constexpr const char * LOCK_OPERATION_BACKGROUND = "background_cleanup";
    auto ephemeral_node = zkutil::EphemeralNodeHolder::tryCreate(
        zookeeper_cleanup_lock_path, *zk_client->getKeeper(), LOCK_OPERATION_BACKGROUND);

    if (!ephemeral_node)
    {
        LOG_TEST(log, "Cleanup is already being executed by another node");
        return;
    }

    /// Everything below runs pinned to `zk_client`, the session that owns the lock, and is not retried.
    /// A hardware error means that session may be gone - and the ephemeral lock with it - so the sweep
    /// stops rather than deleting nodes on a session holding nothing, and `setAlreadyRemoved` keeps the
    /// holder's destructor from removing a lock node that by then may belong to another replica. No
    /// outer retry is needed here: unlike the user-facing drop, this task is periodic, so the next
    /// scheduled run is the retry.
    try
    {
        /// Check the TTL as well: it is changeable at runtime and zero disables
        /// the cleanup (otherwise every node would be treated as stale).
        if (cleanup_processing_files && persistent_processing_node_ttl_seconds)
            cleanupPersistentProcessingNodes(zk_client);

        /// Re-derived per run rather than taken from the members: `tracked_files_limit`,
        /// `tracked_file_ttl_sec` and `failed_files_ttl_sec` are all alterable at runtime, so a decision
        /// made once at construction would go stale. The members remain the coarse "could this table ever
        /// need a sweep" answer that `startup` uses.
        const bool sweep_processed = isUnordered(mode) && table_metadata.hasTrackedFilesLimit();
        /// `/failed` has two independent controls, and each trims by its own criterion: the count-based
        /// `tracked_files_limit`, and the time-based `failed_files_ttl_sec`. They are deliberately not
        /// collapsed into one call - neither overrides the other, and a table may have either, both or
        /// neither. Both passes run under the same cleanup lock this function already holds.
        ///
        /// The count pass is gated on `tracked_files_limit` alone rather than `hasTrackedFilesLimit`,
        /// which is also true for `tracked_files_ttl_sec`. `tracked_files_ttl_sec` is the retention of
        /// `/processed`; letting it enable a `/failed` pass would put the two sets back on one knob, and
        /// would reach `cleanupTrackedNodes` with no limit and no TTL, which it asserts against.
        const bool sweep_failed_by_limit = !isExclusive(mode) && table_metadata.tracked_files_limit;
        const bool sweep_failed_by_ttl = isUnordered(mode) && table_metadata.failed_files_ttl_sec;

        if (sweep_processed || sweep_failed_by_limit || sweep_failed_by_ttl)
        {
            if (sweep_processed)
                cleanupTrackedNodes(zk_client, zookeeper_path / "processed", "processed", table_metadata.tracked_files_ttl_sec, table_metadata.tracked_files_limit);

            /// Count-only, with no TTL of its own: `/failed` expires by `failed_files_ttl_sec` and by
            /// nothing else. Passing `tracked_files_ttl_sec` here would keep `/processed` retention
            /// trimming `/failed` behind the new setting's back, which is exactly what this setting
            /// exists to separate - and for a table old enough to inherit the legacy fallback it would
            /// scan the whole subtree twice per run with the same TTL.
            if (sweep_failed_by_limit)
                cleanupTrackedNodes(zk_client, zookeeper_path / "failed", "failed", /* ttl_seconds */0, table_metadata.tracked_files_limit);

            if (sweep_failed_by_ttl)
                cleanupTrackedNodes(zk_client, zookeeper_path / "failed", "failed", table_metadata.failed_files_ttl_sec, 0);

            /// One reconciliation covers both passes: either may have removed terminal nodes, and the
            /// cache has to stop claiming a file is Failed once its node is gone.
            if (sweep_failed_by_limit || sweep_failed_by_ttl)
                reconcileFailedFilesCache();
        }
    }
    catch (const Coordination::Exception & e)
    {
        if (!Coordination::isHardwareError(e.code))
            throw;

        LOG_WARNING(log, "Keeper error while holding the cleanup lock: {}. The lock may no longer be ours, "
                         "so this sweep is abandoned; the next scheduled run will retry.", e.displayText());
        ephemeral_node->setAlreadyRemoved();
        return;
    }

    LOG_TRACE(log, "Node limits check finished");
}

void ObjectStorageQueueMetadata::cleanupTrackedNodes(
    const std::shared_ptr<ZooKeeperWithFaultInjection> & zk_client,
    const std::string & nodes_path,
    std::string_view description,
    UInt64 ttl_seconds,
    UInt64 nodes_limit)
{
    LOG_TEST(log, "Checking {} nodes for tracking limits", description);

    Strings nodes;
    Coordination::Error code = {};
    code = zk_client->tryGetChildren(nodes_path, nodes);
    if (code != Coordination::Error::ZOK)
    {
        if (code == Coordination::Error::ZNONODE)
        {
            LOG_TEST(log, "Path {} does not exist", nodes_path);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error: {}", magic_enum::enum_name(code));
    }

    if (nodes.empty())
    {
        LOG_TEST(log, "There are no {} nodes at path {}", description, nodes_path);
        return;
    }

    const bool check_nodes_limit = nodes_limit > 0;
    const bool check_nodes_ttl = ttl_seconds > 0;
    chassert(check_nodes_limit || check_nodes_ttl);

    const bool nodes_limit_exceeded = nodes.size() > nodes_limit;
    if ((!nodes_limit_exceeded || !check_nodes_limit) && !check_nodes_ttl)
    {
        LOG_TEST(log, "No limit exceeded (nodes: {}/{})", nodes.size(), nodes_limit);
        return;
    }

    LOG_TRACE(log, "Will check limits for {} {} nodes", nodes.size(), description);

    struct Node
    {
        std::string zk_path;
        ObjectStorageQueueIFileMetadata::NodeMetadata metadata;
    };
    auto node_cmp = [](const Node & a, const Node & b)
    {
        return std::tie(a.metadata.last_processed_timestamp, a.metadata.file_path)
            < std::tie(b.metadata.last_processed_timestamp, b.metadata.file_path);
    };

    /// Ordered in ascending order of timestamps.
    std::set<Node, decltype(node_cmp)> sorted_nodes(node_cmp);

    std::vector<std::string> paths;
    auto get_paths = [&]
    {
        LOG_TEST(log, "Fetching info for {} paths", paths.size());

        zkutil::ZooKeeper::MultiTryGetResponse response;
        response = zk_client->tryGet(paths);

        for (size_t i = 0; i < response.size(); ++i)
        {
            if (response[i].error == Coordination::Error::ZNONODE)
            {
                LOG_ERROR(log, "Failed to fetch node metadata {}", paths[i]);
                continue;
            }

            chassert(response[i].error == Coordination::Error::ZOK);
            sorted_nodes.emplace(paths[i], ObjectStorageQueueIFileMetadata::NodeMetadata::fromString(response[i].data));
            LOG_TEST(log, "Fetched metadata for node {}", paths[i]);
        }
        paths.clear();
    };

    std::filesystem::path nodes_fs_path(nodes_path);
    for (const auto & node : nodes)
    {
        /// Skip retry-state nodes - only terminal failed nodes are cleaned up here.
        ///
        /// The reason is the retry counter, not the node's lifetime: a `.retriable` node carries how
        /// many attempts a file has already used, and deleting it mid-retry silently resets that count,
        /// so a file that should have been given up on keeps being retried forever.
        ///
        /// These nodes are persistent, not ephemeral, and nothing here reaps them. The transition that
        /// exhausts the retries removes the marker as it creates the terminal node, but two paths still
        /// leave one behind: a file that fails and later succeeds, and `loading_retries` being altered
        /// from a positive value to zero. Fixing those belongs to the success and failure transitions
        /// rather than to this sweep, and is deliberately left to a separate change - it is a standalone
        /// bug fix that predates the failed-files TTL work. Until it lands, such markers accumulate.
        if (node.ends_with(".retriable"))
            continue;

        paths.push_back(nodes_fs_path / node);
        if (paths.size() == keeper_multiread_batch_size)
            get_paths();
    }

    if (!paths.empty())
        get_paths();

    auto get_nodes_str = [&]()
    {
        WriteBufferFromOwnString wb;
        for (const auto & [node, metadata] : sorted_nodes)
            wb << fmt::format("Node: {}, path: {}, timestamp: {};\n", node, metadata.file_path, metadata.last_processed_timestamp);
        return wb.str();
    };

    LOG_TEST(
        log, "Checking node limits (max size: {}, max age: {}) for {}",
        nodes_limit,
        ttl_seconds,
        get_nodes_str());

    static constexpr size_t keeper_multi_batch_size = 100;
    Coordination::Requests remove_requests;
    Coordination::Responses remove_responses;
    remove_requests.reserve(keeper_multi_batch_size);
    remove_responses.reserve(keeper_multi_batch_size);

    /// Track file paths corresponding to remove requests for success-based cache invalidation.
    /// Parallel to remove_requests: batch_file_paths[i] is the file_path for remove_requests[i].
    std::vector<std::string> batch_file_paths;
    batch_file_paths.reserve(keeper_multi_batch_size);

    /// Snapshot generations before starting Keeper deletes
    /// to prevent race where file re-fails with new generation before cache removal
    std::unordered_map<std::string, uint64_t> generations_snapshot;
    {
        auto all_entries = local_file_statuses.dump();
        for (const auto & entry : all_entries)
        {
            generations_snapshot[entry.mapped->path] = entry.mapped->generation.load();
        }
    }

    size_t nodes_to_remove = check_nodes_limit && nodes_limit_exceeded
        ? nodes.size() - nodes_limit
        : 0;

    const auto remove_nodes = [&](bool node_limit)
    {
        code = zk_client->tryMulti(remove_requests, remove_responses);

        if (code == Coordination::Error::ZOK)
        {
            /// Full batch succeeded - clear cache for all requests in this batch
            for (const auto & file_path : batch_file_paths)
            {
                using KeyType = UInt128;
                using StatusPtr = ObjectStorageQueueIFileMetadata::FileStatusPtr;
                local_file_statuses.remove(std::function<bool(const KeyType &, const StatusPtr &)>(
                    [&generations_snapshot, &file_path](const KeyType &, const StatusPtr & status)
                    {
                        if (status->path == file_path)
                        {
                            auto it = generations_snapshot.find(file_path);
                            if (it != generations_snapshot.end() && status->generation.load() == it->second)
                                return true;
                        }
                        return false;
                    }
                ));
            }

            if (node_limit)
                nodes_to_remove -= remove_requests.size();
        }
        else
        {
            /// Partial success: reconcile individual responses.
            /// Clear cache only for nodes that were successfully deleted.
            for (size_t i = 0; i < remove_requests.size(); ++i)
            {
                if (remove_responses[i]->error == Coordination::Error::ZOK)
                {
                    const auto & file_path = batch_file_paths[i];
                    using KeyType = UInt128;
                    using StatusPtr = ObjectStorageQueueIFileMetadata::FileStatusPtr;
                    local_file_statuses.remove(std::function<bool(const KeyType &, const StatusPtr &)>(
                        [&generations_snapshot, &file_path](const KeyType &, const StatusPtr & status)
                        {
                            if (status->path == file_path)
                            {
                                auto it = generations_snapshot.find(file_path);
                                if (it != generations_snapshot.end() && status->generation.load() == it->second)
                                    return true;
                            }
                            return false;
                        }
                    ));

                    if (node_limit)
                        --nodes_to_remove;
                }
                else if (remove_responses[i]->error == Coordination::Error::ZRUNTIMEINCONSISTENCY)
                {
                    /// requests with ZRUNTIMEINCONSISTENCY were not processed because the multi request was aborted before
                    /// so we try removing it again without multi requests
                    code = zk_client->tryRemove(remove_requests[i]->getPath());
                    if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE)
                    {
                        /// ZOK: retry succeeded. ZNONODE: first attempt already deleted the node
                        /// before the multi aborted. Either way, the node is gone - clear cache.
                        const auto & file_path = batch_file_paths[i];
                        using KeyType = UInt128;
                        using StatusPtr = ObjectStorageQueueIFileMetadata::FileStatusPtr;
                        local_file_statuses.remove(std::function<bool(const KeyType &, const StatusPtr &)>(
                            [&generations_snapshot, &file_path](const KeyType &, const StatusPtr & status)
                            {
                                if (status->path == file_path)
                                {
                                    auto it = generations_snapshot.find(file_path);
                                    if (it != generations_snapshot.end() && status->generation.load() == it->second)
                                        return true;
                                }
                                return false;
                            }
                        ));

                        if (node_limit)
                            --nodes_to_remove;

                        if (code == Coordination::Error::ZNONODE)
                            LOG_TRACE(log, "Node `{}` already removed (likely by first attempt before multi aborted)", remove_requests[i]->getPath());
                    }
                    else
                    {
                        LOG_ERROR(log, "Failed to remove a node `{}` (code: {})", remove_requests[i]->getPath(), code);
                    }
                }
                else
                {
                    LOG_ERROR(log, "Failed to remove a node `{}` (code: {})", remove_requests[i]->getPath(), remove_responses[i]->error);
                }
            }
        }

        remove_requests.clear();
        batch_file_paths.clear();
    };

    for (const auto & node : sorted_nodes)
    {
        if (nodes_to_remove)
        {
            LOG_TRACE(log, "Removing node at path {} ({}) because max files limit is reached",
                     node.metadata.file_path, node.zk_path);

            batch_file_paths.push_back(node.metadata.file_path);
            remove_requests.push_back(zkutil::makeRemoveRequest(node.zk_path, -1));
            /// we either reach max multi batch size OR we already added maximum amount of nodes we want to delete based on the node limit
            if (remove_requests.size() == keeper_multi_batch_size || remove_requests.size() == nodes_to_remove)
                remove_nodes(/*node_limit=*/true);
        }
        else if (check_nodes_ttl)
        {
            UInt64 node_age = getCurrentTime() - node.metadata.last_processed_timestamp;
            if (node_age >= ttl_seconds)
            {
                LOG_TRACE(log, "Removing node at path {} ({}) because file ttl is reached",
                        node.metadata.file_path, node.zk_path);

                batch_file_paths.push_back(node.metadata.file_path);
                remove_requests.push_back(zkutil::makeRemoveRequest(node.zk_path, -1));
                if (remove_requests.size() == keeper_multi_batch_size)
                    remove_nodes(/*node_limit=*/false);
            }
            else if (!nodes_to_remove)
            {
                /// Nodes limit satisfied.
                /// Nodes ttl satisfied as well as if current node is under tll, then all remaining as well
                /// (because we are iterating in timestamp ascending order).
                break;
            }
        }
        else
        {
            /// Nodes limit and ttl are satisfied.
            break;
        }
    }

    if (!remove_requests.empty())
        remove_nodes(/*node_limit=*/false);
}

void ObjectStorageQueueMetadata::removeFromCacheIfGenerationMatches(
    const std::string & file_path, const std::unordered_map<std::string, uint64_t> & failed_generations)
{
    using KeyType = UInt128;
    using StatusPtr = ObjectStorageQueueIFileMetadata::FileStatusPtr;
    local_file_statuses.remove(std::function<bool(const KeyType &, const StatusPtr &)>(
        [&failed_generations, &file_path](const KeyType &, const StatusPtr & status)
        {
            if (status->path == file_path && status->state == ObjectStorageQueueIFileMetadata::FileStatus::State::Failed)
            {
                auto it = failed_generations.find(file_path);
                if (it != failed_generations.end() && status->generation.load() == it->second)
                    return true;
            }
            return false;
        }
    ));
}


size_t ObjectStorageQueueMetadata::removeStaleFailedCacheEntries(
    const std::unordered_map<std::string, uint64_t> & failed_generations,
    const std::function<bool(const std::string &)> & path_filter)
{
    size_t removed = 0;
    using KeyType = UInt128;
    using StatusPtr = ObjectStorageQueueIFileMetadata::FileStatusPtr;
    local_file_statuses.remove(std::function<bool(const KeyType &, const StatusPtr &)>(
        [&failed_generations, &path_filter, &removed](const KeyType & /* key */, const StatusPtr & status)
        {
            if (status->state == ObjectStorageQueueIFileMetadata::FileStatus::State::Failed
                && path_filter(status->path))
            {
                auto it = failed_generations.find(status->path);
                if (it != failed_generations.end() && status->generation.load() == it->second)
                {
                    ++removed;
                    return true;
                }
                return false;
            }
            return false;
        }
    ));
    return removed;
}

void ObjectStorageQueueMetadata::reconcileFailedFilesCache()
{
    /// Reconcile local cache with Keeper state by removing cache entries
    /// for files that no longer have /failed nodes in Keeper.
    /// Used by losing replicas in ON CLUSTER execution to achieve cache consistency
    /// after the winning replica completes the cleanup.

    const std::string failed_path = zookeeper_path / "failed";
    auto zk_client = getZooKeeper();
    auto zk_retries = getKeeperRetriesControl(log);

    /// Snapshot cache FIRST to establish consistent baseline before any Keeper checks.
    /// Any file failing between this snapshot and subsequent Keeper checks will appear
    /// in Keeper, preventing wrongful eviction based on stale Keeper snapshot.
    std::unordered_map<std::string, uint64_t> failed_generations;
    std::vector<std::tuple<std::string, std::string, std::string, uint64_t>> cache_entries;
    {
        auto all_entries = local_file_statuses.dump();
        for (const auto & entry : all_entries)
        {
            if (entry.mapped->state == ObjectStorageQueueIFileMetadata::FileStatus::State::Failed)
            {
                failed_generations[entry.mapped->path] = entry.mapped->generation.load();

                /// Also build cache_entries for non-empty branch
                SipHash path_hash;
                path_hash.update(entry.mapped->path);
                auto node_name = toString(path_hash.get64());
                auto failed_node_path = fs::path(failed_path) / node_name;
                cache_entries.emplace_back(
                    entry.mapped->path,
                    failed_node_path.string(),
                    failed_node_path.string() + ".retriable",
                    entry.mapped->generation.load());
            }
        }
    }

    /// Now check Keeper AFTER cache snapshot
    Strings keeper_failed_nodes;
    Coordination::Error code = {};
    zk_retries.retryLoop([&]
    {
        code = zk_client->tryGetChildren(failed_path, keeper_failed_nodes);
    });

    if (code == Coordination::Error::ZNONODE || (code == Coordination::Error::ZOK && keeper_failed_nodes.empty()))
    {
        /// No /failed path or empty /failed means all failed files were deleted.

        /// Remove only entries whose generation still matches the snapshot
        size_t removed = removeStaleFailedCacheEntries(failed_generations);
        LOG_INFO(log, "Reconciled cache: removed {} Failed entries (no /failed nodes in Keeper)", removed);
        return;
    }

    if (code != Coordination::Error::ZOK)
    {
        LOG_WARNING(log, "Failed to list /failed nodes for cache reconciliation: {}", magic_enum::enum_name(code));
        return;
    }

    if (cache_entries.empty())
    {
        LOG_TRACE(log, "No Failed cache entries to reconcile");
        return;
    }

    /// Check existence of both terminal and retriable nodes for each cache entry in batches.
    /// Only remove cache entries if BOTH nodes are confirmed absent (ZNONODE).
    /// This preserves actively-retrying files that have only .retriable nodes.
    std::unordered_set<std::string> paths_to_remove;
    const size_t batch_size = keeper_multiread_batch_size;

    for (size_t i = 0; i < cache_entries.size(); i += batch_size)
    {
        size_t batch_end = std::min(i + batch_size, cache_entries.size());
        std::vector<std::string> batch_paths;
        batch_paths.reserve(2 * (batch_end - i));  // 2 paths per cache entry

        for (size_t j = i; j < batch_end; ++j)
        {
            batch_paths.push_back(std::get<1>(cache_entries[j]));  // terminal node
            batch_paths.push_back(std::get<2>(cache_entries[j]));  // retriable node
        }

        zkutil::ZooKeeper::MultiTryGetResponse response;
        zk_retries.resetFailures();
        zk_retries.retryLoop([&]
        {
            response = zk_client->tryGet(batch_paths);
        });

        for (size_t j = 0; j < response.size(); j += 2)
        {
            bool terminal_absent = (response[j].error == Coordination::Error::ZNONODE);
            bool retriable_absent = (response[j + 1].error == Coordination::Error::ZNONODE);

            /// Only mark for removal if BOTH terminal and retriable nodes are confirmed absent
            if (terminal_absent && retriable_absent)
            {
                paths_to_remove.insert(std::get<0>(cache_entries[i + j / 2]));
            }
        }
    }

    /// Remove cache entries confirmed absent in Keeper
    size_t removed = removeStaleFailedCacheEntries(failed_generations,
        [&paths_to_remove](const std::string & path) { return paths_to_remove.contains(path); });
    LOG_INFO(log, "Reconciled cache: removed {} Failed entries confirmed absent in Keeper", removed);
}

/// The cleanup lock's value tells a replica finding the lock held what is holding it. For a manual drop
/// it is `manual_drop_failed:<command_id>`: the prefix distinguishes it from the background sweep's
/// `background_cleanup`, and the id identifies the statement, so a waiter can tell one attempt of the
/// command it waits for from a different command that happened to take the path next.
static constexpr const char * LOCK_OPERATION_DROP_FAILED_PREFIX = "manual_drop_failed:";

namespace
{
    /// The command id from a lock value, or empty when the value is not a manual drop's.
    std::string extractDropCommandId(const std::string & lock_value)
    {
        const std::string_view prefix{LOCK_OPERATION_DROP_FAILED_PREFIX};
        if (!lock_value.starts_with(prefix))
            return {};
        return lock_value.substr(prefix.size());
    }
}

ObjectStorageQueueMetadata::WaitOutcome ObjectStorageQueueMetadata::waitForConcurrentDropToComplete(std::shared_ptr<ZooKeeperWithFaultInjection> zk_client, const fs::path & zookeeper_cleanup_lock_path)
{
    /// Lock is held by another process. Check if it's another dropFailedFiles invocation.
    /// When invoked via ON CLUSTER, multiple replicas attempt this concurrently;
    /// if another dropFailedFiles holds the lock, treat it as idempotent success.
    /// If the generic background cleanup holds the lock, it may not be cleaning failed files,
    /// so we must fail and let the user retry.
    try
    {
        /// The window this failpoint opens is the whole point of `LockVanished`: between our `tryCreate`
        /// failing and this `get`, the holder can finish and release the lock, and then there is no
        /// attempt left to bind to. It is microseconds wide in production, so a test cannot hit it by
        /// racing for it.
        FailPointInjection::pauseFailPoint(FailPoints::object_storage_queue_pause_before_cleanup_lock_read);

        /// Bind to the command holding the lock, not to the lock node. A command retries by taking the
        /// lock again, which gives the node a new `czxid`; binding to that would make every retry look
        /// like a different operation and leave this waiter unable to accept the result of the very
        /// command it is waiting for.
        Coordination::Stat lock_stat;
        std::string lock_value = zk_client->get(zookeeper_cleanup_lock_path, &lock_stat);
        const std::string waited_command_id = extractDropCommandId(lock_value);

        if (!waited_command_id.empty())
        {
            /// Another replica is executing the same operation. Wait for it to complete and take its
            /// verdict from the result it publishes, rather than from a wall-clock timeout that could be
            /// too short for large backlogs.
            LOG_INFO(log, "Another replica is executing SYSTEM DROP S3QUEUE FAILED FILES, waiting for completion");

            /// Wait for the lock to be released, then read the result the winner published under it.
            ///
            /// Liveness is taken from the lock itself, not from the `/failed` node count. The lock is
            /// ephemeral, so a winner that dies takes it with it on session expiry and the `ZNONODE` path
            /// below runs; a winner that is alive holds it. The node count cannot tell those apart: it is
            /// written by every replica that fails a file, so deletions and fresh failures at similar
            /// rates hold it flat while the winner is working normally - the same false positive that made
            /// the loser reject a healthy winner's cleanup.
            static constexpr size_t POLL_INTERVAL_MS = 100;
            static constexpr size_t ABSOLUTE_MAX_WAIT_MS = 1800000;      /// 30 min absolute cap (safety net)

            const size_t max_total_iterations = ABSOLUTE_MAX_WAIT_MS / POLL_INTERVAL_MS;

            for (size_t i = 0; i < max_total_iterations; ++i)
            {
                sleepForMilliseconds(POLL_INTERVAL_MS);

                bool attempt_finished = false;
                try
                {
                    /// Poll the lock. The command being waited on is over when the lock is gone, or when
                    /// the value at that path names a different command - the path is reused, so a
                    /// release and a re-acquisition inside one poll interval is invisible to a plain
                    /// existence check, and continuing to wait would silently transfer this waiter onto
                    /// an operation that started after it did and says nothing about it.
                    ///
                    /// The same command id means the command retried after losing its session. That is
                    /// still the command being waited for, so keep waiting: its verdict is still coming.
                    Coordination::Stat poll_stat;
                    std::string poll_value;
                    if (!zk_client->tryGet(zookeeper_cleanup_lock_path, poll_value, &poll_stat))
                        attempt_finished = true;
                    else if (extractDropCommandId(poll_value) != waited_command_id)
                        attempt_finished = true;
                }
                catch (const Coordination::Exception & poll_e)
                {
                    /// Transient during polling; the next iteration retries.
                    LOG_TEST(log, "Transient Keeper error while polling the cleanup lock: {}", poll_e.displayText());
                }

                if (attempt_finished)
                {
                    /// The attempt is over, which does not by itself mean it succeeded - it could have
                    /// partially failed - so read the result it published.
                    size_t terminal_failed_count = 0;
                    if (verifyCleanupSucceeded(zk_client,
                            fmt::format("Cleanup command finished after {}ms, verifying cleanup succeeded", (i + 1) * 100),
                            waited_command_id, terminal_failed_count))
                        return WaitOutcome::CommandCompleted;

                    throw Exception(ErrorCodes::KEEPER_EXCEPTION,
                        "The replica holding the cleanup lock published no usable result and {} terminal failed nodes "
                        "remain in /failed, so the cleanup cannot be confirmed. Please retry the command.",
                        terminal_failed_count);
                }

                /// Hit absolute safety-net timeout
                if (i == max_total_iterations - 1)
                {
                    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                        "Cleanup did not complete within {} minute safety-net timeout. "
                        "The winner may be processing an extremely large backlog. Please retry or investigate.",
                        ABSOLUTE_MAX_WAIT_MS / 60000);
                }
            }
        }
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNONODE)
        {
            /// The lock node disappeared between our `tryCreate` and this `get`. There is no attempt to
            /// bind to - its `Stat` was never read - and we do not even know whose lock it was, a manual
            /// drop or the background sweep.
            ///
            /// Nothing here can be verified, and trying to was the bug: with no command id the only
            /// observation left was "is `/failed` empty", which is a stronger postcondition than any
            /// winner enforces. A winner deletes the snapshot it opened with and is not answerable for
            /// files that fail afterwards, so a single new failure arriving after the winner's snapshot
            /// made this waiter throw about a cleanup that had in fact succeeded - the ordinary
            /// interleaving on a queue that is actively failing files, not a rare one.
            ///
            /// The answer is not a better verification but no verification: the lock is ephemeral and it
            /// is gone, so nobody holds it and nobody is doing the work. Start the attempt over and try
            /// to take it. Doing the drop ourselves is correct whether the previous holder was a drop
            /// that finished, a drop that died, or the background sweep - and re-deleting an
            /// already-deleted node is a no-op, so a redundant attempt costs correctness nothing.
            LOG_INFO(log, "The cleanup lock was released before it could be read, so there is no attempt "
                          "to wait on; retrying the drop from the start");
            return WaitOutcome::LockVanished;
        }

        /// For other errors (connection issues, etc.), treat as a transient error.
        LOG_WARNING(log, "Failed to read cleanup lock: {}. Will ask user to retry.", e.displayText());
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Failed file cleanup cannot proceed: another operation is holding the cleanup lock. "
        "Please retry in a moment.");
}
void ObjectStorageQueueMetadata::publishDropResult(const std::shared_ptr<ZooKeeperWithFaultInjection> & zk_client,
    const std::string & command_id, const std::string & attempt_id,
    bool success, size_t snapshot_size, size_t deleted, const std::string & error)
{
    Poco::JSON::Object json;
    /// What a waiter matches on: stable for the whole statement, including across its retries.
    json.set("command_id", command_id);
    /// Which attempt of that statement produced the result. Kept for the writer-side ownership check and
    /// for diagnosis; a decimal string rather than a number, because `czxid` is 64-bit and JSON numbers
    /// are not required to carry that range exactly.
    json.set("attempt_id", attempt_id);
    json.set("success", success);
    json.set("snapshot_size", snapshot_size);
    json.set("deleted", deleted);
    json.set("error", error);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);

    /// Overwritten in place, so the node stays single and needs no pruning. Keeper bumps its version on
    /// every set, and that version is the attempt id a waiting replica compares against.
    zk_client->createOrUpdate(zookeeper_path / "last_drop_result", oss.str(), zkutil::CreateMode::Persistent);
}

bool ObjectStorageQueueMetadata::verifyCleanupSucceeded(std::shared_ptr<ZooKeeperWithFaultInjection> zk_client, const std::string & context_msg,
    const std::string & waited_command_id, size_t & out_terminal_failed_count)
{
    LOG_INFO(log, "{}", context_msg);

    out_terminal_failed_count = 0;

    /// The winner publishes what it actually did before releasing the lock, so ask it rather than
    /// inferring the answer from `/failed`. Inference cannot work here: the winner deletes the snapshot
    /// of terminal nodes it took when it started and is not responsible for files that fail afterwards,
    /// while `/failed` has other writers, so "not empty" says nothing about whether the winner succeeded.
    ///
    /// The result is matched by command identity, not by "the marker moved since I started waiting".
    /// Ordering cannot answer this: the lock path is reused, so a later and entirely unrelated command
    /// publishes a newer result too, and adopting it would report a verdict about a cleanup this waiter
    /// never waited for. Nor can the attempt's `czxid` answer it, since the command it waits for may
    /// have retried and published under a different one.
    /// Every caller now binds to a command before waiting: the one path that used to arrive here with no
    /// id - the lock vanishing before it could be read - retries the whole attempt instead of trying to
    /// verify something it cannot name.
    chassert(!waited_command_id.empty());

    std::string marker_value;
    if (zk_client->tryGet(zookeeper_path / "last_drop_result", marker_value))
    {
        Poco::JSON::Parser parser;
        auto json = parser.parse(marker_value).extract<Poco::JSON::Object::Ptr>();
        chassert(json);

        if (json->getValue<std::string>("command_id") == waited_command_id)
        {
            const bool success = json->getValue<bool>("success");
            const size_t snapshot_size = json->getValue<size_t>("snapshot_size");
            const size_t deleted = json->getValue<size_t>("deleted");

            if (success)
            {
                LOG_INFO(log, "Winner replica reported success: dropped {} of {} failed files it had selected",
                         deleted, snapshot_size);
                reconcileFailedFilesCache();
                return true;
            }

            /// A partial failure is the winner's own verdict, reported with the winner's own numbers.
            throw Exception(ErrorCodes::KEEPER_EXCEPTION,
                "Failed file cleanup on the replica holding the lock did not complete: {}. "
                "It dropped {} of the {} failed files it had selected. Please retry the command.",
                json->getValue<std::string>("error"), deleted, snapshot_size);
        }

        /// The marker belongs to some other command: the one waited on either failed every attempt without
        /// publishing, or another command has already overwritten its result - the marker is a single node
        /// kept in place. That verdict is unrecoverable, so claim nothing about it and fall through to what
        /// can still be observed directly.
        LOG_INFO(log, "The drop result in Keeper was published by a different command, so it says nothing "
                      "about the one this waiter waited for");
    }

    /// No usable result: the command died before publishing anything, or a later command has already
    /// overwritten the marker - it is a single node kept in place. Either way the snapshot that attempt
    /// worked on is unknown here, and the only statement still available is about `/failed` as a whole.
    /// This is weaker than what a winner guarantees, so it can only ever confirm success, never diagnose
    /// a failure; a non-empty `/failed` leaves the caller to report that the cleanup is unconfirmed.
    LOG_INFO(log, "No drop result is available for the command that was waited on, falling back to checking /failed");

    const std::string failed_path = zookeeper_path / "failed";
    Strings remaining_failed_nodes;
    Coordination::Error check_code = zk_client->tryGetChildren(failed_path, remaining_failed_nodes);

    if (check_code != Coordination::Error::ZOK && check_code != Coordination::Error::ZNONODE)
    {
        /// Transient Keeper error during verification; safe to treat as unknown state
        throw Exception(ErrorCodes::KEEPER_EXCEPTION,
            "Failed to verify cleanup completion (Keeper error: {}). Please retry the command.",
            magic_enum::enum_name(check_code));
    }

    /// Count only terminal failed nodes (exclude .retriable suffix nodes, which are preserved)
    size_t terminal_failed_count = 0;
    for (const auto & node : remaining_failed_nodes)
    {
        if (!node.ends_with(".retriable"))
            ++terminal_failed_count;
    }

    out_terminal_failed_count = terminal_failed_count;

    if (terminal_failed_count == 0)
    {
        /// Cleanup succeeded: no terminal failed nodes remain
        reconcileFailedFilesCache();
        LOG_INFO(log, "Verified cleanup completed successfully");
        return true;
    }

    return false;
}

void ObjectStorageQueueMetadata::deleteFailedNodeBatch(
    const Coordination::Requests & remove_requests,
    const std::vector<std::string> & batch_file_paths,
    const std::unordered_map<std::string, uint64_t> & failed_generations,
    const std::shared_ptr<ZooKeeperWithFaultInjection> & zk_client,
    std::string_view batch_description,
    size_t report_batch_index,
    size_t & total_deleted,
    std::vector<std::string> & file_paths,
    std::vector<std::pair<size_t, Coordination::Error>> & failed_batches)
{
    Coordination::Responses remove_responses;
    /// Pinned to the session that owns the cleanup lock, and not retried: a hardware error here means the
    /// session may be gone, and with it the lock. Retrying would delete nodes on a session that holds no
    /// lock, racing whichever replica has legitimately taken it. The caller catches and restarts instead.
    Coordination::Error code = zk_client->tryMulti(remove_requests, remove_responses);

    if (code == Coordination::Error::ZOK)
    {
        /// Clear cache immediately for successfully deleted znodes
        for (size_t k = batch_file_paths.size() - remove_requests.size(); k < batch_file_paths.size(); ++k)
        {
            const auto & file_path = batch_file_paths[k];
            removeFromCacheIfGenerationMatches(file_path, failed_generations);
            file_paths.push_back(file_path);
        }
        total_deleted += remove_requests.size();
    }
    else
    {
        /// Partial success: reconcile individual responses.
        /// Some operations may have succeeded even if the multi request returned non-ZOK.
        size_t batch_succeeded = 0;
        size_t batch_start_idx = batch_file_paths.size() - remove_requests.size();

        for (size_t k = 0; k < remove_requests.size(); ++k)
        {
            if (remove_responses[k]->error == Coordination::Error::ZOK)
            {
                /// This specific remove succeeded - update cache
                const auto & file_path = batch_file_paths[batch_start_idx + k];
                removeFromCacheIfGenerationMatches(file_path, failed_generations);
                file_paths.push_back(file_path);
                ++batch_succeeded;
            }
            else if (remove_responses[k]->error == Coordination::Error::ZRUNTIMEINCONSISTENCY)
            {
                /// Request was not processed because multi was aborted - retry individually, on the same
                /// session for the same reason as above.
                Coordination::Error retry_code = zk_client->tryRemove(remove_requests[k]->getPath());

                if (retry_code == Coordination::Error::ZOK || retry_code == Coordination::Error::ZNONODE)
                {
                    /// ZOK: retry succeeded. ZNONODE: first attempt already deleted the node
                    /// before the multi aborted. Either way, the node is gone - clear cache.
                    const auto & file_path = batch_file_paths[batch_start_idx + k];
                    removeFromCacheIfGenerationMatches(file_path, failed_generations);
                    file_paths.push_back(file_path);
                    ++batch_succeeded;

                    if (retry_code == Coordination::Error::ZNONODE)
                        LOG_TRACE(log, "Node `{}` already removed (likely by first attempt before multi aborted)",
                                  remove_requests[k]->getPath());
                }
                else
                {
                    LOG_ERROR(log, "Failed to remove node `{}` after retry (code: {})",
                        remove_requests[k]->getPath(), magic_enum::enum_name(retry_code));
                }
            }
            else
            {
                LOG_ERROR(log, "Failed to remove node `{}` (code: {})",
                    remove_requests[k]->getPath(), magic_enum::enum_name(remove_responses[k]->error));
            }
        }

        total_deleted += batch_succeeded;

        if (batch_succeeded < remove_requests.size())
        {
            LOG_WARNING(log, "{} remove of failed nodes: {}/{} succeeded, overall status: {}",
                batch_description, batch_succeeded, remove_requests.size(), magic_enum::enum_name(code));
            failed_batches.emplace_back(report_batch_index, code);
        }
    }
}

bool ObjectStorageQueueMetadata::stillHoldsCleanupLock(const std::shared_ptr<ZooKeeperWithFaultInjection> & zk_client,
    const fs::path & zookeeper_cleanup_lock_path, const std::string & attempt_id) const
{
    Coordination::Stat lock_stat;
    std::string lock_value;
    if (!zk_client->tryGet(zookeeper_cleanup_lock_path, lock_value, &lock_stat))
        return false;
    return toString(lock_stat.czxid) == attempt_id;
}

void ObjectStorageQueueMetadata::dropFailedFiles()
{
    if (!isUnordered(mode))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "SYSTEM DROP S3QUEUE FAILED FILES is only supported for unordered mode tables. "
            "Support for ordered mode will be added in a future release.");

    /// The work under the cleanup lock is not retried operation by operation, because a Keeper session
    /// error there may mean the ephemeral lock is gone; retrying would act on a session holding no lock.
    /// The whole command is retried instead: a new attempt takes the lock again and starts from a fresh
    /// listing. Re-deleting an already deleted node is a no-op, so an attempt costs correctness nothing.
    ///
    /// Two things send an attempt back here: losing the Keeper session while holding the lock, and
    /// finding the lock already gone when going to read whose it was. The second is why a waiter never
    /// has to guess whether an unnamed predecessor succeeded - it just takes the lock and does the work.
    static constexpr size_t MAX_ATTEMPTS = 3;

    /// Generated once for the whole statement, so every attempt takes the lock under the same identity.
    /// A replica waiting on this command matches results by this id: were it per attempt, a retry would
    /// look to the waiter like a different operation and its verdict would be rejected.
    const std::string command_id = toString(UUIDHelpers::generateV4());

    for (size_t attempt = 0; attempt < MAX_ATTEMPTS; ++attempt)
    {
        if (tryDropFailedFilesOnce(command_id))
            return;

        LOG_INFO(log, "Attempt {} of {} to drop failed files reached no verdict - it either lost its Keeper "
                      "session before publishing a result, or the lock it meant to wait on was gone before "
                      "it could be read - starting over", attempt + 1, MAX_ATTEMPTS);
    }

    throw Exception(ErrorCodes::KEEPER_EXCEPTION,
        "Could not drop failed files within {} attempts: each one either lost its Keeper session while "
        "holding the cleanup lock, or lost the lock to another replica and then found it released again "
        "before it could be read. Please retry the command.", MAX_ATTEMPTS);
}

bool ObjectStorageQueueMetadata::tryDropFailedFilesOnce(const std::string & command_id)
{
    const fs::path zookeeper_cleanup_lock_path = zookeeper_path / "cleanup_lock";
    const auto zk_client = getZooKeeper();

    /// Acquire the same distributed lock used by the periodic cleanup sweep
    /// to prevent concurrent modification of failed files.
    /// The lock value is `manual_drop_failed:<command_id>`: the prefix distinguishes a manual drop from
    /// the generic background cleanup, and the id lets a waiting replica follow this statement across
    /// the attempts it may make.
    auto ephemeral_node = zkutil::EphemeralNodeHolder::tryCreate(
        zookeeper_cleanup_lock_path, *zk_client->getKeeper(), LOCK_OPERATION_DROP_FAILED_PREFIX + command_id);

    if (!ephemeral_node)
    {
        /// `LockVanished` is not a failure: it means nobody holds the lock any more, so there is neither
        /// an attempt to wait on nor anyone doing the work. Reporting the attempt unfinished sends it
        /// back to the retry loop, which tries to take the lock and drop the files itself.
        if (waitForConcurrentDropToComplete(zk_client, zookeeper_cleanup_lock_path) == WaitOutcome::LockVanished)
            return false;
        return true;
    }

    /// This attempt's identity, published with its result so a waiting replica can tell this attempt's
    /// verdict from any other attempt's. `EphemeralNodeHolder` does not hand back the `Stat`, so the
    /// `czxid` costs one extra read of the node just created - once per command.
    Coordination::Stat lock_stat;
    zk_client->get(zookeeper_cleanup_lock_path, &lock_stat);
    const std::string attempt_id = toString(lock_stat.czxid);

    /// Everything below runs pinned to `zk_client`, the session that owns the lock, and without retries.
    /// A hardware error means that session may be gone - and the ephemeral lock with it - so the attempt
    /// gives up rather than continuing on a session that holds nothing. `setAlreadyRemoved` keeps the
    /// holder's destructor from deleting a lock node that by then may belong to another replica.
    try
    {
        return dropFailedFilesUnderLock(zk_client, ephemeral_node, zookeeper_cleanup_lock_path, command_id, attempt_id);
    }
    catch (const Coordination::Exception & e)
    {
        if (!Coordination::isHardwareError(e.code))
            throw;

        LOG_WARNING(log, "Keeper error while holding the cleanup lock: {}. The lock may no longer be ours, "
                         "so this attempt is abandoned without publishing a result.", e.displayText());
        ephemeral_node->setAlreadyRemoved();
        return false;
    }
}

bool ObjectStorageQueueMetadata::dropFailedFilesUnderLock(
    const std::shared_ptr<ZooKeeperWithFaultInjection> & zk_client,
    const zkutil::EphemeralNodeHolder::Ptr & ephemeral_node,
    const fs::path & zookeeper_cleanup_lock_path,
    const std::string & command_id,
    const std::string & attempt_id)
{
    /// Publishing is the one write a waiting replica depends on, so the lock is re-checked immediately
    /// before it. The window between the check and the write cannot be closed from here, but a stale
    /// publish is the failure that matters most - it overwrites the legitimate holder's verdict.
    auto publish_if_still_ours = [&](bool success, size_t snapshot_size, size_t deleted, const std::string & error)
    {
        if (!stillHoldsCleanupLock(zk_client, zookeeper_cleanup_lock_path, attempt_id))
        {
            LOG_WARNING(log, "The cleanup lock is no longer held by this attempt, so its result is not published");
            ephemeral_node->setAlreadyRemoved();
            return false;
        }
        publishDropResult(zk_client, command_id, attempt_id, success, snapshot_size, deleted, error);
        return true;
    };

    const std::string failed_path = zookeeper_path / "failed";

    /// Get list of failed file nodes
    Strings failed_nodes;
    Coordination::Error code = zk_client->tryGetChildren(failed_path, failed_nodes);

    if (code == Coordination::Error::ZNONODE)
    {
        /// No failed path exists yet - nothing to drop.
        /// Reconcile cache to clear any stale entries before returning.
        LOG_TRACE(log, "Failed files path does not exist, nothing to drop");
        if (!publish_if_still_ours(/* success */ true, /* snapshot_size */ 0, /* deleted */ 0, /* error */ ""))
            return false;
        reconcileFailedFilesCache();
        return true;
    }

    if (code != Coordination::Error::ZOK)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to list failed files: {}", magic_enum::enum_name(code));

    if (failed_nodes.empty())
    {
        /// No failed files to drop (or only .retriable nodes remain).
        /// Reconcile cache to clear any stale entries before returning.
        LOG_TRACE(log, "No failed files to drop");
        if (!publish_if_still_ours(/* success */ true, /* snapshot_size */ 0, /* deleted */ 0, /* error */ ""))
            return false;
        reconcileFailedFilesCache();
        return true;
    }

    LOG_TRACE(log, "Dropping {} failed files", failed_nodes.size());

    /// Read metadata and delete only the specific nodes from our snapshot
    /// to avoid race with concurrent failures
    std::vector<std::string> file_paths;
    file_paths.reserve(failed_nodes.size());

    std::filesystem::path failed_fs_path(failed_path);

    /// Process in batches for both reading metadata and deleting nodes
    const size_t batch_size = keeper_multiread_batch_size;
    static constexpr size_t keeper_multi_batch_size = 100;

    /// Track failed deletions to report at the end
    std::vector<std::pair<size_t, Coordination::Error>> failed_batches;
    /// Track successful deletions for partial success reporting
    size_t total_deleted = 0;

    /// Snapshot generations of all Failed files before starting Keeper deletes
    /// to prevent race where file re-fails with new generation before cache removal
    std::unordered_map<std::string, uint64_t> failed_generations;
    {
        auto all_entries = local_file_statuses.dump();
        for (const auto & entry : all_entries)
        {
            if (entry.mapped->state == ObjectStorageQueueIFileMetadata::FileStatus::State::Failed)
                failed_generations[entry.mapped->path] = entry.mapped->generation.load();
        }
    }

    for (size_t i = 0; i < failed_nodes.size(); i += batch_size)
    {
        size_t batch_end = std::min(i + batch_size, failed_nodes.size());
        std::vector<std::string> batch_paths;
        batch_paths.reserve(batch_end - i);

        for (size_t j = i; j < batch_end; ++j)
        {
            /// Skip retry-state nodes - only drop terminal failed nodes.
            /// See comment in cleanupTrackedNodes for rationale.
            if (failed_nodes[j].ends_with(".retriable"))
                continue;

            batch_paths.push_back(failed_fs_path / failed_nodes[j]);
        }

        /// Read metadata for this batch
        zkutil::ZooKeeper::MultiTryGetResponse response = zk_client->tryGet(batch_paths);

        /// Delete nodes from this batch and collect file paths for immediate cache cleanup
        Coordination::Requests remove_requests;
        std::vector<std::string> batch_file_paths;
        remove_requests.reserve(std::min(batch_paths.size(), keeper_multi_batch_size));
        batch_file_paths.reserve(batch_paths.size());

        for (size_t j = 0; j < response.size(); ++j)
        {
            const auto & zk_path = batch_paths[j];

            if (response[j].error == Coordination::Error::ZNONODE)
            {
                LOG_TEST(log, "Failed file node already deleted: {}", zk_path);
                continue;
            }

            if (response[j].error != Coordination::Error::ZOK)
            {
                LOG_ERROR(log, "Failed to fetch metadata for {}: {}", zk_path, magic_enum::enum_name(response[j].error));
                continue;
            }

            auto metadata = ObjectStorageQueueIFileMetadata::NodeMetadata::fromString(response[j].data);
            batch_file_paths.push_back(metadata.file_path);
            LOG_TEST(log, "Read metadata for failed file: {}", metadata.file_path);

            remove_requests.push_back(zkutil::makeRemoveRequest(zk_path, -1));

            /// Execute batch removes when we hit the limit
            if (remove_requests.size() >= keeper_multi_batch_size)
            {
                deleteFailedNodeBatch(
                    remove_requests, batch_file_paths, failed_generations, zk_client,
                    "Batch", i, total_deleted, file_paths, failed_batches);

                remove_requests.clear();
            }
        }

        /// Remove any remaining nodes in this batch
        if (!remove_requests.empty())
        {
            deleteFailedNodeBatch(
                remove_requests, batch_file_paths, failed_generations, zk_client,
                "Final batch", i, total_deleted, file_paths, failed_batches);
        }
    }

    /// Report failures after attempting all batches, but note partial success
    if (!failed_batches.empty())
    {
        String error_msg = fmt::format(
            "Failed to remove {} batch(es) of failed file nodes ({} nodes successfully deleted before failure):",
            failed_batches.size(), total_deleted);
        for (const auto & [batch_idx, err] : failed_batches)
        {
            error_msg += fmt::format(" [batch starting at index {} failed with {}]", batch_idx, magic_enum::enum_name(err));
        }
        /// Published before the throw, and so before the lock is released, so the replicas waiting on this
        /// one are told the cleanup failed instead of having to guess it from what remains in `/failed`.
        ///
        /// Terminal: a partial failure is a verdict, and the caller does not retry it. Retrying would
        /// either contradict a verdict already published or withhold one, and a waiting replica has no way
        /// to reconcile that. Only an attempt that produced no verdict at all is started over.
        publish_if_still_ours(/* success */ false, failed_nodes.size(), total_deleted, error_msg);
        throw Exception(ErrorCodes::KEEPER_EXCEPTION, "{}", error_msg);
    }

    /// Published while the lock is still held: this replica deleted every terminal node of the snapshot it
    /// took when it started, which is all it is responsible for. Files that failed after that snapshot are
    /// not part of this attempt and must not make it look unsuccessful.
    if (!publish_if_still_ours(/* success */ true, failed_nodes.size(), total_deleted, /* error */ ""))
        return false;

    reconcileFailedFilesCache();
    LOG_INFO(log, "Successfully dropped {} failed files", file_paths.size());
    return true;
}

void ObjectStorageQueueMetadata::updateSettings(const SettingsChanges & changes)
{
    for (const auto & change : changes)
    {
        if (change.name == "cleanup_interval_min_ms")
            cleanup_interval_min_ms = change.value.safeGet<UInt64>();
        if (change.name == "cleanup_interval_max_ms")
            cleanup_interval_max_ms = change.value.safeGet<UInt64>();
        if (change.name == "use_persistent_processing_nodes")
            use_persistent_processing_nodes = change.value.safeGet<bool>();
        if (change.name == "persistent_processing_node_ttl_seconds")
            persistent_processing_node_ttl_seconds = change.value.safeGet<UInt64>();
    }
}

void ObjectStorageQueueMetadata::cleanupPersistentProcessingNodes(const std::shared_ptr<ZooKeeperWithFaultInjection> & zk_client)
{
    const fs::path zookeeper_persistent_processing_path = zookeeper_path / "processing";

    Strings persistent_processing_nodes;

    Coordination::Error code = {};
    code = zk_client->tryGetChildren(zookeeper_persistent_processing_path, persistent_processing_nodes);
    if (code != Coordination::Error::ZOK)
    {
        if (code == Coordination::Error::ZNONODE)
        {
            LOG_TEST(log, "Path {} does not exist", zookeeper_persistent_processing_path.string());
            return;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error: {}", magic_enum::enum_name(code));
    }

    Strings bucket_lock_paths;
    if (useBucketsForProcessing())
    {
        const auto buckets_path = zookeeper_path / "buckets";
        for (size_t i = 0; i < getBucketsNum(); ++i)
        {
            bucket_lock_paths.push_back(buckets_path / toString(i) / "lock");
        }
    }

    auto current_time = getCurrentTime();
    std::vector<std::pair<String, int32_t>> nodes_to_remove;
    Strings get_batch;
    auto get_paths = [&]
    {
        zkutil::ZooKeeper::MultiTryGetResponse response;
        response = zk_client->tryGet(get_batch);

        for (size_t i = 0; i < response.size(); ++i)
        {
            if (response[i].error == Coordination::Error::ZNONODE)
            {
                LOG_TEST(log, "Failed to fetch node metadata {}", get_batch[i]);
                continue;
            }

            LOG_TEST(
                log, "Node: {}, mtime: {}, ttl sec: {}, current time: {}",
                get_batch[i], response[i].stat.mtime, persistent_processing_node_ttl_seconds.load(), current_time);

            if (response[i].stat.mtime / 1000 + persistent_processing_node_ttl_seconds < current_time)
                nodes_to_remove.emplace_back(get_batch[i], response[i].stat.version);
        }
        get_batch.clear();
    };

    for (const auto & node : persistent_processing_nodes)
    {
        get_batch.push_back(zookeeper_persistent_processing_path / node);
        if (get_batch.size() == keeper_multiread_batch_size)
            get_paths();
    }
    for (const auto & node : bucket_lock_paths)
    {
        get_batch.push_back(node);
        if (get_batch.size() == keeper_multiread_batch_size)
            get_paths();
    }

    if (!get_batch.empty())
        get_paths();

    if (nodes_to_remove.empty())
    {
        if (!persistent_processing_nodes.empty())
            LOG_TRACE(log, "No persistent processing nodes to remove, "
                     "total persistent processing nodes: {}", persistent_processing_nodes.size());
        return;
    }

    size_t removed = 0;
    for (const auto & node_with_version : nodes_to_remove)
    {
        const auto & node = node_with_version.first;
        const auto version = node_with_version.second;
        LOG_TRACE(log, "Removing stale processing node: {}", node);
        code = zk_client->tryRemove(node, version);
        if (code == Coordination::Error::ZOK)
            ++removed;
        else if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZBADVERSION)
            LOG_TRACE(log, "Processing node {} was already removed or recreated, skipping", node);
        else
            throw zkutil::KeeperException::fromPath(code, node);
    }

    LOG_DEBUG(log, "Removed {}/{} stale processing nodes", removed, nodes_to_remove.size());
}

}
