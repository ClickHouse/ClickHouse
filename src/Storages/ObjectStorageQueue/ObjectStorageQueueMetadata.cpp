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
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>
#include <Storages/StorageSnapshot.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/randomSeed.h>
#include <Common/DNSResolver.h>
#include <Interpreters/DDLTask.h>
#include <shared_mutex>
#include <Core/ServerUUID.h>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueueCleanupMaxSetSizeOrTTLMicroseconds;
    extern const Event ObjectStorageQueueLockLocalFileStatusesMicroseconds;
};

namespace CurrentMetrics
{
    extern const Metric ObjectStorageQueueRegisteredServers;
};

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int REPLICA_ALREADY_EXISTS;
    extern const int SUPPORT_IS_DISABLED;
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
}

class ObjectStorageQueueMetadata::LocalFileStatuses
{
public:
    LocalFileStatuses() = default;

    FileStatuses getAll() const
    {
        auto lk = lock();
        return file_statuses;
    }

    FileStatusPtr get(const std::string & filename, bool create)
    {
        auto lk = lock();
        auto it = file_statuses.find(filename);
        if (it == file_statuses.end())
        {
            if (create)
                it = file_statuses.emplace(filename, std::make_shared<FileStatus>()).first;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "File status for {} doesn't exist", filename);
        }
        return it->second;
    }

    bool remove(const std::string & filename, bool if_exists)
    {
        auto lk = lock();
        auto it = file_statuses.find(filename);
        if (it == file_statuses.end())
        {
            if (if_exists)
                return false;
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "File status for {} doesn't exist", filename);
        }
        file_statuses.erase(it);
        return true;
    }

private:
    FileStatuses file_statuses;
    mutable std::mutex mutex;

    std::unique_lock<std::mutex> lock() const
    {
        auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::ObjectStorageQueueLockLocalFileStatusesMicroseconds);
        return std::unique_lock(mutex);
    }
};

ObjectStorageQueueMetadata::ObjectStorageQueueMetadata(
    ObjectStorageType storage_type_,
    const fs::path & zookeeper_path_,
    const ObjectStorageQueueTableMetadata & table_metadata_,
    size_t cleanup_interval_min_ms_,
    size_t cleanup_interval_max_ms_,
    bool use_persistent_processing_nodes_,
    size_t persistent_processing_nodes_ttl_seconds_,
    size_t keeper_multiread_batch_size_)
    : table_metadata(table_metadata_)
    , storage_type(storage_type_)
    , mode(table_metadata.getMode())
    , zookeeper_path(zookeeper_path_)
    , keeper_multiread_batch_size(keeper_multiread_batch_size_)
    , cleanup_interval_min_ms(cleanup_interval_min_ms_)
    , cleanup_interval_max_ms(cleanup_interval_max_ms_)
    , use_persistent_processing_nodes(use_persistent_processing_nodes_)
    , persistent_processing_node_ttl_seconds(persistent_processing_nodes_ttl_seconds_)
    , buckets_num(table_metadata_.getBucketsNum())
    , log(getLogger("StorageObjectStorageQueue(" + zookeeper_path_.string() + ")"))
    , local_file_statuses(std::make_shared<LocalFileStatuses>())
{
    LOG_TRACE(
        log, "Mode: {}, buckets: {}, processing threads: {}, result buckets num: {}, use persistent processing nodes: {}",
        table_metadata.mode, table_metadata.buckets.load(),
        table_metadata.processing_threads_num.load(), buckets_num, use_persistent_processing_nodes.load());
}

ObjectStorageQueueMetadata::~ObjectStorageQueueMetadata()
{
    shutdown();
}

ZooKeeperWithFaultInjection::Ptr ObjectStorageQueueMetadata::getZooKeeper(LoggerPtr log)
{
    auto context = Context::getGlobalContextInstance();
    auto zk_client = context->getZooKeeper();
    if (context->getSettingsRef()[Setting::s3queue_keeper_fault_injection_probability] != 0.0)
    {
        return ZooKeeperWithFaultInjection::createInstance(
            context->getSettingsRef()[Setting::s3queue_keeper_fault_injection_probability],
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

    bool need_cleanup_for_unordered = mode == ObjectStorageQueueMode::UNORDERED
        && (table_metadata.tracked_files_limit
            || table_metadata.tracked_files_ttl_sec
            || (use_persistent_processing_nodes && persistent_processing_node_ttl_seconds));
    bool need_cleanup_for_ordered = mode == ObjectStorageQueueMode::ORDERED
        && use_persistent_processing_nodes && persistent_processing_node_ttl_seconds;

    if (!task && (need_cleanup_for_unordered || need_cleanup_for_ordered))
    {
        task = Context::getGlobalContextInstance()->getSchedulePool().createTask(
            "ObjectStorageQueueCleanupFunc",
            [this] { cleanupThreadFunc(); });

        task->activate();
        task->scheduleAfter(
            generateRescheduleInterval(
                cleanup_interval_min_ms, cleanup_interval_max_ms));
    }
    if (!update_registry_thread)
        update_registry_thread = std::make_unique<ThreadFromGlobalPool>([this](){ updateRegistryFunc(); });
}

void ObjectStorageQueueMetadata::shutdown()
{
    shutdown_called = true;
    if (task)
        task->deactivate();
    if (update_registry_thread && update_registry_thread->joinable())
        update_registry_thread->join();
}

ObjectStorageQueueMetadata::FileStatuses ObjectStorageQueueMetadata::getFileStatuses() const
{
    return local_file_statuses->getAll();
}

ObjectStorageQueueMetadata::FileMetadataPtr ObjectStorageQueueMetadata::getFileMetadata(
    const std::string & path,
    ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr bucket_info)
{
    chassert(metadata_ref_count);
    auto file_status = local_file_statuses->get(path, /* create */true);
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
                log);
        case ObjectStorageQueueMode::UNORDERED:
            return std::make_shared<ObjectStorageQueueUnorderedFileMetadata>(
                zookeeper_path,
                path,
                file_status,
                table_metadata.loading_retries,
                *metadata_ref_count,
                use_persistent_processing_nodes,
                log);
    }
}

bool ObjectStorageQueueMetadata::useBucketsForProcessing() const
{
    return mode == ObjectStorageQueueMode::ORDERED && (buckets_num > 1);
}

ObjectStorageQueueMetadata::Bucket ObjectStorageQueueMetadata::getBucketForPath(const std::string & path) const
{
    return ObjectStorageQueueOrderedFileMetadata::getBucketForPath(path, buckets_num);
}

ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr
ObjectStorageQueueMetadata::tryAcquireBucket(const Bucket & bucket)
{
    return ObjectStorageQueueOrderedFileMetadata::tryAcquireBucket(zookeeper_path, bucket, use_persistent_processing_nodes, log);
}

void ObjectStorageQueueMetadata::alterSettings(const SettingsChanges & changes, const ContextPtr & context)
{
    bool is_initial_query = context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY ||
                            (context->getZooKeeperMetadataTransaction() && context->getZooKeeperMetadataTransaction()->isInitialQuery());

    const fs::path alter_settings_lock_path = zookeeper_path / "alter_settings_lock";
    zkutil::EphemeralNodeHolder::Ptr alter_settings_lock;
    auto zookeeper = getZooKeeper(log);

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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to take alter setting lock");

            sleepForMilliseconds(50);
        }
    }

    Coordination::Stat stat;
    auto metadata_str = zookeeper->get(fs::path(zookeeper_path) / "metadata", &stat);
    auto metadata_from_zk = ObjectStorageQueueTableMetadata::parse(metadata_str);
    auto new_table_metadata{table_metadata};

    for (const auto & change : changes)
    {
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
                LOG_TRACE(log, "Setting `buckets` already equals {}. "
                        "Will do nothing", value);
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
    ObjectStorageQueueOrderedFileMetadata::migrateToBuckets(zookeeper_path, value, /* prev_value */table_metadata.buckets);
    buckets_num = value;
    table_metadata.buckets = value;
}

ObjectStorageQueueTableMetadata ObjectStorageQueueMetadata::syncWithKeeper(
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
    else
    {
        metadata_paths = ObjectStorageQueueUnorderedFileMetadata::getMetadataPaths();
    }

    auto zk_retries = getKeeperRetriesControl(log);
    const auto table_metadata_path = zookeeper_path / "metadata";
    bool warned = false;

    zk_retries.retryLoop([&] { getZooKeeper(log)->createAncestors(zookeeper_path); });

    for (size_t i = 0; i < 1000; ++i)
    {
        Coordination::Requests requests;
        Coordination::Responses responses;
        std::optional<Coordination::Error> code;
        zk_retries.retryLoop([&]
        {
            auto zk_client = getZooKeeper(log);
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
                ObjectStorageQueueOrderedFileMetadata(
                    zookeeper_path,
                    table_metadata.last_processed_path,
                    std::make_shared<FileStatus>(),
                    /* bucket_info */nullptr,
                    buckets_num,
                    table_metadata.loading_retries,
                    noop,
                    /* use_persistent_processing_nodes */false, /// Processing nodes will not be created.
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

        static Info deserialize(const std::string & str)
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

void ObjectStorageQueueMetadata::registerActive(const StorageID & storage_id)
{
    const auto id = getProcessorID(storage_id);
    const auto table_path = zookeeper_path / "registry" / id;
    const auto self = Info::create(storage_id);

    Coordination::Error code;
    getKeeperRetriesControl(log).retryLoop([&]
    {
        code = getZooKeeper(log)->tryCreate(
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

    Coordination::Error code;
    const size_t max_tries = 1000;
    for (size_t i = 0; i < max_tries; ++i)
    {
        Coordination::Stat stat;
        std::string registry_str;

        Coordination::Requests requests;
        Coordination::Responses responses;

        zk_retries.retryLoop([&]
        {
            auto zk_client = getZooKeeper(log);
            bool registry_exists = zk_client->tryGet(registry_path, registry_str, &stat);
            if (registry_exists)
            {
                created_new_metadata = false;

                Strings registered;
                splitInto<','>(registered, registry_str);

                for (const auto & elem : registered)
                {
                    if (elem.empty())
                        continue;

                    auto info = Info::deserialize(elem);
                    if (info == self)
                    {
                        LOG_TRACE(log, "Table {} is already registered", self.table_id);
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
                    zkutil::addCheckNotExistsRequest(requests, *getZooKeeper(log), drop_lock_path);
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
        Coordination::Error code;
        zk_retries.retryLoop([&] { code = getZooKeeper(log)->tryGetChildren(registry_path, registered); });
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
            throw zkutil::KeeperException(code);
    }
    else
    {
        std::string registry_str;
        zk_retries.retryLoop([&] { getZooKeeper(log)->tryGet(registry_path, registry_str); });
        if (!registry_str.empty())
            splitInto<','>(registered, registry_str);
    }
    return registered;
}

void ObjectStorageQueueMetadata::unregisterActive(const StorageID & storage_id)
{
    const auto registry_path = zookeeper_path / "registry";
    const auto table_path = registry_path / getProcessorID(storage_id);

    Coordination::Error code;
    getKeeperRetriesControl(log).retryLoop([&] { code = getZooKeeper(log)->tryRemove(table_path); });

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
            zk_client = getZooKeeper(log);
            supports_remove_recursive = zk_client->isFeatureEnabled(DB::KeeperFeatureFlag::REMOVE_RECURSIVE);

            Coordination::Stat stat;
            std::string registry_str;
            bool node_exists = zk_client->tryGet(registry_path, registry_str, &stat);
            if (!node_exists)
            {
                LOG_WARNING(log, "Cannot unregister: registry does not exist");
                if (!is_retry)
                    chassert(false);
                return;
            }

            Strings registered;
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
                    requests.push_back(zkutil::makeRemoveRecursiveRequest(*zk_client, zookeeper_path, std::numeric_limits<uint32_t>::max()));
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
    size_t nodes_num;
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

    task->scheduleAfter(
        generateRescheduleInterval(
            cleanup_interval_min_ms, cleanup_interval_max_ms));
}

void ObjectStorageQueueMetadata::cleanupThreadFuncImpl()
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::ObjectStorageQueueCleanupMaxSetSizeOrTTLMicroseconds);

    const fs::path zookeeper_cleanup_lock_path = zookeeper_path / "cleanup_lock";
    const auto zk_client = getZooKeeper(log);
    auto zk_retries = getKeeperRetriesControl(log);

    /// Create a lock so that with distributed processing
    /// multiple nodes do not execute cleanup in parallel.
    auto ephemeral_node = zkutil::EphemeralNodeHolder::tryCreate(zookeeper_cleanup_lock_path, *zk_client->getKeeper(), toString(getCurrentTime()));
    if (!ephemeral_node)
    {
        LOG_TEST(log, "Cleanup is already being executed by another node");
        return;
    }
    /// TODO because of this lock we might not update local file statuses on time on one of the nodes.

    cleanupPersistentProcessingNodes();
    if (mode == ObjectStorageQueueMode::ORDERED)
        return;

    const fs::path zookeeper_processed_path = zookeeper_path / "processed";
    const fs::path zookeeper_failed_path = zookeeper_path / "failed";

    Strings processed_nodes;
    Coordination::Error code;
    zk_retries.retryLoop([&]
    {
        code = getZooKeeper(log)->tryGetChildren(zookeeper_processed_path, processed_nodes);
    });
    if (code != Coordination::Error::ZOK)
    {
        if (code == Coordination::Error::ZNONODE)
        {
            LOG_TEST(log, "Path {} does not exist", zookeeper_processed_path.string());
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error: {}", magic_enum::enum_name(code));
    }

    Strings failed_nodes;
    zk_retries.retryLoop([&]
    {
        code = getZooKeeper(log)->tryGetChildren(zookeeper_failed_path, failed_nodes);
    });
    if (code != Coordination::Error::ZOK)
    {
        if (code == Coordination::Error::ZNONODE)
            LOG_TEST(log, "Path {} does not exist", zookeeper_failed_path.string());
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error: {}", magic_enum::enum_name(code));
    }

    const size_t nodes_num = processed_nodes.size() + failed_nodes.size();
    if (!nodes_num)
    {
        LOG_TEST(log, "There are neither processed nor failed nodes (in {} and in {})",
                 zookeeper_processed_path.string(), zookeeper_failed_path.string());
        return;
    }

    chassert(table_metadata.tracked_files_limit || table_metadata.tracked_files_ttl_sec);
    const bool check_nodes_limit = table_metadata.tracked_files_limit > 0;
    const bool check_nodes_ttl = table_metadata.tracked_files_ttl_sec > 0;

    const bool nodes_limit_exceeded = nodes_num > table_metadata.tracked_files_limit;
    if ((!nodes_limit_exceeded || !check_nodes_limit) && !check_nodes_ttl)
    {
        LOG_TEST(log, "No limit exceeded");
        return;
    }

    LOG_TRACE(log, "Will check limits for {} nodes", nodes_num);

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
    auto fetch_nodes = [&](const Strings & nodes, const fs::path & base_path)
    {
        auto get_paths = [&]
        {
            zkutil::ZooKeeper::MultiTryGetResponse response;
            zk_retries.retryLoop([&]
            {
                response = zk_client->tryGet(paths);
            });

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

        for (const auto & node : nodes)
        {
            paths.push_back(base_path / node);
            if (paths.size() == keeper_multiread_batch_size)
                get_paths();
        }

        if (!paths.empty())
            get_paths();
    };

    fetch_nodes(processed_nodes, zookeeper_processed_path);
    fetch_nodes(failed_nodes, zookeeper_failed_path);

    auto get_nodes_str = [&]()
    {
        WriteBufferFromOwnString wb;
        for (const auto & [node, metadata] : sorted_nodes)
            wb << fmt::format("Node: {}, path: {}, timestamp: {};\n", node, metadata.file_path, metadata.last_processed_timestamp);
        return wb.str();
    };

    LOG_TEST(log, "Checking node limits (max size: {}, max age: {}) for {}",
             table_metadata.tracked_files_limit.load(), table_metadata.tracked_files_ttl_sec.load(), get_nodes_str());

    static constexpr size_t keeper_multi_batch_size = 100;
    Coordination::Requests remove_requests;
    Coordination::Responses remove_responses;
    remove_requests.reserve(keeper_multi_batch_size);
    remove_responses.reserve(keeper_multi_batch_size);

    size_t nodes_to_remove = check_nodes_limit && nodes_limit_exceeded ? nodes_num - table_metadata.tracked_files_limit : 0;

    const auto remove_nodes = [&](bool node_limit)
    {
        code = zk_client->tryMulti(remove_requests, remove_responses);

        if (code == Coordination::Error::ZOK)
        {
            if (node_limit)
                nodes_to_remove -= remove_requests.size();
        }
        else
        {
            for (size_t i = 0; i < remove_requests.size(); ++i)
            {
                if (remove_responses[i]->error == Coordination::Error::ZOK)
                {
                    if (node_limit)
                        --nodes_to_remove;
                }
                else if (remove_responses[i]->error == Coordination::Error::ZRUNTIMEINCONSISTENCY)
                {
                    /// requests with ZRUNTIMEINCONSISTENCY were not processed because the multi request was aborted before
                    /// so we try removing it again without multi requests
                    zk_retries.retryLoop([&]
                    {
                        code = getZooKeeper(log)->tryRemove(remove_requests[i]->getPath());
                    });
                    if (code == Coordination::Error::ZOK)
                    {
                        if (node_limit)
                            --nodes_to_remove;
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
    };

    for (const auto & node : sorted_nodes)
    {
        if (nodes_to_remove)
        {
            LOG_TRACE(log, "Removing node at path {} ({}) because max files limit is reached",
                     node.metadata.file_path, node.zk_path);

            local_file_statuses->remove(node.metadata.file_path, /* if_exists */true);
            remove_requests.push_back(zkutil::makeRemoveRequest(node.zk_path, -1));
            /// we either reach max multi batch size OR we already added maximum amount of nodes we want to delete based on the node limit
            if (remove_requests.size() == keeper_multi_batch_size || remove_requests.size() == nodes_to_remove)
                remove_nodes(/*node_limit=*/true);
        }
        else if (check_nodes_ttl)
        {
            UInt64 node_age = getCurrentTime() - node.metadata.last_processed_timestamp;
            if (node_age >= table_metadata.tracked_files_ttl_sec)
            {
                LOG_TRACE(log, "Removing node at path {} ({}) because file ttl is reached",
                        node.metadata.file_path, node.zk_path);

                local_file_statuses->remove(node.metadata.file_path, /* if_exists */true);
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

    LOG_TRACE(log, "Node limits check finished");
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

void ObjectStorageQueueMetadata::cleanupPersistentProcessingNodes()
{
    auto zk_retries = getKeeperRetriesControl(log);
    const fs::path zookeeper_persistent_processing_path = zookeeper_path / "processing";

    Strings persistent_processing_nodes;

    Coordination::Error code;
    zk_retries.retryLoop([&]
    {
        code = getZooKeeper(log)->tryGetChildren(zookeeper_persistent_processing_path, persistent_processing_nodes);
    });
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
    Strings nodes_to_remove;
    Strings get_batch;
    auto get_paths = [&]
    {
        zkutil::ZooKeeper::MultiTryGetResponse response;
        zk_retries.retryLoop([&]
        {
            response = getZooKeeper(log)->tryGet(get_batch);
        });

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
                nodes_to_remove.push_back(get_batch[i]);
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

    for (const auto & node : nodes_to_remove)
    {
        zk_retries.retryLoop([&]
        {
            code = getZooKeeper(log)->tryRemove(node);
        });
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
            throw zkutil::KeeperException::fromPath(code, node);
    }

    LOG_DEBUG(log, "Removed {} persistent processing nodes", nodes_to_remove.size());
}

}
