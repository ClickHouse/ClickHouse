#include <Storages/StreamingStorageRegistry.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/setThreadName.h>

namespace CurrentMetrics
{
    extern const Metric ObjectStorageQueueShutdownThreads;
    extern const Metric ObjectStorageQueueShutdownThreadsActive;
    extern const Metric ObjectStorageQueueShutdownThreadsScheduled;
}

namespace DB
{
namespace
{
std::string makeMetadataKey(const std::string & zookeeper_name, const std::string & zookeeper_path)
{
    if (zookeeper_name == zkutil::DEFAULT_ZOOKEEPER_NAME)
        return zookeeper_path;
    return zookeeper_name + ":" + zookeeper_path;
}
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
}

StreamingStorageRegistry & StreamingStorageRegistry::instance()
{
    static StreamingStorageRegistry ret;
    return ret;
}

size_t StreamingStorageRegistry::IdentityHash::operator()(const StorageID & storage_id) const
{
    SipHash hash;
    if (storage_id.hasUUID())
    {
        const auto & uuid = storage_id.uuid;
        hash.update(reinterpret_cast<const char *>(&uuid), sizeof(uuid));
    }
    else
    {
        hash.update(storage_id.database_name.data(), storage_id.database_name.size());
        hash.update(storage_id.table_name.data(), storage_id.table_name.size());
    }
    return hash.get64();
}

bool StreamingStorageRegistry::IdentityEqual::operator()(const StorageID & lhs, const StorageID & rhs) const
{
    const bool lhs_has = lhs.hasUUID();
    const bool rhs_has = rhs.hasUUID();

    /// Both have UUID — compare by UUID only.
    if (lhs_has && rhs_has)
        return lhs.uuid == rhs.uuid;

    /// Mixed: one has UUID, the other does not — treat as non-equal.
    /// This preserves the hash/equality contract: IdentityHash uses UUID for one
    /// and name for the other, producing different hashes. Equal elements must
    /// have identical hashes, so mixed cases must never compare equal.
    if (lhs_has != rhs_has)
        return false;

    /// Neither has UUID — fall back to name comparison.
    return lhs.database_name == rhs.database_name && lhs.table_name == rhs.table_name;
}

void StreamingStorageRegistry::registerTable(const StorageID & storage)
{
    std::lock_guard lock(mutex);

    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shutdown was called");

    const bool inserted = storages.emplace(storage).second;
    if (!inserted)
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table with storage id {} already registered", storage.getNameForLogs());

    LOG_TRACE(log, "Registered table: {}", storage.getNameForLogs());
}

void StreamingStorageRegistry::unregisterTable(const StorageID & storage, bool if_exists)
{
    std::lock_guard lock(mutex);

    if (shutdown_called)
    {
        /// We can call unregisterTable from table shutdown.
        return;
    }

    auto it = storages.find(storage);
    if (it == storages.end())
    {
        if (if_exists)
        {
            LOG_DEBUG(getLogger("ObjectStorageQueueFactory"), "Table does not exist, nothing to unregister");
            return;
        }

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Table with storage id {} is not registered", storage.getNameForLogs());
    }
    storages.erase(it);
    LOG_TRACE(log, "Unregistered table: {}", storage.getNameForLogs());
}

void StreamingStorageRegistry::renameTable(const StorageID & from, const StorageID & to)
{
    std::lock_guard lock(mutex);
    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shutdown was called");

    /// With UUID-based identity tracking, `find` locates the entry by UUID (when available),
    /// so batch renames (e.g., A↔B swap) correctly identify each table regardless of
    /// intermediate name collisions.
    auto from_it = storages.find(from);
    if (from_it == storages.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table with storage id {} is not registered", from.getNameForLogs());

    storages.erase(from_it);
    storages.emplace(to);
    LOG_TRACE(log, "Renamed table: {} -> {}", from.getNameForLogs(), to.getNameForLogs());
}

void StreamingStorageRegistry::shutdown()
{
    std::vector<StorageID> shutdown_storages;
    {
        std::lock_guard lock(mutex);
        shutdown_called = true;
        shutdown_storages = std::vector<StorageID>(storages.begin(), storages.end());
    }

    if (shutdown_storages.empty())
    {
        LOG_DEBUG(log, "There are no queue storages to shutdown");
        return;
    }

    static const auto default_shutdown_threads = 10;
    ThreadPool pool(
        CurrentMetrics::ObjectStorageQueueShutdownThreads,
        CurrentMetrics::ObjectStorageQueueShutdownThreadsActive,
        CurrentMetrics::ObjectStorageQueueShutdownThreadsScheduled,
        std::min<size_t>(default_shutdown_threads, shutdown_storages.size()));

    ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::OBJECT_STORAGE_SHUTDOWN);

    LOG_DEBUG(log, "Will shutdown {} queue storages", shutdown_storages.size());

    for (const auto & storage : shutdown_storages)
    {
        runner.enqueueAndKeepTrack([&]()
        {
            DatabaseCatalog::instance().tryGetTable(storage, Context::getGlobalContextInstance())->shutdown();
        });
    }

    runner.waitForAllToFinish();

    LOG_DEBUG(log, "Already shutdown {} queue storages", shutdown_storages.size());
}

ObjectStorageQueueMetadataFactory & ObjectStorageQueueMetadataFactory::instance()
{
    static ObjectStorageQueueMetadataFactory ret;
    return ret;
}

ObjectStorageQueueMetadataFactory::FilesMetadataPtr ObjectStorageQueueMetadataFactory::getOrCreate(
    const std::string & zookeeper_name,
    const std::string & zookeeper_path,
    ObjectStorageQueueMetadataPtr metadata,
    const StorageID & storage_id,
    bool & created_new_metadata)
{
    std::lock_guard lock(mutex);
    const auto metadata_key = makeMetadataKey(zookeeper_name, zookeeper_path);
    auto it = metadata_by_path.find(metadata_key);
    if (it == metadata_by_path.end())
    {
        it = metadata_by_path.emplace(metadata_key, std::move(metadata)).first;
        it->second.metadata->setMetadataRefCount(*it->second.ref_count);
    }
    else
    {
        auto & metadata_from_table = metadata->getTableMetadata();
        auto & metadata_from_keeper = it->second.metadata->getTableMetadata();

        metadata_from_table.checkEquals(metadata_from_keeper);
    }

    it->second.metadata->registerNonActive(storage_id, created_new_metadata);
    *it->second.ref_count += 1;
    return it->second.metadata;
}

void ObjectStorageQueueMetadataFactory::remove(
    const std::string & zookeeper_name,
    const std::string & zookeeper_path,
    const StorageID & storage_id,
    bool is_drop,
    bool keep_data_in_keeper)
{
    std::lock_guard lock(mutex);
    const auto metadata_key = makeMetadataKey(zookeeper_name, zookeeper_path);
    auto it = metadata_by_path.find(metadata_key);

    if (it == metadata_by_path.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Metadata with zookeeper path {} does not exist", metadata_key);

    *it->second.ref_count -= 1;

    LOG_TRACE(
        log, "Removed table {} (is drop: {}, keep data in keeper: {})",
        storage_id.getNameForLogs(), is_drop, keep_data_in_keeper);

    if (is_drop)
    {
        try
        {
            it->second.metadata->unregisterNonActive(
                storage_id,
                /* remove_metadata_if_no_registered */is_drop && !keep_data_in_keeper);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (*it->second.ref_count == 0)
        metadata_by_path.erase(it);
}

std::unordered_map<std::string, ObjectStorageQueueMetadataFactory::FilesMetadataPtr> ObjectStorageQueueMetadataFactory::getAll()
{
    std::unordered_map<std::string, ObjectStorageQueueMetadataFactory::FilesMetadataPtr> result;
    for (const auto & [key, metadata] : metadata_by_path)
    {
        result.emplace(key, metadata.metadata);
    }
    return result;
}

}
