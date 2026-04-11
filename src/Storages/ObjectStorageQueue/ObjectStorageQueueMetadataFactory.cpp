#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadataFactory.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
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
}

ObjectStorageQueueFactory & ObjectStorageQueueFactory::instance()
{
    static ObjectStorageQueueFactory ret;
    return ret;
}

void ObjectStorageQueueFactory::registerTable(const StorageID & storage)
{
    std::lock_guard lock(mutex);

    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shutdown was called");

    const bool inserted = storages.emplace(storage).second;
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table with storage id {} already registered", storage.getNameForLogs());

    LOG_TRACE(log, "Registered table: {}", storage.getNameForLogs());
}

void ObjectStorageQueueFactory::unregisterTable(const StorageID & storage, bool if_exists)
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

void ObjectStorageQueueFactory::renameTable(const StorageID & from, const StorageID & to)
{
    std::lock_guard lock(mutex);
    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shutdown was called");

    auto from_it = storages.find(from);
    if (from_it == storages.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table with storage id {} is not registered", from.getNameForLogs());

    storages.erase(from_it);

    auto to_it = storages.find(to);
    if (to_it == storages.end())
    {
        storages.emplace(to);
        LOG_TRACE(log, "Unregistered table: {}, registered table: {}", from.getNameForLogs(), to.getNameForLogs());
    }
    else
    {
        /// This could happen because of exchange/replace tables.
        LOG_TRACE(log, "Unregistered table: {}, table {} was already registered", from.getNameForLogs(), to.getNameForLogs());
    }
}

void ObjectStorageQueueFactory::shutdown()
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
