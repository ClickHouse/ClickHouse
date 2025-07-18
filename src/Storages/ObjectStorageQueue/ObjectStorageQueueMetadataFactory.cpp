#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadataFactory.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>

namespace CurrentMetrics
{
    extern const Metric ObjectStorageQueueShutdownThreads;
    extern const Metric ObjectStorageQueueShutdownThreadsActive;
    extern const Metric ObjectStorageQueueShutdownThreadsScheduled;
}

namespace DB
{

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
}

void ObjectStorageQueueFactory::unregisterTable(const StorageID & storage, bool if_exists)
{
    std::lock_guard lock(mutex);

    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shutdown was called");

    auto it = storages.find(storage);
    if (it == storages.end())
    {
        if (if_exists)
            return;

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Table with storage id {} is not registered", storage.getNameForLogs());
    }
    storages.erase(it);
}

void ObjectStorageQueueFactory::renameTable(const StorageID & from, const StorageID & to)
{
    std::lock_guard lock(mutex);
    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shutdown was called");

    auto from_it = storages.find(from);
    if (from_it == storages.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table with storage id {} is not registered", from.getNameForLogs());

    auto to_it = storages.find(to);
    if (to_it != storages.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table with storage id {} is already registered", to.getNameForLogs());

    storages.erase(from_it);
    storages.emplace(to);
}

void ObjectStorageQueueFactory::shutdown()
{
    std::vector<StorageID> shutdown_storages;
    {
        std::lock_guard lock(mutex);
        shutdown_called = true;
        shutdown_storages = std::vector<StorageID>(storages.begin(), storages.end());
    }

    auto log = getLogger("ObjectStorageFactory::shutdown");
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

    ThreadPoolCallbackRunnerLocal<void> runner(pool, "ObjectStorageQueueFactory::shutdown");

    LOG_DEBUG(log, "Will shutdown {} queue storages", shutdown_storages.size());

    for (const auto & storage : shutdown_storages)
    {
        runner([&]()
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
    const std::string & zookeeper_path,
    ObjectStorageQueueMetadataPtr metadata,
    const StorageID & storage_id)
{
    std::lock_guard lock(mutex);
    auto it = metadata_by_path.find(zookeeper_path);
    if (it == metadata_by_path.end())
    {
        it = metadata_by_path.emplace(zookeeper_path, std::move(metadata)).first;
        it->second.metadata->setMetadataRefCount(*it->second.ref_count);
    }
    else
    {
        auto & metadata_from_table = metadata->getTableMetadata();
        auto & metadata_from_keeper = it->second.metadata->getTableMetadata();

        metadata_from_table.checkEquals(metadata_from_keeper);
    }

    it->second.metadata->registerIfNot(storage_id, false);
    *it->second.ref_count += 1;
    return it->second.metadata;
}

void ObjectStorageQueueMetadataFactory::remove(const std::string & zookeeper_path, const StorageID & storage_id, bool remove_metadata_if_no_registered)
{
    std::lock_guard lock(mutex);
    auto it = metadata_by_path.find(zookeeper_path);

    if (it == metadata_by_path.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Metadata with zookeeper path {} does not exist", zookeeper_path);

    *it->second.ref_count -= 1;

    try
    {
        const auto registry_size = it->second.metadata->unregister(
            storage_id,
            /* active */ false,
            remove_metadata_if_no_registered);

        LOG_TRACE(log, "Remaining registry size: {}", registry_size);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (*it->second.ref_count == 0)
        metadata_by_path.erase(it);
}

std::unordered_map<std::string, ObjectStorageQueueMetadataFactory::FilesMetadataPtr> ObjectStorageQueueMetadataFactory::getAll()
{
    std::unordered_map<std::string, ObjectStorageQueueMetadataFactory::FilesMetadataPtr> result;
    for (const auto & [zk_path, metadata] : metadata_by_path)
        result.emplace(zk_path, metadata.metadata);
    return result;
}

}
