#include "CachedStatisticsStorage.h"
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{

CachedStatisticsStorage::CachedStatisticsStorage(UInt64 refresh_interval_sec_, const String & load_thread_name_)
    : loader(StatisticsLoader())
    , collector(StatisticsCollector())
    , load_thread_name(load_thread_name_)
    , refresh_interval_sec(refresh_interval_sec_)
    , shutdown_called(false)
    , log(&Poco::Logger::get("CachedStatisticsStorage"))
{
    load_thread = ThreadFromGlobalPool([this] { loadTask(); });
}

StatisticsPtr CachedStatisticsStorage::get(const StorageID & storage_id, const String & cluster_name)
{
    std::unique_lock<std::mutex> lock(mutex); /// TODO add cache hit ratio metrics
    if (cache.contains(storage_id))
        return cache.at(storage_id)->clonePtr();

    LOG_DEBUG(log, "Getting statistics for table {}, but not found, will load it", storage_id.getFullNameNotQuoted());

    if (auto loaded = loader.load(storage_id, cluster_name))
    {
        cache.insert({storage_id, loaded});
        table_and_clusters.insert({storage_id, cluster_name});
        return loaded->clonePtr();
    }
    LOG_DEBUG(log, "No statistics for table {}", storage_id.getFullNameNotQuoted());
    return nullptr;
}

void CachedStatisticsStorage::collect(const StorageID & storage_id, const Names & columns, ContextMutablePtr context)
{
    collector.collect(storage_id, columns, context);
}

void CachedStatisticsStorage::loadAll()
{
    if (shutdown_called)
        return;

    TableAndClusters table_and_clusters_copy;
    StatisticsMap loaded_stats;

    {
        std::unique_lock<std::mutex> lock(mutex);
        table_and_clusters_copy = table_and_clusters;
    }

    LOG_DEBUG(log, "Will refresh statistics for {} tables", table_and_clusters_copy.size());

    for (auto & [table, cluster] : table_and_clusters_copy)
    {
        auto loaded = loader.load(table, cluster);
        if (loaded)
            loaded_stats.insert({table, loaded});
        else
            LOG_WARNING(log, "No statistics for table {} when refreshing", table.getFullNameNotQuoted());
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        cache.clear();
        cache = loaded_stats;
    }
}

void CachedStatisticsStorage::loadTask()
{
    setThreadName(load_thread_name.c_str());
    while (true)
    {
        if (shutdown_called)
            break;

        std::this_thread::sleep_for(std::chrono::seconds(refresh_interval_sec));

        try
        {
            loadAll();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Error when refresh table statistics in background.");
        }
    }
}

void CachedStatisticsStorage::shutdown()
{
    if (shutdown_called)
        return;

    shutdown_called = true;
    load_thread.join();
}

CachedStatisticsStorage::~CachedStatisticsStorage()
{
    shutdown();
}

}
