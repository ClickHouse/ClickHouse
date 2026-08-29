#pragma once

#include <Common/SQLClusters/SQLClusterMetadataStorage.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTSQLClusterQuery.h>
#include <Common/logger_useful.h>

#include <atomic>
#include <mutex>
#include <unordered_set>


namespace DB
{

class SQLClusterFactory
{
public:
    static SQLClusterFactory & instance();

    void shutdown();

    bool isEnabled() const;

    void loadIfNot();

    void createFromSQL(const ASTCreateSQLClusterQuery & query);
    void alterFromSQL(const ASTAlterSQLClusterQuery & query);
    void dropFromSQL(const ASTDropSQLClusterQuery & query);

private:
    SQLClusterFactory() = default;

    static ClusterPtr materializeCluster(const ASTCreateSQLClusterQuery & query, ContextPtr context);

    void loadIfNot(std::lock_guard<std::mutex> & lock);
    void reloadFromStorage();
    void updateFunc();

    mutable std::mutex mutex;
    std::unique_ptr<SQLClusterMetadataStorage> metadata_storage TSA_GUARDED_BY(mutex);
    /// Snapshot of cluster names from the last Keeper sync, used only to drop removed clusters from Context.
    std::unordered_set<String> keeper_cluster_names TSA_GUARDED_BY(mutex);
    bool loaded TSA_GUARDED_BY(mutex) = false;
    std::atomic<bool> shutdown_called = false;
    BackgroundSchedulePoolTaskHolder update_task;
    LoggerPtr log = getLogger("SQLClusterFactory");
};

}
