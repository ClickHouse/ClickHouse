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

    void loadIfNot();

    bool usesReplicatedStorage();

    void createFromSQL(const ASTCreateSQLClusterQuery & query);
    void alterFromSQL(const ASTAlterSQLClusterQuery & query);
    void dropFromSQL(const ASTDropSQLClusterQuery & query);

private:
    SQLClusterFactory() = default;

    static ClusterPtr materializeCluster(
        const ASTCreateSQLClusterQuery & query,
        ContextPtr context,
        String create_statement);

    void loadIfNotImpl(std::lock_guard<std::mutex> & lock);
    void reloadFromStorage();
    /// Removes a SQL cluster from `Context` if it was loaded locally but is already gone from the metadata storage.
    void removeStaleLocalCluster(const String & cluster_name);
    void updateFunc();

    mutable std::mutex mutex;
    std::unique_ptr<SQLClusterMetadataStorage> metadata_storage;
    /// Names of SQL clusters currently applied to `Context`, used only to drop removed clusters from it.
    std::unordered_set<String> stored_cluster_names;
    bool loaded = false;
    std::atomic<bool> shutdown_called = false;
    BackgroundSchedulePoolTaskHolder update_task;
    LoggerPtr log = getLogger("SQLClusterFactory");
};

}
