#pragma once

#include "Aliases.h"
#include "Internals.h"
#include "TaskCluster.h"
#include "TaskTableAndShard.h"
#include "ShardPartition.h"
#include "ZooKeeperStaff.h"

namespace DB
{

class ClusterCopier
{
public:

    ClusterCopier(const String & task_path_,
                  const String & host_id_,
                  const String & proxy_database_name_,
                  Context & context_)
            :
            task_zookeeper_path(task_path_),
            host_id(host_id_),
            working_database_name(proxy_database_name_),
            context(context_),
            log(&Poco::Logger::get("ClusterCopier")) {}

    void init();

    template<typename T>
    decltype(auto) retry(T && func, UInt64 max_tries = 100);

    void discoverShardPartitions(const ConnectionTimeouts & timeouts, const TaskShardPtr & task_shard) ;

    /// Compute set of partitions, assume set of partitions aren't changed during the processing
    void discoverTablePartitions(const ConnectionTimeouts & timeouts, TaskTable & task_table, UInt64 num_threads = 0);

    void uploadTaskDescription(const std::string & task_path, const std::string & task_file, const bool force);

    void reloadTaskDescription();

    void updateConfigIfNeeded();

    void process(const ConnectionTimeouts & timeouts);

    /// Disables DROP PARTITION commands that used to clear data after errors
    void setSafeMode(bool is_safe_mode_ = true)
    {
        is_safe_mode = is_safe_mode_;
    }

    void setCopyFaultProbability(double copy_fault_probability_)
    {
        copy_fault_probability = copy_fault_probability_;
    }


protected:

    String getWorkersPath() const
    {
        return task_cluster->task_zookeeper_path + "/task_active_workers";
    }

    String getWorkersPathVersion() const
    {
        return getWorkersPath() + "_version";
    }

    String getCurrentWorkerNodePath() const
    {
        return getWorkersPath() + "/" + host_id;
    }

    zkutil::EphemeralNodeHolder::Ptr createTaskWorkerNodeAndWaitIfNeed(
            const zkutil::ZooKeeperPtr &zookeeper,
            const String &description,
            bool unprioritized);

    /** Checks that the whole partition of a table was copied. We should do it carefully due to dirty lock.
     * State of some task could change during the processing.
     * We have to ensure that all shards have the finished state and there is no dirty flag.
     * Moreover, we have to check status twice and check zxid, because state can change during the checking.
     */
    bool checkPartitionIsDone(const TaskTable & task_table, const String & partition_name,
                              const TasksShard & shards_with_partition);

    /// Removes MATERIALIZED and ALIAS columns from create table query
    static ASTPtr removeAliasColumnsFromCreateQuery(const ASTPtr &query_ast);

    /// Replaces ENGINE and table name in a create query
    std::shared_ptr<ASTCreateQuery>
    rewriteCreateQueryStorage(const ASTPtr & create_query_ast, const DatabaseAndTableName & new_table,
                              const ASTPtr & new_storage_ast);

    bool tryDropPartition(ShardPartition & task_partition,
                          const zkutil::ZooKeeperPtr & zookeeper,
                          const CleanStateClock & clean_state_clock);


    static constexpr UInt64 max_table_tries = 1000;
    static constexpr UInt64 max_shard_partition_tries = 600;

    bool tryProcessTable(const ConnectionTimeouts & timeouts, TaskTable & task_table);

    PartitionTaskStatus tryProcessPartitionTask(const ConnectionTimeouts & timeouts,
                                                ShardPartition & task_partition,
                                                bool is_unprioritized_task);

    PartitionTaskStatus processPartitionTaskImpl(const ConnectionTimeouts & timeouts,
                                                 ShardPartition & task_partition,
                                                 bool is_unprioritized_task);

    void dropAndCreateLocalTable(const ASTPtr & create_ast);

    void dropLocalTableIfExists (const DatabaseAndTableName & table_name) const;

    String getRemoteCreateTable(const DatabaseAndTableName & table,
                                Connection & connection,
                                const Settings * settings = nullptr);

    ASTPtr getCreateTableForPullShard(const ConnectionTimeouts & timeouts,
                                      TaskShard & task_shard);

    void createShardInternalTables(const ConnectionTimeouts & timeouts,
                                   TaskShard & task_shard,
                                   bool create_split = true);

    std::set<String> getShardPartitions(const ConnectionTimeouts & timeouts,
                                        TaskShard & task_shard);

    bool checkShardHasPartition(const ConnectionTimeouts & timeouts,
                                TaskShard & task_shard,
                                const String & partition_quoted_name);

    /** Executes simple query (without output streams, for example DDL queries) on each shard of the cluster
      * Returns number of shards for which at least one replica executed query successfully
      */
    UInt64 executeQueryOnCluster(
            const ClusterPtr & cluster,
            const String & query,
            const ASTPtr & query_ast_ = nullptr,
            const Settings * settings = nullptr,
            PoolMode pool_mode = PoolMode::GET_ALL,
            UInt64 max_successful_executions_per_shard = 0) const;

private:
    String task_zookeeper_path;
    String task_description_path;
    String host_id;
    String working_database_name;

    /// Auto update config stuff
    UInt64 task_descprtion_current_version = 1;
    std::atomic<UInt64> task_descprtion_version{1};
    Coordination::WatchCallback task_description_watch_callback;
    /// ZooKeeper session used to set the callback
    zkutil::ZooKeeperPtr task_description_watch_zookeeper;

    ConfigurationPtr task_cluster_initial_config;
    ConfigurationPtr task_cluster_current_config;
    Coordination::Stat task_descprtion_current_stat{};

    std::unique_ptr<TaskCluster> task_cluster;

    bool is_safe_mode = false;
    double copy_fault_probability = 0.0;

    Context & context;
    Poco::Logger * log;

    std::chrono::milliseconds default_sleep_time{1000};
};

}
