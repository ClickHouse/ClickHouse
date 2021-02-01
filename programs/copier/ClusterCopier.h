#pragma once

#include "Aliases.h"
#include "Internals.h"
#include "TaskCluster.h"
#include "TaskTableAndShard.h"
#include "ShardPartition.h"
#include "ShardPartitionPiece.h"
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

    template <typename T>
    decltype(auto) retry(T && func, UInt64 max_tries = 100);

    void discoverShardPartitions(const ConnectionTimeouts & timeouts, const TaskShardPtr & task_shard);

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

    void setMoveFaultProbability(double move_fault_probability_)
    {
        move_fault_probability = move_fault_probability_;
    }

    void setExperimentalUseSampleOffset(bool value)
    {
        experimental_use_sample_offset = value;
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
            const zkutil::ZooKeeperPtr & zookeeper,
            const String & description,
            bool unprioritized);

    /*
     * Checks that partition piece or some other entity is clean.
     * The only requirement is that you have to pass is_dirty_flag_path and is_dirty_cleaned_path to the function.
     * And is_dirty_flag_path is a parent of is_dirty_cleaned_path.
     * */
    static bool checkPartitionPieceIsClean(
            const zkutil::ZooKeeperPtr & zookeeper,
            const CleanStateClock & clean_state_clock,
            const String & task_status_path);

    bool checkAllPiecesInPartitionAreDone(const TaskTable & task_table, const String & partition_name, const TasksShard & shards_with_partition);

    /** Checks that the whole partition of a table was copied. We should do it carefully due to dirty lock.
     * State of some task could change during the processing.
     * We have to ensure that all shards have the finished state and there is no dirty flag.
     * Moreover, we have to check status twice and check zxid, because state can change during the checking.
     */

    /* The same as function above
     * Assume that we don't know on which shards do we have partition certain piece.
     * We'll check them all (I mean shards that contain the whole partition)
     * And shards that don't have certain piece MUST mark that piece is_done true.
     * */
    bool checkPartitionPieceIsDone(const TaskTable & task_table, const String & partition_name,
                                   size_t piece_number, const TasksShard & shards_with_partition);


    /*Alter successful insertion to helping tables it will move all pieces to destination table*/
    TaskStatus tryMoveAllPiecesToDestinationTable(const TaskTable & task_table, const String & partition_name);

    /// Removes MATERIALIZED and ALIAS columns from create table query
    static ASTPtr removeAliasColumnsFromCreateQuery(const ASTPtr & query_ast);

    bool tryDropPartitionPiece(ShardPartition & task_partition, const size_t current_piece_number,
            const zkutil::ZooKeeperPtr & zookeeper, const CleanStateClock & clean_state_clock);

    static constexpr UInt64 max_table_tries = 1000;
    static constexpr UInt64 max_shard_partition_tries = 600;
    static constexpr UInt64 max_shard_partition_piece_tries_for_alter = 100;

    bool tryProcessTable(const ConnectionTimeouts & timeouts, TaskTable & task_table);

    /// Job for copying partition from particular shard.
    TaskStatus tryProcessPartitionTask(const ConnectionTimeouts & timeouts,
                                       ShardPartition & task_partition,
                                       bool is_unprioritized_task);

    TaskStatus iterateThroughAllPiecesInPartition(const ConnectionTimeouts & timeouts,
                                                  ShardPartition & task_partition,
                                                  bool is_unprioritized_task);

    TaskStatus processPartitionPieceTaskImpl(const ConnectionTimeouts & timeouts,
                                             ShardPartition & task_partition,
                                             const size_t current_piece_number,
                                             bool is_unprioritized_task);

    void dropAndCreateLocalTable(const ASTPtr & create_ast);

    void dropLocalTableIfExists(const DatabaseAndTableName & table_name) const;

    void dropHelpingTables(const TaskTable & task_table);

    /// Is used for usage less disk space.
    /// After all pieces were successfully moved to original destination
    /// table we can get rid of partition pieces (partitions in helping tables).
    void dropParticularPartitionPieceFromAllHelpingTables(const TaskTable & task_table, const String & partition_name);

    String getRemoteCreateTable(const DatabaseAndTableName & table, Connection & connection, const Settings * settings = nullptr);

    ASTPtr getCreateTableForPullShard(const ConnectionTimeouts & timeouts, TaskShard & task_shard);

    /// If it is implicitly asked to create split Distributed table for certain piece on current shard, we will do it.
    void createShardInternalTables(const ConnectionTimeouts & timeouts, TaskShard & task_shard, bool create_split = true);

    std::set<String> getShardPartitions(const ConnectionTimeouts & timeouts, TaskShard & task_shard);

    bool checkShardHasPartition(const ConnectionTimeouts & timeouts, TaskShard & task_shard, const String & partition_quoted_name);

    bool checkPresentPartitionPiecesOnCurrentShard(const ConnectionTimeouts & timeouts,
             TaskShard & task_shard, const String & partition_quoted_name, size_t current_piece_number);

    /*
     * This class is used in executeQueryOnCluster function
     * You can execute query on each shard (no sense it is executed on each replica of a shard or not)
     * or you can execute query on each replica on each shard.
     * First mode is useful for INSERTS queries.
     * */
    enum ClusterExecutionMode
    {
        ON_EACH_SHARD,
        ON_EACH_NODE
    };

    /** Executes simple query (without output streams, for example DDL queries) on each shard of the cluster
      * Returns number of shards for which at least one replica executed query successfully
      */
    UInt64 executeQueryOnCluster(
            const ClusterPtr & cluster,
            const String & query,
            const Settings & current_settings,
            PoolMode pool_mode = PoolMode::GET_ALL,
            ClusterExecutionMode execution_mode = ClusterExecutionMode::ON_EACH_SHARD,
            UInt64 max_successful_executions_per_shard = 0) const;

private:
    String task_zookeeper_path;
    String task_description_path;
    String host_id;
    String working_database_name;

    /// Auto update config stuff
    UInt64 task_description_current_version = 1;
    std::atomic<UInt64> task_description_version{1};
    Coordination::WatchCallback task_description_watch_callback;
    /// ZooKeeper session used to set the callback
    zkutil::ZooKeeperPtr task_description_watch_zookeeper;

    ConfigurationPtr task_cluster_initial_config;
    ConfigurationPtr task_cluster_current_config;
    Coordination::Stat task_description_current_stat{};

    std::unique_ptr<TaskCluster> task_cluster;

    bool is_safe_mode = false;
    double copy_fault_probability = 0.0;
    double move_fault_probability = 0.0;

    bool experimental_use_sample_offset{false};

    Context & context;
    Poco::Logger * log;

    std::chrono::milliseconds default_sleep_time{1000};
};
}
