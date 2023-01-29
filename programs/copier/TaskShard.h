#pragma once

#include "Aliases.h"
#include "Internals.h"
#include "ClusterPartition.h"
#include "ShardPartition.h"


namespace DB
{

struct TaskTable;

struct TaskShard
{
    TaskShard(TaskTable & parent, const Cluster::ShardInfo & info_);

    TaskTable & task_table;

    Cluster::ShardInfo info;

    UInt32 numberInCluster() const;

    UInt32 indexInCluster() const;

    String getDescription() const;

    String getHostNameExample() const;

    /// Used to sort clusters by their proximity
    ShardPriority priority;

    /// Column with unique destination partitions (computed from engine_push_partition_key expr.) in the shard
    ColumnWithTypeAndName partition_key_column;

    /// There is a task for each destination partition
    TasksPartition partition_tasks;

    /// Which partitions have been checked for existence
    /// If some partition from this lists is exists, it is in partition_tasks
    std::set<String> checked_partitions;

    /// Last CREATE TABLE query of the table of the shard
    ASTPtr current_pull_table_create_query;
    ASTPtr current_push_table_create_query;

    /// Internal distributed tables
    DatabaseAndTableName table_read_shard;
    DatabaseAndTableName main_table_split_shard;
    ListOfDatabasesAndTableNames list_of_split_tables_on_shard;
};

using TaskShardPtr = std::shared_ptr<TaskShard>;
using TasksShard = std::vector<TaskShardPtr>;

}
