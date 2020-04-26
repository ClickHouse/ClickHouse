#pragma once

#include <Interpreters/Cluster.h>

namespace DB
{
    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    using DatabaseAndTableName = std::pair<String, String>;
    using ListOfDatabasesAndTableNames = std::vector<DatabaseAndTableName>;

    /// Hierarchical description of the tasks
    struct ShardPartitionPiece;
    struct ShardPartition;
    struct TaskShard;
    struct TaskTable;
    struct TaskCluster;
    struct ClusterPartition;

    using PartitionPieces = std::vector<ShardPartitionPiece>;
    using TasksPartition = std::map<String, ShardPartition, std::greater<>>;
    using ShardInfo = Cluster::ShardInfo;
    using TaskShardPtr = std::shared_ptr<TaskShard>;
    using TasksShard = std::vector<TaskShardPtr>;
    using TasksTable = std::list<TaskTable>;
    using ClusterPartitions = std::map<String, ClusterPartition, std::greater<>>;
}

