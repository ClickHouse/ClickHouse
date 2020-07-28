#pragma once

#include <Interpreters/Cluster.h>

namespace DB

{
    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    using DatabaseAndTableName = std::pair<String, String>;

    /// Hierarchical description of the tasks
    struct ShardPartition;
    struct TaskShard;
    struct TaskTable;
    struct TaskCluster;
    struct ClusterPartition;

    using TasksPartition = std::map<String, ShardPartition, std::greater<>>;
    using ShardInfo = Cluster::ShardInfo;
    using TaskShardPtr = std::shared_ptr<TaskShard>;
    using TasksShard = std::vector<TaskShardPtr>;
    using TasksTable = std::list<TaskTable>;
    using ClusterPartitions = std::map<String, ClusterPartition, std::greater<>>;
}

