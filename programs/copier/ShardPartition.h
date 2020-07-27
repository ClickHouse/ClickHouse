#pragma once

#include "Aliases.h"

namespace DB
{

/// Just destination partition of a shard
struct ShardPartition
{
    ShardPartition(TaskShard & parent, const String & name_quoted_) : task_shard(parent), name(name_quoted_) {}

    String getPartitionPath() const;
    String getPartitionCleanStartPath() const;
    String getCommonPartitionIsDirtyPath() const;
    String getCommonPartitionIsCleanedPath() const;
    String getPartitionActiveWorkersPath() const;
    String getActiveWorkerPath() const;
    String getPartitionShardsPath() const;
    String getShardStatusPath() const;

    TaskShard & task_shard;
    String name;
};

inline String ShardPartition::getPartitionCleanStartPath() const
{
    return getPartitionPath() + "/clean_start";
}

inline String ShardPartition::getPartitionPath() const
{
    return task_shard.task_table.getPartitionPath(name);
}

inline String ShardPartition::getShardStatusPath() const
{
    // schema: /<root...>/tables/<table>/<partition>/shards/<shard>
    // e.g. /root/table_test.hits/201701/shards/1
    return getPartitionShardsPath() + "/" + toString(task_shard.numberInCluster());
}

inline String ShardPartition::getPartitionShardsPath() const
{
    return getPartitionPath() + "/shards";
}

inline String ShardPartition::getPartitionActiveWorkersPath() const
{
    return getPartitionPath() + "/partition_active_workers";
}

inline String ShardPartition::getActiveWorkerPath() const
{
    return getPartitionActiveWorkersPath() + "/" + toString(task_shard.numberInCluster());
}

inline String ShardPartition::getCommonPartitionIsDirtyPath() const
{
    return getPartitionPath() + "/is_dirty";
}

inline String ShardPartition::getCommonPartitionIsCleanedPath() const
{
    return getCommonPartitionIsDirtyPath() + "/cleaned";
}

}
