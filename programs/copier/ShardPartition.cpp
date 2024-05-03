#include "ShardPartition.h"

#include "TaskShard.h"
#include "TaskTable.h"

namespace DB
{

ShardPartition::ShardPartition(TaskShard & parent, String name_quoted_, size_t number_of_splits)
    : task_shard(parent)
    , name(std::move(name_quoted_))
{
    pieces.reserve(number_of_splits);
}

String ShardPartition::getPartitionCleanStartPath() const
{
    return getPartitionPath() + "/clean_start";
}

String ShardPartition::getPartitionPieceCleanStartPath(size_t current_piece_number) const
{
    assert(current_piece_number < task_shard.task_table.number_of_splits);
    return getPartitionPiecePath(current_piece_number) + "/clean_start";
}

String ShardPartition::getPartitionPath() const
{
    return task_shard.task_table.getPartitionPath(name);
}

String ShardPartition::getPartitionPiecePath(size_t current_piece_number) const
{
    assert(current_piece_number < task_shard.task_table.number_of_splits);
    return task_shard.task_table.getPartitionPiecePath(name, current_piece_number);
}

String ShardPartition::getShardStatusPath() const
{
    // schema: /<root...>/tables/<table>/<partition>/shards/<shard>
    // e.g. /root/table_test.hits/201701/shards/1
    return getPartitionShardsPath() + "/" + toString(task_shard.numberInCluster());
}

String ShardPartition::getPartitionShardsPath() const
{
    return getPartitionPath() + "/shards";
}

String ShardPartition::getPartitionActiveWorkersPath() const
{
    return getPartitionPath() + "/partition_active_workers";
}

String ShardPartition::getActiveWorkerPath() const
{
    return getPartitionActiveWorkersPath() + "/" + toString(task_shard.numberInCluster());
}

String ShardPartition::getCommonPartitionIsDirtyPath() const
{
    return getPartitionPath() + "/is_dirty";
}

String ShardPartition::getCommonPartitionIsCleanedPath() const
{
    return getCommonPartitionIsDirtyPath() + "/cleaned";
}

}
