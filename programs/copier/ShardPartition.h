#pragma once

#include "Aliases.h"
#include "TaskTableAndShard.h"

namespace DB
{

/// Just destination partition of a shard
/// I don't know what this comment means.
/// In short, when we discovered what shards contain currently processing partition,
/// This class describes a partition (name) that is stored on the shard (parent).
struct ShardPartition
{
    ShardPartition(TaskShard &parent, String name_quoted_, size_t number_of_splits = 10)
            : task_shard(parent), name(std::move(name_quoted_)) { pieces.reserve(number_of_splits); }

    String getPartitionPath() const;

    String getPartitionPiecePath(size_t current_piece_number) const;

    String getPartitionCleanStartPath() const;

    String getPartitionPieceCleanStartPath(size_t current_piece_number) const;

    String getCommonPartitionIsDirtyPath() const;

    String getCommonPartitionIsCleanedPath() const;

    String getPartitionActiveWorkersPath() const;

    String getActiveWorkerPath() const;

    String getPartitionShardsPath() const;

    String getShardStatusPath() const;

    /// What partition pieces are present in current shard.
    /// FYI: Piece is a part of partition which has modulo equals to concrete constant (less than number_of_splits obliously)
    /// For example SELECT ... from ... WHERE partition=current_partition AND cityHash64(*) == const;
    /// Absent pieces have field is_absent_piece equals to true.
    PartitionPieces pieces;

    TaskShard & task_shard;
    String name;
};

inline String ShardPartition::getPartitionCleanStartPath() const
{
    return getPartitionPath() + "/clean_start";
}

inline String ShardPartition::getPartitionPieceCleanStartPath(size_t current_piece_number) const
{
    assert(current_piece_number < task_shard.task_table.number_of_splits);
    return getPartitionPiecePath(current_piece_number) + "/clean_start";
}

inline String ShardPartition::getPartitionPath() const
{
    return task_shard.task_table.getPartitionPath(name);
}

inline String ShardPartition::getPartitionPiecePath(size_t current_piece_number) const
{
    assert(current_piece_number < task_shard.task_table.number_of_splits);
    return task_shard.task_table.getPartitionPiecePath(name, current_piece_number);
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
