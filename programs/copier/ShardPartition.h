#pragma once

#include "ShardPartitionPiece.h"

#include <base/types.h>

#include <map>

namespace DB
{

struct TaskShard;

/// Just destination partition of a shard
/// I don't know what this comment means.
/// In short, when we discovered what shards contain currently processing partition,
/// This class describes a partition (name) that is stored on the shard (parent).
struct ShardPartition
{
    ShardPartition(TaskShard &parent, String name_quoted_, size_t number_of_splits = 10);

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

using TasksPartition = std::map<String, ShardPartition, std::greater<>>;

}
