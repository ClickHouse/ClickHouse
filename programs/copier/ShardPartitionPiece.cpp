#include "ShardPartitionPiece.h"

#include "ShardPartition.h"
#include "TaskShard.h"

#include <IO/WriteHelpers.h>

namespace DB
{

ShardPartitionPiece::ShardPartitionPiece(ShardPartition & parent, size_t current_piece_number_, bool is_present_piece_)
    : is_absent_piece(!is_present_piece_)
    , current_piece_number(current_piece_number_)
    , shard_partition(parent)
{
}

String ShardPartitionPiece::getPartitionPiecePath() const
{
    return shard_partition.getPartitionPath() + "/piece_" + toString(current_piece_number);
}

String ShardPartitionPiece::getPartitionPieceCleanStartPath() const
{
    return getPartitionPiecePath() + "/clean_start";
}

String ShardPartitionPiece::getPartitionPieceIsDirtyPath() const
{
    return getPartitionPiecePath() + "/is_dirty";
}

String ShardPartitionPiece::getPartitionPieceIsCleanedPath() const
{
    return getPartitionPieceIsDirtyPath() + "/cleaned";
}

String ShardPartitionPiece::getPartitionPieceActiveWorkersPath() const
{
    return getPartitionPiecePath() + "/partition_piece_active_workers";
}

String ShardPartitionPiece::getActiveWorkerPath() const
{
    return getPartitionPieceActiveWorkersPath() + "/" + toString(shard_partition.task_shard.numberInCluster());
}

/// On what shards do we have current partition.
String ShardPartitionPiece::getPartitionPieceShardsPath() const
{
    return getPartitionPiecePath() + "/shards";
}

String ShardPartitionPiece::getShardStatusPath() const
{
    return getPartitionPieceShardsPath() + "/" + toString(shard_partition.task_shard.numberInCluster());
}

String ShardPartitionPiece::getPartitionPieceCleanerPath() const
{
    return getPartitionPieceIsDirtyPath() + "/cleaner";
}

}
