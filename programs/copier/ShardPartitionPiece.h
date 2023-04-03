#pragma once

#include "Internals.h"

namespace DB
{

struct ShardPartitionPiece
{

    ShardPartitionPiece(ShardPartition &parent, size_t current_piece_number_, bool is_present_piece_)
            : is_absent_piece(!is_present_piece_), current_piece_number(current_piece_number_),
              shard_partition(parent) {}

    String getPartitionPiecePath() const;

    String getPartitionPieceCleanStartPath() const;

    String getPartitionPieceIsDirtyPath() const;

    String getPartitionPieceIsCleanedPath() const;

    String getPartitionPieceActiveWorkersPath() const;

    String getActiveWorkerPath() const ;

    /// On what shards do we have current partition.
    String getPartitionPieceShardsPath() const;

    String getShardStatusPath() const;

    String getPartitionPieceCleanerPath() const;

    bool is_absent_piece;
    const size_t current_piece_number;

    ShardPartition & shard_partition;
};


inline String ShardPartitionPiece::getPartitionPiecePath() const
{
    return shard_partition.getPartitionPath() + "/piece_" + toString(current_piece_number);
}

inline String ShardPartitionPiece::getPartitionPieceCleanStartPath() const
{
    return getPartitionPiecePath() + "/clean_start";
}

inline String ShardPartitionPiece::getPartitionPieceIsDirtyPath() const
{
    return getPartitionPiecePath() + "/is_dirty";
}

inline String ShardPartitionPiece::getPartitionPieceIsCleanedPath() const
{
    return getPartitionPieceIsDirtyPath() + "/cleaned";
}

inline String ShardPartitionPiece::getPartitionPieceActiveWorkersPath() const
{
    return getPartitionPiecePath() + "/partition_piece_active_workers";
}

inline String ShardPartitionPiece::getActiveWorkerPath() const
{
    return getPartitionPieceActiveWorkersPath() + "/" + toString(shard_partition.task_shard.numberInCluster());
}

/// On what shards do we have current partition.
inline String ShardPartitionPiece::getPartitionPieceShardsPath() const
{
    return getPartitionPiecePath() + "/shards";
}

inline String ShardPartitionPiece::getShardStatusPath() const
{
    return getPartitionPieceShardsPath() + "/" + toString(shard_partition.task_shard.numberInCluster());
}

inline String ShardPartitionPiece::getPartitionPieceCleanerPath() const
{
    return getPartitionPieceIsDirtyPath() + "/cleaner";
}


}
