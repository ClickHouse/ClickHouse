#pragma once

#include <base/types.h>

#include <vector>

namespace DB
{

struct ShardPartition;

struct ShardPartitionPiece
{
    ShardPartitionPiece(ShardPartition & parent, size_t current_piece_number_, bool is_present_piece_);

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

using PartitionPieces = std::vector<ShardPartitionPiece>;

}
