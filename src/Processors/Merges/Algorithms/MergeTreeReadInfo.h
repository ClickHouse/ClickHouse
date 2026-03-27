#pragma once

#include <Processors/Chunk.h>
#include <Core/Block.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// To carry part level and virtual row if chunk is produced by a merge tree source
class MergeTreeReadInfo : public ChunkInfoCloneable<MergeTreeReadInfo>
{
public:
    MergeTreeReadInfo() = default;
    explicit MergeTreeReadInfo(size_t part_level);
    explicit MergeTreeReadInfo(size_t part_level, const Block & pk_block_, ExpressionActionsPtr virtual_row_conversions_);
    MergeTreeReadInfo(const MergeTreeReadInfo & other);

    size_t origin_merge_tree_part_level = 0;

    /// If is virtual_row, block should not be empty.
    Block pk_block;
    ExpressionActionsPtr virtual_row_conversions;
};

size_t getPartLevelFromChunk(const Chunk & chunk);

bool isVirtualRow(const Chunk & chunk);

void setVirtualRow(Chunk & chunk, const Block & header, bool apply_virtual_row_conversions);

}
