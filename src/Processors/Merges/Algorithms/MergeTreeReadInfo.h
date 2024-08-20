#pragma once

#include <Processors/Chunk.h>

namespace DB
{

/// To carry part level and virtual row if chunk is produced by a merge tree source
class MergeTreeReadInfo : public ChunkInfoCloneable<MergeTreeReadInfo>
{
public:
    MergeTreeReadInfo() = delete;
    explicit MergeTreeReadInfo(size_t part_level, bool virtual_row_) :
        origin_merge_tree_part_level(part_level), virtual_row(virtual_row_) { }
    MergeTreeReadInfo(const MergeTreeReadInfo & other) = default;

    size_t origin_merge_tree_part_level = 0;
    /// If virtual_row is true, the chunk must contain the virtual row only.
    bool virtual_row = false;
};

inline size_t getPartLevelFromChunk(const Chunk & chunk)
{
    const auto read_info = chunk.getChunkInfos().get<MergeTreeReadInfo>();
    if (read_info)
        return read_info->origin_merge_tree_part_level;
    return 0;
}

inline bool getVirtualRowFromChunk(const Chunk & chunk)
{
    const auto read_info = chunk.getChunkInfos().get<MergeTreeReadInfo>();
    if (read_info)
        return read_info->virtual_row;
    return false;
}

}
