#pragma once

#include <Processors/Chunk.h>

#include <optional>

namespace DB
{

/// A slice of MergeTreeReadPoolInOrderSliced: a lane (one part) and the number of the slice within the
/// lane, counted in reading order.
struct MergeTreeSliceTag
{
    size_t lane = 0;
    size_t seq = 0;

    bool operator==(const MergeTreeSliceTag &) const = default;
};

/// On every chunk a source emits while reading from MergeTreeReadPoolInOrderSliced. A chunk with rows
/// (or a virtual row) carries the slice its rows belong to. The empty chunk a source emits right after
/// it asked the pool for its next slice carries the slice of the task it finished, if any, and whether
/// it got nothing to read. The router takes the info off; nothing of it reaches the merge.
class MergeTreeSliceInfo : public ChunkInfoCloneable<MergeTreeSliceInfo>
{
public:
    MergeTreeSliceInfo() = default;
    MergeTreeSliceInfo(const MergeTreeSliceInfo &) = default;

    std::optional<MergeTreeSliceTag> slice;
    std::optional<MergeTreeSliceTag> ended;
    bool idle = false;
};

}
