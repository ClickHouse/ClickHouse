#pragma once

#include <Processors/Chunk.h>

namespace DB
{

/// Attached to an empty chunk that a MergeTreeSource emits after the last chunk of a read task when
/// the source reads slices assigned by MergeTreeInOrderSliceRouter. The router uses it to learn that
/// the source is free for the next slice; the chunk itself never reaches the query pipeline.
class MergeTreeSliceEndInfo : public ChunkInfoCloneable<MergeTreeSliceEndInfo>
{
public:
    MergeTreeSliceEndInfo() = default;
    MergeTreeSliceEndInfo(const MergeTreeSliceEndInfo &) = default;
};

}
