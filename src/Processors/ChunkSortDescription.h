#pragma once

#include <Processors/Chunk.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Carries sort-order metadata on a Chunk.
///
/// Attached by sources (e.g. MergeTreeSelectProcessor) that know the data
/// is physically sorted, and consumed by sorting transforms
/// (e.g. PartialSortingTransform) to skip redundant work.
///
/// Automatically preserved through ISimpleTransform (filter, expression, etc.)
/// because those swap the whole Chunk including its ChunkInfos.
/// Dropped by accumulating / merging transforms which is correct — they
/// establish a new sort order.
class ChunkSortDescription : public ChunkInfoCloneable<ChunkSortDescription>
{
public:
    ChunkSortDescription() = default;
    explicit ChunkSortDescription(SortDescription sort_description_)
        : sort_description(std::move(sort_description_))
    {
    }
    ChunkSortDescription(const ChunkSortDescription & other) = default;

    SortDescription sort_description;
};

}
