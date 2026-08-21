#pragma once

#include <Storages/MergeTree/MarkRange.h>

#include <memory>

namespace DB
{

struct MergeTreeReadTaskInfo;

/// Refines (shrinks) mark ranges cut from a part by a read pool right before they become a read task.
///
/// The ranges a pool was created with are an upper bound: some information usable for pruning
/// becomes available only at execution time. Examples: skip indexes evaluated at data-read time
/// (including a runtime filter collected from the build side of a JOIN) and a projection index
/// bitmap, both built lazily per part (see `IndexReadRangesRefiner`). Pools call `refine` on
/// every cut; ranges dropped here never become read tasks.
///
/// Contract:
/// - `refine` returns a subset of `ranges`: marks may only be removed, never added.
/// - Refinement is an optimization: it must be sound to skip it for any particular cut
///   (rows outside the refined ranges are filtered later during reading anyway).
/// - It may block on first use per part (e.g. to read a projection index), therefore pools
///   must not call it while holding their scheduling mutex.
/// - It is called concurrently from reading threads and must be thread-safe.
class IMergeTreeReadRangesRefiner
{
public:
    virtual ~IMergeTreeReadRangesRefiner() = default;

    virtual MarkRanges refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const = 0;
};

using MergeTreeReadRangesRefinerPtr = std::shared_ptr<const IMergeTreeReadRangesRefiner>;

}
