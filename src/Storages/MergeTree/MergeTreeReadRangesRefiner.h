#pragma once

#include <Storages/MergeTree/MarkRange.h>

#include <cstdint>
#include <memory>

namespace DB
{

struct MergeTreeReadTaskInfo;

enum class MergeTreeReadRangesRefinementDirection : uint8_t
{
    Forward,
    Reverse,
};

/// A stateful refinement session for one part and one read task. A pool may feed
/// several consecutive cuts into the same session while collecting enough
/// surviving marks for a task. Sessions are owned by one reading thread, so
/// mutable cursors do not require synchronization.
class IMergeTreeReadRangesRefinementSession
{
public:
    virtual ~IMergeTreeReadRangesRefinementSession() = default;

    /// Returns a subset of `ranges`: marks may only be removed, never added.
    virtual MarkRanges refine(MarkRanges ranges) = 0;
};

using MergeTreeReadRangesRefinementSessionPtr = std::unique_ptr<IMergeTreeReadRangesRefinementSession>;

/// Creates stateful refinements for mark ranges cut from a part by a read pool.
///
/// The ranges a pool was created with are an upper bound: some information usable for pruning
/// becomes available only at execution time. Examples: a projection index bitmap that is built
/// lazily per part (see `ProjectionIndexReadRangesRefiner`), or, in the future, a runtime filter
/// collected from the build side of a JOIN. Ranges dropped by a refinement never become read tasks.
///
/// Contract:
/// - Refinement is an optimization: it must be sound to skip it for any particular cut
///   (rows outside the refined ranges are filtered later during reading anyway).
/// - `createSession` may block on first use per part (e.g. to read a projection index), therefore
///   pools must not call it while holding their scheduling mutex.
/// - `createSession` is called concurrently from reading threads and must be thread-safe.
/// - A returned session is used by only one thread. It may assume cuts arrive monotonically
///   in the requested direction, but cuts assigned to another thread may create gaps.
class IMergeTreeReadRangesRefiner
{
public:
    virtual ~IMergeTreeReadRangesRefiner() = default;

    virtual MergeTreeReadRangesRefinementSessionPtr
    createSession(const MergeTreeReadTaskInfo & info, MergeTreeReadRangesRefinementDirection direction) const
        = 0;
};

using MergeTreeReadRangesRefinerPtr = std::shared_ptr<const IMergeTreeReadRangesRefiner>;

}
