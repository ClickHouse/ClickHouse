#pragma once

#include <Core/Block_fwd.h>
#include <Core/Joins.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <QueryPipeline/SizeLimits.h>

#include <atomic>
#include <mutex>
#include <vector>

namespace DB
{

/// What a block nested loop join produces when the build side turns out to be empty.
enum class EmptyBuildSideAction : uint8_t
{
    /// No pair can match and there is no build row to pad, so the result is empty.
    ProduceNothing,
    /// Every probe row is emitted once with the build-side columns set to their defaults.
    PassProbeRowsPadded,
};

EmptyBuildSideAction emptyBuildSideActionFor(JoinKind kind, JoinStrictness strictness);

/// The materialized build side of a block nested loop join, shared by every build and probe stream.
/// The build streams append to it concurrently until `finish`; from then on it is read-only, and
/// only then are the stored blocks and their global row numbering observable.
class BlockNestedLoopJoinData
{
public:
    BlockNestedLoopJoinData(SharedHeader build_header_, JoinKind kind_, JoinStrictness strictness_, const SizeLimits & size_limits_);

    /// Appends one build block; `num_rows` is authoritative, because a block with no columns still
    /// has rows. Thread-safe. Returns false when the size limits are exceeded under
    /// `join_overflow_mode = 'break'`, asking the caller to stop reading the build side.
    bool addBlock(Block block, size_t num_rows);

    /// Records the build side's `WITH TOTALS` row. It is not a build row: it never takes part in
    /// matching, it only contributes its columns to the joined totals row.
    void setBuildSideTotals(Block totals);
    /// The build side's totals row, or a block with no columns when it has none.
    /// Valid only after `finish`.
    const Block & getBuildSideTotals() const;

    /// Ends the build phase: assigns the global row numbers and makes the store read-only.
    /// Must be called exactly once, after every build stream is done appending.
    void finish();
    bool isFinished() const { return finished.load(std::memory_order_acquire); }

    /// The stored build blocks, in an arbitrary order. Valid only after `finish`.
    const std::vector<StoredBlock> & getBlocks() const;
    /// Global row number of the first row of block `i`, with a trailing entry equal to
    /// `getTotalRows()`. A global row number identifies a build row for the whole probe phase and
    /// stays stable when a block is later moved out of memory. Valid only after `finish`.
    const std::vector<size_t> & getRowOffsets() const;

    size_t getTotalRows() const { return total_rows.load(std::memory_order_relaxed); }
    size_t getTotalBytes() const { return total_bytes.load(std::memory_order_relaxed); }

    /// Valid only after `finish`.
    bool isBuildSideEmpty() const;
    EmptyBuildSideAction getEmptyBuildSideAction() const { return empty_build_side_action; }

    const SharedHeader & getHeader() const { return build_header; }
    JoinKind getKind() const { return kind; }
    JoinStrictness getStrictness() const { return strictness; }

private:
    void assertFinished(const char * what) const;

    const SharedHeader build_header;
    const JoinKind kind;
    const JoinStrictness strictness;
    const SizeLimits size_limits;
    const EmptyBuildSideAction empty_build_side_action;

    mutable std::mutex mutex;
    std::vector<StoredBlock> blocks TSA_GUARDED_BY(mutex);
    std::vector<size_t> row_offsets TSA_GUARDED_BY(mutex);
    Block build_side_totals TSA_GUARDED_BY(mutex);

    std::atomic<size_t> total_rows{0};
    std::atomic<size_t> total_bytes{0};
    std::atomic<bool> finished{false};
};

using BlockNestedLoopJoinDataPtr = std::shared_ptr<BlockNestedLoopJoinData>;

/// Fills `BlockNestedLoopJoinData` with the build side. Carries no data downstream: its output port
/// has an empty header and is finished once the whole build side is stored, which is how the probe
/// side learns that it may start.
class BlockNestedLoopBuildTransform final : public IProcessor
{
public:
    BlockNestedLoopBuildTransform(SharedHeader input_header, BlockNestedLoopJoinDataPtr data_, FinishCounterPtr finish_counter_);

    String getName() const override { return "BlockNestedLoopBuild"; }

    /// Routes the build side's `WITH TOTALS` row into the store. Only one build stream may carry
    /// it, so a pipeline with build-side totals uses a single build stream.
    InputPort * addTotalsPort();

    Status prepare() override;
    void work() override;

private:
    /// Counts this stream out of the build phase, closing the store when it is the last one.
    void finishBuild();

    BlockNestedLoopJoinDataPtr data;
    FinishCounterPtr finish_counter;
    Chunk chunk;
    bool stop_reading = false;
    bool for_totals = false;
    bool build_finished = false;
};

}
