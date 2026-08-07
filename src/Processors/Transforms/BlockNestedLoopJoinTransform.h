#pragma once

#include <Columns/FilterDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/BlockNestedLoopJoinData.h>

#include <deque>
#include <optional>

namespace DB
{

/// An arbitrary `JOIN ON` condition evaluated on (left row, right row) pairs inside the operator.
struct BlockNestedLoopPredicate
{
    /// Where a required column of `actions` comes from: the input (0 = left, 1 = right)
    /// and the column's position in that input's header.
    struct Source
    {
        size_t side = 0;
        size_t position = 0;
    };

    ExpressionActionsPtr actions;
    /// One entry per required column of `actions`, in `getRequiredColumnsWithTypes` order.
    std::vector<Source> inputs;
};

/// Matches every probe (left) row against the materialized build side by evaluating the join
/// condition on tiles of candidate pairs. Runs only after the build phase is over, which the
/// pipeline guarantees by holding the probe streams back until the build streams finish.
class BlockNestedLoopProbeTransform final : public IProcessor
{
public:
    BlockNestedLoopProbeTransform(
        SharedHeader probe_header_,
        SharedHeader output_header_,
        BlockNestedLoopJoinDataPtr data_,
        BlockNestedLoopPredicate predicate_,
        size_t max_block_size_,
        size_t max_block_bytes_);

    String getName() const override { return "BlockNestedLoopProbe"; }

    Status prepare() override;
    void work() override;

private:
    /// How far the walk over the current probe chunk has got.
    enum class Stage : uint8_t
    {
        /// Matching the chunk against the stored build blocks.
        Matching,
        /// Emitting the probe rows that no build row matched.
        UnmatchedProbeRows,
        /// The chunk is fully processed and a new one may be pulled.
        Done,
    };

    /// A maximal group of accumulated pairs that share a stored build block. An output chunk can
    /// span several blocks, and the build side of each group is gathered from its own block.
    struct BuildRun
    {
        size_t block_index;
        size_t length;
    };

    void startProbeChunk(Chunk chunk);
    /// Evaluates the condition on the next tile of candidate pairs, accumulates the surviving ones
    /// and advances the build cursor. Returns the number of pairs evaluated.
    size_t matchNextTile();
    /// Whether the accumulated pairs already fill an output chunk.
    bool hasFullOutputChunk() const;
    /// Materializes at most `max_block_size` accumulated pairs, keeping the rest for the next call.
    Chunk takeMatchedRows();
    /// Emits the next window of probe rows that stayed unmatched, padded with build-side defaults.
    Chunk takeUnmatchedProbeRows();

    const SharedHeader probe_header;
    const SharedHeader output_header;
    BlockNestedLoopJoinDataPtr data;
    BlockNestedLoopPredicate predicate;
    /// The fixed input structure of `predicate.actions`, and its precomputed input mapping.
    Block predicate_input_header;
    std::vector<ssize_t> predicate_input_positions;

    /// Limits on one output chunk; the walk over the build side yields as soon as either is reached.
    const size_t max_block_size;
    const size_t max_block_bytes;

    /// Whether this kind and strictness are implemented by the probe at all.
    const bool implemented;
    /// Whether a probe row that matched nothing is still part of the result, padded.
    const bool keep_unmatched_probe_rows;

    /// The probe chunk being walked.
    Columns probe_columns;
    size_t probe_num_rows = 0;
    bool has_probe_chunk = false;
    Stage stage = Stage::Done;

    /// The walk over the build side: one tile is `probe_num_rows x build_rows_per_tile` pairs.
    size_t build_rows_per_tile = 1;
    size_t build_block_cursor = 0;
    size_t build_row_cursor = 0;

    /// The pairs that satisfied the condition and are not emitted yet: a row index within the probe
    /// chunk, and a row index within the stored block named by the matching entry of `build_runs`.
    PaddedPODArray<UInt64> matched_probe_rows;
    PaddedPODArray<UInt64> matched_build_rows;
    std::deque<BuildRun> build_runs;
    /// Rough size of one output row, used only to decide when `max_block_bytes` is reached.
    size_t probe_row_bytes = 0;
    size_t build_row_bytes = 0;

    /// Set for every probe row that matched at least one build row. Empty for the kinds that do
    /// not keep unmatched probe rows.
    IColumnFilter probe_row_matched;
    size_t unmatched_probe_cursor = 0;

    std::optional<Chunk> output_chunk;
};

/// Produces the joined `WITH TOTALS` row: the probe side's totals row extended with the build
/// side's totals row, or with defaults where the build side has no totals of its own. The totals
/// rows never take part in matching, exactly as in `JoinCommon::joinTotals`.
class BlockNestedLoopTotalsTransform final : public ISimpleTransform
{
public:
    BlockNestedLoopTotalsTransform(
        SharedHeader probe_header_,
        SharedHeader output_header_,
        BlockNestedLoopJoinDataPtr data_,
        bool probe_totals_are_default_);

    String getName() const override { return "BlockNestedLoopTotals"; }

    void transform(Chunk & chunk) override;

private:
    BlockNestedLoopJoinDataPtr data;
    /// The probe side had no totals of its own; the row was synthesized only to carry the build
    /// side's totals. With no build totals either there is nothing to report, so the row is dropped.
    bool probe_totals_are_default;
};

}
