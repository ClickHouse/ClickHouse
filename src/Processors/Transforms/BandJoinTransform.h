#pragma once

#include <deque>
#include <limits>
#include <memory>
#include <optional>

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/BuildProbeJoinTransforms.h>
#include <Processors/Transforms/JoinResidualCondition.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/PODArray.h>

namespace DB
{

/// One bound of the band, normalized to `point op interval_key`: the comparison operator and
/// the key positions in the point-side and interval-side headers (not bound to column names).
struct BandJoinCondition
{
    JoinConditionOperator op = JoinConditionOperator::Unknown;
    size_t point_key_position = 0;
    size_t interval_key_position = 0;
};

/// The band shape `point {>,>=} lo AND point {<,<=} hi`: [0] is the lower bound, [1] the
/// upper one. The two point-side positions may differ when the bounds cast the shared point
/// expression to different common types; each bound compares within its own type.
using BandJoinConditions = std::array<BandJoinCondition, 2>;

/// The join types the band join operator executes, relative to the point side (the executed
/// "left" is always the point side; the step's swap mirror maps the query kinds onto these).
/// Kinds that emit unmatched interval-side rows are out of scope: the probe never revisits
/// the index, so it cannot know which interval rows stayed unmatched.
enum class BandJoinKind : uint8_t
{
    Inner,
    Left,
    LeftSemi,
    LeftAnti,
};

const char * toString(BandJoinKind kind);

/// The finalized interval side: built once by `BandJoinBuildTransform`, then shared read-only
/// by every probe stream. The interval side stays as the blocks delivered by the pre-sort
/// (globally ordered by `lo` ascending, NULLS LAST), never concatenated; rows with NULL or NaN
/// keys are filtered out at finalize (they can never match, and the in-scope kinds never emit
/// interval-side unmatched rows, so the exclusion is complete).
struct BandJoinIndex
{
    struct Block
    {
        /// The columns as delivered (full, non-sparse; invalid rows filtered out).
        Columns columns;
        size_t num_rows = 0;
        /// Rough per-row output size, for the joined-block byte cap.
        size_t avg_row_bytes = 0;
        /// Key columns prepared for comparisons (`LowCardinality` stripped).
        ColumnPtr lo_key;
        ColumnPtr hi_key;
        /// Fixed-width key encodings (see JoinKeyEncoding.h); filled per the index-wide
        /// `lower_encoded` / `upper_encoded` flags.
        PaddedPODArray<UInt64> encoded_lo;
        PaddedPODArray<UInt64> encoded_hi;
        /// prefix_max_hi[i] = max(hi[0..i]) in the encoded domain; the generic path keeps the
        /// arg-max row index instead.
        PaddedPODArray<UInt64> prefix_max_hi;
        PaddedPODArray<UInt32> prefix_max_hi_row;
    };

    /// (block, row) reference for the generic-path directory entries.
    struct RowRef
    {
        UInt32 block = 0;
        UInt32 row = 0;
    };

    std::deque<Block> blocks;

    /// Block directory, one entry per block, in `lo` order. The three `hi` maxima split the
    /// walk's stop decision: the within-block prefix-max stops the scan inside a block, the
    /// block's own max skips a non-matching block in O(1), and only the across-blocks running
    /// max can end the walk entirely.
    PaddedPODArray<UInt64> dir_first_lo;
    PaddedPODArray<UInt64> dir_block_max_hi;
    PaddedPODArray<UInt64> dir_prefix_max_hi;
    /// Generic-path directory: references instead of encoded values (a block's first `lo` is
    /// its row 0, so only the `hi` maxima need storage).
    std::vector<UInt32> dir_block_max_hi_row;
    std::vector<RowRef> dir_prefix_max_hi_ref;

    /// Whether each bound uses the encoded fast path; decided from the key type, so the build
    /// and every probe agree. The generic `compareAt` path is the reference the encoded path
    /// must reproduce.
    bool lower_encoded = false;
    bool upper_encoded = false;

    size_t total_rows = 0;
};

/// The typed slot the build transform publishes the index through; created at wiring time and
/// handed to the build transform and every probe transform. The barrier port closure is the
/// happens-before edge (see BuildProbeJoinTransforms.h).
struct BandJoinSharedState
{
    std::shared_ptr<const BandJoinIndex> index;
};

using BandJoinSharedStatePtr = std::shared_ptr<BandJoinSharedState>;

/// Accumulates the pre-sorted interval side and finalizes it into a `BandJoinIndex`.
/// Each block is finalized as it arrives (filter, encode, prefix-max, directory entry), so
/// `finishBuild` only publishes; the pre-sorted order is trusted and re-checked in debug.
class BandJoinBuildTransform final : public JoinBuildSideTransform
{
public:
    BandJoinBuildTransform(
        SharedHeader input_header_,
        const BandJoinConditions & conditions_,
        const SizeLimits & size_limits_,
        BandJoinSharedStatePtr state_);

    String getName() const override { return "BandJoinBuildTransform"; }

protected:
    bool consumeBuildChunk(Chunk chunk) override;
    void finishBuild() override;

private:
    /// Filter out NULL/NaN-keyed rows and append the block with its per-block index
    /// structures and directory entry; drops the block when nothing remains.
    void appendBlock(Columns columns);

    SharedHeader input_header;
    BandJoinConditions conditions;
    /// Limits on the accumulated interval side, from `max_rows_in_join` / `max_bytes_in_join`.
    SizeLimits size_limits;
    BandJoinSharedStatePtr state;
    std::shared_ptr<BandJoinIndex> index;

    size_t total_rows = 0;
    size_t total_bytes = 0;

#ifndef NDEBUG
    /// `lo` key of the last appended block, to check the pre-sorted order across chunks.
    ColumnPtr last_lo_key;
#endif
};

/// Probes one point-side stream against the shared index. Per point row: a two-level binary
/// search (block directory, then within the block) finds the last position admissible under
/// the lower bound, then a backward walk pruned by the `hi` maxima emits the matches. The
/// resume state (point row, block, walk position) and the reusable scratch are members, so a
/// single high-fanout point row splits across as many output chunks as the caps demand.
/// LEFT pads the interval side with column-type defaults inline on a no-match row (with
/// `join_use_nulls` the planner made those columns Nullable in the pre-join actions); SEMI
/// emits the first match and skips the rest of the walk; ANTI emits padded on no-match only.
/// NULL/NaN point keys match nothing, so LEFT/ANTI emit them padded.
/// With a residual ON condition the walk's candidates are buffered and gated through it in
/// bounded mini-batches; only the passing ones count as matches of the point row.
class BandJoinProbeTransform final : public JoinProbeSideTransform
{
public:
    BandJoinProbeTransform(
        SharedHeader input_header_,
        SharedHeader output_header_,
        const BandJoinConditions & conditions_,
        BandJoinKind kind_,
        std::optional<JoinResidualCondition> residual_,
        BandJoinSharedStatePtr state_,
        size_t max_joined_block_rows_,
        size_t max_joined_block_bytes_);

    String getName() const override { return "BandJoinProbeTransform"; }

protected:
    void onBarrierReleased() override;
    void consumeProbeChunk(Chunk chunk) override;
    std::optional<Chunk> produceChunk() override;

private:
    /// Whether the interval row's `lo` / `hi` admits the point row under the bound's
    /// strictness (generic path; the reference the encoded comparisons must reproduce).
    bool lowerAdmits(const BandJoinIndex::Block & block, size_t row, size_t point_row) const;
    bool upperAdmits(const BandJoinIndex::Block & block, size_t row, size_t point_row) const;
    bool lowerAdmitsEncoded(UInt64 encoded_lo, size_t point_row) const;
    bool upperAdmitsEncoded(UInt64 encoded_hi, size_t point_row) const;

    /// Position the walk at the last position admissible under the lower bound;
    /// false when the point row has no admissible position at all.
    bool findWalkStart(size_t point_row);
    /// Continue the backward walk of the point row from (walk_block, walk_row), emitting
    /// matches. Returns true when the row's walk is complete; false when the output caps or
    /// the work budget stopped it (the position is saved for resumption).
    bool continueWalk(size_t point_row);
    /// Move the walk to the nearest earlier block that can hold a match; true when found,
    /// false when the walk is complete or the work budget stopped it (out_of_budget set).
    bool descendDirectory(size_t point_row, bool & out_of_budget);

    /// Register a match of the walk; returns whether the walk keeps looking for more
    /// (SEMI/ANTI are decided by their first match and skip the rest). With a residual the
    /// match only becomes a buffered candidate, decided by a later flush.
    bool onMatch(size_t point_row, size_t block_index, size_t row);
    /// Evaluate the residual over the pending candidates and consume them; the passing ones
    /// are the row's matches (LEFT emits each, SEMI emits the first and stops, ANTI only
    /// decides). Returns whether the walk keeps looking for more candidates.
    bool flushPendingCandidates(size_t point_row);
    /// The point row's walk is complete: LEFT/ANTI emit the row padded when nothing matched.
    void finishRow(size_t point_row);
    bool emitsUnmatchedRows() const { return kind == BandJoinKind::Left || kind == BandJoinKind::LeftAnti; }

    void emitMatch(size_t point_row, size_t block_index, size_t row);
    void emitUnmatched(size_t point_row);
    bool outputFull() const;
    Chunk buildOutputChunk();
    void resetChunkState();

    /// Re-verify an emitted pair against both bounds by direct evaluation (debug builds).
    bool checkEmittedPair(size_t point_row, const BandJoinIndex::Block & block, size_t row) const;

    BandJoinConditions conditions;
    BandJoinKind kind;
    /// Strictness of each bound, derived from its operator in the constructor.
    bool lower_strict = false;
    bool upper_strict = false;
    /// The residual ON condition gating the candidates, bound to the (point, interval) sides.
    std::optional<JoinResidualConditionEvaluator> residual;
    BandJoinSharedStatePtr state;
    std::shared_ptr<const BandJoinIndex> index;
    /// Output caps with hash-join semantics: the byte cap takes effect only with a non-zero
    /// row cap; it is what bounds fat rows with many matches.
    size_t max_joined_block_rows;
    size_t max_joined_block_bytes;

    /// State of the probe chunk being processed.
    Columns point_columns;
    size_t num_point_rows = 0;
    size_t point_avg_row_bytes = 0;
    /// Point key column per bound, prepared for comparisons (may alias each other).
    std::array<ColumnPtr, 2> point_keys;
    /// Encoded point keys per bound (reusable scratch); filled per the index flags.
    std::array<PaddedPODArray<UInt64>, 2> encoded_point_keys;
    /// Byte mask of the point rows valid as keys (rows with NULL/NaN keys match nothing);
    /// empty = every row is valid.
    IColumn::Filter point_valid;

    /// Walk state, resumable mid-row: `walk_row` < 0 means the current block is exhausted and
    /// the walk descends the directory from `walk_block - 1`.
    size_t current_row = 0;
    bool in_walk = false;
    bool current_row_matched = false;
    size_t walk_block = 0;
    ssize_t walk_row = -1;

    /// Output accumulator: point rows to replicate, and interval (block, row) references
    /// grouped into per-block segments in emission order; a segment with the sentinel block
    /// index emits column-type defaults instead (unmatched rows of LEFT/ANTI).
    static constexpr size_t padded_segment = std::numeric_limits<size_t>::max();
    PaddedPODArray<UInt64> out_point_rows;
    std::vector<std::pair<size_t, ColumnUInt64::MutablePtr>> out_segments;
    size_t out_bytes_estimate = 0;

    /// Pending residual candidates of the current point row, as per-block segments in walk
    /// order; flushed per bounded mini-batch (the first passing candidate decides SEMI/ANTI,
    /// so small batches restore the first-match short-circuit at batch granularity) and when
    /// the pending rows would reach the output row cap, which keeps the cap exact.
    static constexpr size_t residual_batch_size = 1024;
    std::vector<std::pair<size_t, ColumnUInt64::MutablePtr>> pending_segments;
    size_t pending_count = 0;

    /// Bound on the work of one produceChunk call - rows scanned, directory entries visited,
    /// search steps - so that control regularly returns to the executor, which observes
    /// cancellation, even when a long stretch of the walk emits nothing.
    static constexpr size_t produce_work_budget = 1 << 20;
    size_t produce_work = 0;
};

}
