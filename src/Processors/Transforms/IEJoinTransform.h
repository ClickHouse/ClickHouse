#pragma once

#include <array>
#include <optional>

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/PODArray.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// One inequality join condition `left.x op right.x`: the comparison operator and the
/// positions of the key columns in the input headers (not bound to column names).
struct IEJoinCondition
{
    JoinConditionOperator op = JoinConditionOperator::Unknown;
    size_t left_key_position = 0;
    size_t right_key_position = 0;

    /// `side` is 0 for the left input, 1 for the right one.
    size_t keyPosition(size_t side) const { return side == 0 ? left_key_position : right_key_position; }
};

/// The join types the IEJoin operator executes. There are no right-side SEMI/ANTI:
/// `IEJoinStep` executes them as the left-side mirror with swapped inputs and reversed operators.
enum class IEJoinKind : uint8_t
{
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    LeftAnti,
};

const char * toString(IEJoinKind kind);

/// The two conditions `left.x op1 right.x AND left.y op2 right.y` executed by the IEJoin
/// algorithm: the first condition defines the L1 order, the second the L2 order.
using IEJoinConditions = std::array<IEJoinCondition, 2>;

/// A residual JOIN ON condition beyond the two inequalities, evaluated per candidate pair:
/// a single-output boolean expression over columns of both inputs. A pair matches only when
/// both inequalities hold AND the residual passes (a NULL result counts as failed).
struct IEJoinResidualCondition
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

/*
 * Joins two fully materialized streams by two inequality conditions
 * `left.x op1 right.x AND left.y op2 right.y` with the IEJoin algorithm
 * (Khayyat et al., "Lightning Fast and Space Efficient Inequality Joins", PVLDB 8(13), 2015).
 *
 * Both inputs are accumulated entirely, then a union of both sides is sorted twice:
 * L1 by the first condition's keys (so that a higher L1 position implies the first condition holds
 * with respect to any lower position) and L2 by the second condition's keys. A single pass over L2
 * with a monotone frontier and a bit array over L1 positions produces all result pairs:
 * the first condition is answered by position in L1, the second by membership in the bit array,
 * so no join predicate is ever evaluated per candidate pair.
 */
class IEJoinAlgorithm final : public IMergingAlgorithm
{
public:
    IEJoinAlgorithm(
        IEJoinKind kind_,
        const IEJoinConditions & conditions_,
        std::optional<IEJoinResidualCondition> residual_,
        bool inputs_sorted_by_first_key_,
        const SharedHeaders & input_headers_,
        const SizeLimits & size_limits_,
        size_t max_block_size_);

    const char * getName() const override { return "IEJoinAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    MergedStats getMergedStats() const override;

private:
    /// Materialize both sides, sort the union (skipping rows with NULL keys), build Li/P and
    /// the bit array.
    void buildJoinState();

    /// Compare key values (key_index: 0 for L1 keys, 1 for L2 keys) of two union entries via the
    /// generic virtual comparator. This is the reference implementation: the encoded fixed-width
    /// fast path must reproduce its order exactly, all debug checks run against it, and it serves
    /// the types that have no encoding.
    int compareKeysAt(size_t key_index, size_t union_a, size_t union_b) const;

    /// Whether the frontier should advance past L2 entry `l2_from` while processing L2 entry `l2_current`,
    /// i.e. whether the value of `l2_from` satisfies the second condition with respect to `l2_current`.
    bool frontierAdvances(size_t l2_from, size_t l2_current) const;

    /// Advance the L2 cursor to the next left-side entry, updating the frontier and the bit array.
    /// Returns false when L2 is exhausted.
    bool nextLeftRow();

    /// Whether the side's unmatched rows are emitted in a post-phase;
    /// exactly these sides get a matched bitmap filled by the pair scan.
    bool sideNeedsUnmatchedRows(size_t side) const;

    /// Emit up to max_block_size rows: the pair scan first, then the unmatched rows of the
    /// sides that need them; sets produce_done when everything is emitted.
    Chunk produceBatch();
    /// Run the pair scan and fill up to max_block_size rows per the join kind, bounded also
    /// by the per-call work budget (see `produce_work_budget`). Returns true when the scan is
    /// exhausted; an empty chunk alone is not a completion signal.
    bool producePairsBatch(Chunk & chunk);
    /// Fill up to max_block_size rows of the side without a set bit in the matched bitmap,
    /// padding the other side. Returns true when all rows of the side were examined.
    bool produceUnmatchedBatch(size_t side, Chunk & chunk);

    /// Append the side's columns gathered by `indexes`.
    void appendGathered(Chunk & chunk, size_t side, const ColumnUInt64 & indexes) const;
    /// Append `num_rows` default values of the side's column types (NULL for a Nullable type).
    void appendPadded(Chunk & chunk, size_t side, size_t num_rows) const;

    /// Gather the residual condition's input columns for the candidate pairs (row i is the pair
    /// `(left_rows[i], right_rows[i])`), evaluate it, and fold the result into a byte mask
    /// (1 = the pair passes; a NULL result folds to 0). Charges the work budget per candidate.
    IColumn::Filter evaluateResidualMask(const ColumnUInt64 & left_rows, const ColumnUInt64 & right_rows);

    void setBit(size_t pos);
    bool testBit(size_t pos) const;
    /// Position of the first set bit >= from, or n_union if there is none.
    size_t findNextSetBit(size_t from);

    /// Re-verify an emitted pair against both conditions by direct evaluation (debug builds).
    bool checkEmittedPair(size_t key_index, size_t left_row, size_t right_row) const;
    /// Check that the bit array is exactly {right-side entries whose L2 key qualifies
    /// against the current left entry} (debug builds, small inputs only).
    void checkFrontierInvariant() const;
    /// Check at the end of emission that every filtered row of each post-phase side ended up
    /// exactly one of matched/unmatched (debug builds).
    void checkFinalInvariants() const;
#ifndef NDEBUG
    /// With `inputs_sorted_by_first_key` the operator trusts the upstream order; check that an
    /// input chunk is ordered by the first condition's key, also against the previous chunk of
    /// the same input (debug builds; catches wiring bugs).
    void checkInputChunkOrder(const Chunk & chunk, size_t source_num);
#endif

    SharedHeaders input_headers;
    size_t max_block_size;

    IEJoinKind kind;
    IEJoinConditions conditions;
    /// The residual ON condition gating candidate pairs, if any.
    std::optional<IEJoinResidualCondition> residual;
    /// Header of the residual's input columns (in its required-columns order) and the
    /// precomputed input positions for `ExpressionActions::executeOnColumns`.
    Block residual_input_header;
    std::vector<ssize_t> residual_input_positions;
    /// The inputs are each sorted by the first condition's key (ascending, NULLS LAST):
    /// selects the merge-based L1 build; with the flag off the operator orders the union
    /// itself with an index sort.
    bool inputs_sorted_by_first_key;

    /// Limits on the accumulated input (both sides), from `max_rows_in_join` / `max_bytes_in_join`.
    SizeLimits size_limits;

    std::array<Chunks, 2> accumulated_chunks;
    std::array<bool, 2> source_finished = {false, false};

#ifndef NDEBUG
    /// Key column of the last accumulated chunk per input, to check order across chunk boundaries.
    std::array<ColumnPtr, 2> last_input_key_column;
#endif

    /// Populated by buildJoinState:

    /// All columns of each side, result rows are gathered from them.
    std::array<Columns, 2> side_columns;

    /// One byte per row of the side, set by the pair scan; allocated only for sides that need it
    std::array<IColumn::Filter, 2> matched;

    /// Both sides' key column of one condition, prepared for comparisons
    /// (from `side_columns`, with `LowCardinality` stripped).
    struct ConditionKeyColumns
    {
        ColumnPtr left;
        ColumnPtr right;

        ColumnPtr & bySide(size_t side) { return side == 0 ? left : right; }
        const ColumnPtr & bySide(size_t side) const { return side == 0 ? left : right; }
    };

    /// Prepared key columns, one entry per condition.
    std::array<ConditionKeyColumns, 2> key_columns;
    std::array<size_t, 2> num_side_rows = {0, 0};
    size_t n_union = 0;

    /// Union entry at each L1 position. Entry u is left row u if u < num_side_rows[0],
    /// otherwise right row u - num_side_rows[0].
    PaddedPODArray<UInt64> l1_union;
    /// Signed 1-based row ids per L1 position: +k for the k-th left row, -k for the k-th right row.
    PaddedPODArray<Int64> li;
    /// L1 position of each L2 entry (the permutation array P).
    PaddedPODArray<UInt64> permutation;
    /// Second-condition keys encoded into fixed-width values whose unsigned order is the L2 order,
    /// indexed by L1 position (the frontier reaches them through the `permutation` entry it
    /// loads for bit-marking anyway); empty when the condition's type has no encoding
    /// (the frontier then falls back to `compareAt`).
    PaddedPODArray<UInt64> l2_keys_by_position;
    /// One bit per L1 position, set for right-side entries that passed the frontier.
    PaddedPODArray<UInt64> bit_array;
    /// One past the highest set bit; lets scans stop instead of walking empty words to the end.
    size_t bit_array_end = 0;

    /// Merge loop state.
    bool join_state_built = false;
    bool produce_done = false;
    size_t l2_cursor = 0;
    size_t frontier = 0;
    size_t scan_pos = 0;
    Int64 current_left_rid = 0;
    bool has_current_left = false;

    /// Bound on the cursor advances plus bit-array words inspected by one producePairsBatch
    /// call, so that control regularly returns to the executor, which observes cancellation.
    static constexpr size_t produce_work_budget = 1 << 20;
    /// Work spent by the current producePairsBatch call.
    size_t produce_work = 0;
    /// Bound on the candidates of one left row accumulated before a residual evaluation in the
    /// SEMI/ANTI scan: the first passing candidate decides the row, so small batches restore
    /// the first-match short-circuit at batch granularity.
    static constexpr size_t residual_scratch_max_size = 1024;

    /// Post-phase state, per side: the row cursor and the number of unmatched rows emitted
    /// (for the final invariant check).
    std::array<size_t, 2> unmatched_row_cursor = {0, 0};
    std::array<size_t, 2> unmatched_emitted = {0, 0};

    struct Statistic
    {
        size_t num_blocks[2] = {0, 0};
        size_t num_rows[2] = {0, 0};
        size_t num_bytes[2] = {0, 0};
    };
    Statistic stat;
};

class IEJoinTransform final : public IMergingTransform<IEJoinAlgorithm>
{
    using Base = IMergingTransform<IEJoinAlgorithm>;

public:
    IEJoinTransform(
        IEJoinKind kind,
        const IEJoinConditions & conditions,
        std::optional<IEJoinResidualCondition> residual,
        bool inputs_sorted_by_first_key,
        SharedHeaders & input_headers,
        SharedHeader output_header,
        const SizeLimits & size_limits,
        size_t max_block_size);

    String getName() const override { return "IEJoinTransform"; }
};

}
